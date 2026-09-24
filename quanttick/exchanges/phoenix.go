package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	PhoenixName                    = "phoenix"
	PhoenixURL                     = "wss://perp-api.phoenix.trade/v1/ws"
	PhoenixRESTURL                 = "https://perp-api.phoenix.trade"
	phoenixTradeLimit              = 10000
	phoenixRecoveryPageLimit       = 1000
	phoenixRecoveryRequestInterval = time.Second / 2
)

var _ quanttick.Exchange = (*Phoenix)(nil)

type Phoenix struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration

	lastTrades       map[string]quanttick.TradeEvent
	seen             *seenTradeIDs
	recoveryThrottle *restThrottle
}

type PhoenixOption func(*Phoenix)

func NewPhoenix(symbols []string, options ...PhoenixOption) *Phoenix {
	exchange := &Phoenix{
		Symbols:             append([]string(nil), symbols...),
		URL:                 PhoenixURL,
		RESTURL:             PhoenixRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		lastTrades:          make(map[string]quanttick.TradeEvent),
		seen:                newSeenTradeIDs(phoenixTradeLimit),
		recoveryThrottle:    newRESTThrottle(phoenixRecoveryRequestInterval),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithPhoenixURL(url string) PhoenixOption {
	return func(p *Phoenix) { p.URL = url }
}

func WithPhoenixRESTURL(url string) PhoenixOption {
	return func(p *Phoenix) { p.RESTURL = url }
}

func WithPhoenixHTTPClient(client *http.Client) PhoenixOption {
	return func(p *Phoenix) { p.HTTPClient = client }
}

func WithPhoenixReconnectDelay(delay time.Duration) PhoenixOption {
	return func(p *Phoenix) { p.ReconnectDelay = delay }
}

func WithPhoenixSubscriptionTimeout(timeout time.Duration) PhoenixOption {
	return func(p *Phoenix) { p.SubscriptionTimeout = timeout }
}

func (p *Phoenix) Name() string { return PhoenixName }

func (p *Phoenix) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(p.ReconnectDelay)
		for ctx.Err() == nil {
			startedAt := time.Now()
			if err := p.run(ctx, trades, errs); err != nil && ctx.Err() == nil {
				sendError(ctx, errs, err)
			}
			if err := sleepContext(ctx, backoff.Next(time.Since(startedAt))); err != nil {
				return
			}
		}
	}()
	return trades, errs
}

func (p *Phoenix) SubscriptionMessages() []map[string]any {
	messages := make([]map[string]any, 0, len(p.Symbols))
	for _, symbol := range p.Symbols {
		messages = append(messages, map[string]any{
			"type":         "subscribe",
			"subscription": map[string]string{"channel": "fills", "marketSymbol": symbol},
		})
	}
	return messages
}

func (p *Phoenix) run(ctx context.Context, trades chan<- quanttick.TradeEvent, errs chan<- error) error {
	conn, err := dialWebSocket(ctx, PhoenixName, p.URL)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	for _, message := range p.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal phoenix subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send phoenix subscription: %w", err)
		}
	}
	counter := &phoenixFillCounter{}
	buffered, err := p.awaitSubscriptions(ctx, conn, counter)
	if err != nil {
		return err
	}
	backlog, err := newTradeBacklog(phoenixTradeLimit, len(buffered))
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, streamErr := p.startTradeReader(streamCtx, conn, backlog, counter)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, recoveryErr := p.recoverTrades(recoveryCtx, time.Now().UTC())
	cancelRecovery()
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, trade := range recovered {
		if err := p.emitTrade(ctx, trades, trade); err != nil {
			return err
		}
	}
	for _, trade := range buffered {
		err := p.emitTrade(ctx, trades, trade)
		backlog.release()
		if err != nil {
			return err
		}
	}
	for trade := range stream {
		err := p.emitTrade(ctx, trades, trade)
		backlog.release()
		if err != nil {
			return err
		}
	}
	err = <-streamErr
	if isNormalWebSocketClose(err) {
		return nil
	}
	return err
}

func (p *Phoenix) awaitSubscriptions(ctx context.Context, conn *websocket.Conn, counter *phoenixFillCounter) ([]quanttick.TradeEvent, error) {
	ackCtx, cancel := context.WithTimeout(ctx, p.SubscriptionTimeout)
	defer cancel()
	pending := make(map[string]bool, len(p.Symbols))
	for _, symbol := range p.Symbols {
		pending[symbol] = true
	}
	var buffered []quanttick.TradeEvent
	for len(pending) > 0 {
		_, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("phoenix subscription acknowledgement timed out after %s", p.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read phoenix subscription acknowledgement: %w", err)
		}
		ack, trades, err := p.parseMessage(data, time.Now().UTC(), counter)
		if err != nil {
			return nil, err
		}
		delete(pending, ack)
		if len(buffered)+len(trades) > phoenixTradeLimit {
			return nil, fmt.Errorf("phoenix pre-acknowledgement trade buffer exceeded %d events", phoenixTradeLimit)
		}
		buffered = append(buffered, trades...)
	}
	return buffered, nil
}

func (p *Phoenix) startTradeReader(ctx context.Context, conn *websocket.Conn, backlog *tradeBacklog, counter *phoenixFillCounter) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent, phoenixTradeLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read phoenix websocket: %w", err)
				return
			}
			_, parsed, err := p.parseMessage(data, time.Now().UTC(), counter)
			if err != nil {
				errs <- err
				return
			}
			for _, trade := range parsed {
				if !backlog.reserve() {
					errs <- fmt.Errorf("phoenix websocket trade buffer exceeded %d events", phoenixTradeLimit)
					_ = conn.CloseNow()
					return
				}
				select {
				case trades <- trade:
				case <-ctx.Done():
					backlog.release()
					errs <- ctx.Err()
					return
				}
			}
		}
	}()
	return trades, errs
}

func (p *Phoenix) emitTrade(ctx context.Context, trades chan<- quanttick.TradeEvent, trade quanttick.TradeEvent) error {
	if !p.seen.Add(trade.Symbol, trade.UID) {
		return nil
	}
	if err := sendTrade(ctx, trades, trade); err != nil {
		return err
	}
	if previous, ok := p.lastTrades[trade.Symbol]; !ok || !trade.Timestamp.Before(previous.Timestamp) {
		p.lastTrades[trade.Symbol] = trade
	}
	return nil
}
