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
	CoinbaseName                    = "coinbase"
	CoinbaseURL                     = "wss://ws-feed.exchange.coinbase.com"
	CoinbaseRESTURL                 = "https://api.exchange.coinbase.com"
	coinbaseSeenTradeLimit          = 10000
	coinbaseSubscriptionBufferLimit = 10000
	coinbaseRecoveryPageLimit       = 1000
	coinbaseRecoveryMaxPages        = 100
	coinbaseRecoveryRequestInterval = time.Second / 3
)

var _ quanttick.Exchange = (*Coinbase)(nil)

type Coinbase struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration

	lastIDs          map[string]int64
	seen             *seenTradeIDs
	subscribed       bool
	recoveryThrottle *restThrottle
}

type CoinbaseOption func(*Coinbase)

func NewCoinbase(symbols []string, options ...CoinbaseOption) *Coinbase {
	exchange := &Coinbase{
		Symbols:             append([]string(nil), symbols...),
		URL:                 CoinbaseURL,
		RESTURL:             CoinbaseRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		lastIDs:             make(map[string]int64),
		seen:                newSeenTradeIDs(coinbaseSeenTradeLimit),
		recoveryThrottle:    newRESTThrottle(coinbaseRecoveryRequestInterval),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithCoinbaseURL(url string) CoinbaseOption {
	return func(exchange *Coinbase) {
		exchange.URL = url
	}
}

func WithCoinbaseRESTURL(url string) CoinbaseOption {
	return func(exchange *Coinbase) {
		exchange.RESTURL = url
	}
}

func WithCoinbaseHTTPClient(client *http.Client) CoinbaseOption {
	return func(exchange *Coinbase) {
		exchange.HTTPClient = client
	}
}

func WithCoinbaseReconnectDelay(delay time.Duration) CoinbaseOption {
	return func(exchange *Coinbase) {
		exchange.ReconnectDelay = delay
	}
}

func WithCoinbaseSubscriptionTimeout(timeout time.Duration) CoinbaseOption {
	return func(exchange *Coinbase) {
		exchange.SubscriptionTimeout = timeout
	}
}

func (c *Coinbase) Name() string {
	return CoinbaseName
}

func (c *Coinbase) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(c.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := c.run(ctx, trades, errs); err != nil {
				if ctx.Err() != nil {
					return
				}
				sendError(ctx, errs, err)
			}

			if err := sleepContext(ctx, backoff.Next(time.Since(startedAt))); err != nil {
				return
			}
		}
	}()

	return trades, errs
}

func (c *Coinbase) SubscriptionMessages() []map[string]any {
	return []map[string]any{
		{
			"type":        "subscribe",
			"product_ids": append([]string(nil), c.Symbols...),
			"channels":    []string{"matches"},
		},
	}
}

func (c *Coinbase) run(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	errs chan<- error,
) error {
	c.subscribed = false
	conn, err := dialWebSocket(ctx, CoinbaseName, c.URL)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	for _, message := range c.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal coinbase subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send coinbase subscription: %w", err)
		}
	}

	buffered, err := c.awaitSubscription(ctx, conn)
	if err != nil {
		return err
	}
	backlog, err := newTradeBacklog(coinbaseSubscriptionBufferLimit, len(buffered))
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, streamErr := c.startTradeReader(streamCtx, conn, backlog)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, recoveryErr := c.recoverTrades(recoveryCtx)
	cancelRecovery()
	for _, parsed := range recovered {
		if err := c.emitParsedTrade(ctx, trades, parsed); err != nil {
			return err
		}
	}
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, parsed := range buffered {
		err := c.emitParsedTrade(ctx, trades, parsed)
		backlog.release()
		if err != nil {
			return err
		}
	}
	for parsed := range stream {
		err := c.emitParsedTrade(ctx, trades, parsed)
		backlog.release()
		if err != nil {
			return err
		}
	}
	cancelStream()
	err = <-streamErr
	if isNormalWebSocketClose(err) || err == nil {
		return nil
	}
	return err
}

func (c *Coinbase) awaitSubscription(
	ctx context.Context,
	conn *websocket.Conn,
) ([]coinbaseParsedTrade, error) {
	ackCtx, cancel := context.WithTimeout(ctx, c.SubscriptionTimeout)
	defer cancel()

	buffered := make([]coinbaseParsedTrade, 0)
	for {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("coinbase subscription acknowledgement timed out after %s", c.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read coinbase subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		handled, err := c.parseControlMessage(data)
		if err != nil {
			return nil, err
		}
		if handled {
			if c.subscribed {
				return buffered, nil
			}
			continue
		}
		parsed, ok, err := parseCoinbaseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if len(buffered) >= coinbaseSubscriptionBufferLimit {
			return nil, fmt.Errorf(
				"coinbase pre-acknowledgement trade buffer exceeded %d events",
				coinbaseSubscriptionBufferLimit,
			)
		}
		buffered = append(buffered, parsed)
	}
}

func (c *Coinbase) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
	backlog *tradeBacklog,
) (<-chan coinbaseParsedTrade, <-chan error) {
	trades := make(chan coinbaseParsedTrade, coinbaseSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read coinbase websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}
			handled, err := c.parseControlMessage(data)
			if err != nil {
				errs <- err
				return
			}
			if handled {
				continue
			}
			parsed, ok, err := parseCoinbaseTradeMessage(data, time.Now().UTC())
			if err != nil {
				errs <- err
				return
			}
			if !ok {
				continue
			}
			if !backlog.reserve() {
				errs <- fmt.Errorf(
					"coinbase websocket trade buffer exceeded %d events",
					coinbaseSubscriptionBufferLimit,
				)
				_ = conn.CloseNow()
				return
			}
			select {
			case trades <- parsed:
			case <-ctx.Done():
				backlog.release()
				errs <- ctx.Err()
				return
			}
		}
	}()
	return trades, errs
}

func (c *Coinbase) emitParsedTrade(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	parsed coinbaseParsedTrade,
) error {
	if c.seen == nil {
		c.seen = newSeenTradeIDs(coinbaseSeenTradeLimit)
	}
	if !c.seen.Add(parsed.event.Symbol, parsed.event.UID) {
		return nil
	}
	previousID, hadPreviousID := c.lastIDs[parsed.event.Symbol]
	parsed.event.IsSequential = hadPreviousID && parsed.tradeID == previousID+1
	if err := sendTrade(ctx, trades, parsed.event); err != nil {
		return err
	}
	if !hadPreviousID || parsed.tradeID > previousID {
		c.lastIDs[parsed.event.Symbol] = parsed.tradeID
	}
	return nil
}
