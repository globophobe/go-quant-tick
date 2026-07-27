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
	DeribitName                    = "deribit"
	DeribitURL                     = "wss://www.deribit.com/ws/api/v2"
	DeribitRESTURL                 = "https://www.deribit.com/api/v2/public"
	deribitSubscriptionRequestID   = 1
	deribitSeenTradeLimit          = 10000
	deribitSubscriptionBufferLimit = 10000
	deribitRecoveryPageLimit       = 1000
	deribitRecoveryMaxPages        = 100
	deribitRecoveryRequestInterval = 50 * time.Millisecond
)

var _ quanttick.Exchange = (*Deribit)(nil)

type Deribit struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration

	lastSequences    map[string]int64
	seen             *seenTradeIDs
	recoveryThrottle *restThrottle
}

type DeribitOption func(*Deribit)

func NewDeribit(symbols []string, options ...DeribitOption) *Deribit {
	exchange := &Deribit{
		Symbols:             append([]string(nil), symbols...),
		URL:                 DeribitURL,
		RESTURL:             DeribitRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		lastSequences:       make(map[string]int64),
		seen:                newSeenTradeIDs(deribitSeenTradeLimit),
		recoveryThrottle:    newRESTThrottle(deribitRecoveryRequestInterval),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithDeribitURL(url string) DeribitOption {
	return func(exchange *Deribit) {
		exchange.URL = url
	}
}

func WithDeribitRESTURL(url string) DeribitOption {
	return func(exchange *Deribit) {
		exchange.RESTURL = url
	}
}

func WithDeribitHTTPClient(client *http.Client) DeribitOption {
	return func(exchange *Deribit) {
		exchange.HTTPClient = client
	}
}

func WithDeribitReconnectDelay(delay time.Duration) DeribitOption {
	return func(exchange *Deribit) {
		exchange.ReconnectDelay = delay
	}
}

func WithDeribitSubscriptionTimeout(timeout time.Duration) DeribitOption {
	return func(exchange *Deribit) {
		exchange.SubscriptionTimeout = timeout
	}
}

func (d *Deribit) Name() string {
	return DeribitName
}

func (d *Deribit) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(d.ReconnectDelay)
		for {
			startedAt := time.Now()
			if err := d.run(ctx, trades, errs); err != nil {
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

func (d *Deribit) SubscriptionMessages() []map[string]any {
	channels := make([]string, 0, len(d.Symbols))
	for _, symbol := range d.Symbols {
		channels = append(channels, deribitTradeChannel(symbol))
	}
	return []map[string]any{
		{
			"jsonrpc": "2.0",
			"id":      deribitSubscriptionRequestID,
			"method":  "public/subscribe",
			"params": map[string]any{
				"channels": channels,
			},
		},
	}
}

func (d *Deribit) run(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	errs chan<- error,
) error {
	conn, _, err := websocket.Dial(ctx, d.URL, nil)
	if err != nil {
		return fmt.Errorf("dial deribit websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range d.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal deribit subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send deribit subscription: %w", err)
		}
	}

	buffered, err := d.awaitSubscription(ctx, conn)
	if err != nil {
		return err
	}
	backlog, err := newTradeBacklog(deribitSubscriptionBufferLimit, len(buffered))
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, streamErr := d.startTradeReader(streamCtx, conn, backlog)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, recoveryErr := d.recoverTrades(recoveryCtx)
	cancelRecovery()
	for _, parsed := range recovered {
		if err := d.emitParsedTrade(ctx, trades, parsed); err != nil {
			return err
		}
	}
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, parsed := range buffered {
		err := d.emitParsedTrade(ctx, trades, parsed)
		backlog.release()
		if err != nil {
			return err
		}
	}
	for parsed := range stream {
		err := d.emitParsedTrade(ctx, trades, parsed)
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

func (d *Deribit) awaitSubscription(
	ctx context.Context,
	conn *websocket.Conn,
) ([]deribitParsedTrade, error) {
	ackCtx, cancel := context.WithTimeout(ctx, d.SubscriptionTimeout)
	defer cancel()
	buffered := make([]deribitParsedTrade, 0)
	for {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("deribit subscription acknowledgement timed out after %s", d.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read deribit subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}
		isAck, err := d.parseSubscriptionResponse(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			return buffered, nil
		}
		parsed, err := d.parseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if len(buffered)+len(parsed) > deribitSubscriptionBufferLimit {
			return nil, fmt.Errorf(
				"deribit pre-acknowledgement trade buffer exceeded %d events",
				deribitSubscriptionBufferLimit,
			)
		}
		buffered = append(buffered, parsed...)
	}
}

func (d *Deribit) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
	backlog *tradeBacklog,
) (<-chan deribitParsedTrade, <-chan error) {
	trades := make(chan deribitParsedTrade, deribitSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read deribit websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}
			isAck, err := d.parseSubscriptionResponse(data)
			if err != nil {
				errs <- err
				return
			}
			if isAck {
				continue
			}
			parsed, err := d.parseTradeMessage(data, time.Now().UTC())
			if err != nil {
				errs <- err
				return
			}
			for _, trade := range parsed {
				if !backlog.reserve() {
					errs <- fmt.Errorf(
						"deribit websocket trade buffer exceeded %d events",
						deribitSubscriptionBufferLimit,
					)
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

func (d *Deribit) emitParsedTrade(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	parsed deribitParsedTrade,
) error {
	if d.seen == nil {
		d.seen = newSeenTradeIDs(deribitSeenTradeLimit)
	}
	if !d.seen.Add(parsed.event.Symbol, parsed.event.UID) {
		return nil
	}
	previous, hadPrevious := d.lastSequences[parsed.event.Symbol]
	parsed.event.IsSequential = hadPrevious && parsed.sequence == previous+1
	if err := sendTrade(ctx, trades, parsed.event); err != nil {
		return err
	}
	if !hadPrevious || parsed.sequence > previous {
		d.lastSequences[parsed.event.Symbol] = parsed.sequence
	}
	return nil
}
