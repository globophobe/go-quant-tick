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
	BybitName                    = "bybit"
	BybitURL                     = "wss://stream.bybit.com/v5/public/linear"
	BybitRESTURL                 = "https://api.bybit.com"
	bybitSubscriptionRequestID   = "trades"
	bybitSeenTradeLimit          = 10000
	bybitSubscriptionBufferLimit = 10000
	bybitHeartbeatInterval       = 20 * time.Second
	bybitRecoveryRetryDelay      = 250 * time.Millisecond
	bybitRecoveryRateLimitDelay  = time.Second
	bybitIPBanDelay              = 10 * time.Minute
	bybitIPRequestLimit          = 600
	bybitIPRequestWindow         = 5 * time.Second
)

var _ quanttick.Exchange = (*Bybit)(nil)

type Bybit struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	HeartbeatInterval   time.Duration
	lastUIDs            map[string]string
	recoveryThrottle    *restThrottle
	recoveryIPLimiter   *requestWindowLimiter
}

type BybitOption func(*Bybit)

func NewBybit(symbols []string, options ...BybitOption) *Bybit {
	exchange := &Bybit{
		Symbols:             append([]string(nil), symbols...),
		URL:                 BybitURL,
		RESTURL:             BybitRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		HeartbeatInterval:   bybitHeartbeatInterval,
		lastUIDs:            make(map[string]string),
		recoveryThrottle:    newRESTThrottle(0),
		recoveryIPLimiter:   newRequestWindowLimiter(bybitIPRequestLimit, bybitIPRequestWindow),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBybitURL(url string) BybitOption {
	return func(exchange *Bybit) {
		exchange.URL = url
	}
}

func WithBybitRESTURL(url string) BybitOption {
	return func(exchange *Bybit) {
		exchange.RESTURL = url
	}
}

func WithBybitHTTPClient(client *http.Client) BybitOption {
	return func(exchange *Bybit) {
		exchange.HTTPClient = client
	}
}

func WithBybitReconnectDelay(delay time.Duration) BybitOption {
	return func(exchange *Bybit) {
		exchange.ReconnectDelay = delay
	}
}

func WithBybitSubscriptionTimeout(timeout time.Duration) BybitOption {
	return func(exchange *Bybit) {
		exchange.SubscriptionTimeout = timeout
	}
}

func WithBybitHeartbeatInterval(interval time.Duration) BybitOption {
	return func(exchange *Bybit) {
		exchange.HeartbeatInterval = interval
	}
}

func (b *Bybit) Name() string {
	return BybitName
}

func (b *Bybit) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)
	seen := newSeenTradeIDs(bybitSeenTradeLimit)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(b.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := b.run(ctx, trades, seen); err != nil {
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

func (b *Bybit) SubscriptionMessages() []map[string]any {
	topics := make([]string, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		topics = append(topics, "publicTrade."+symbol)
	}
	return []map[string]any{
		{
			"req_id": bybitSubscriptionRequestID,
			"op":     "subscribe",
			"args":   topics,
		},
	}
}

func (b *Bybit) run(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	seen *seenTradeIDs,
) error {
	conn, _, err := websocket.Dial(ctx, b.URL, nil)
	if err != nil {
		return fmt.Errorf("dial bybit websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal bybit subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send bybit subscription: %w", err)
		}
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatErr, heartbeatDone := b.startHeartbeats(heartbeatCtx, conn)
	defer func() {
		cancelHeartbeat()
		<-heartbeatDone
	}()

	buffered, err := b.awaitSubscription(ctx, conn)
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	backlog, err := newTradeBacklog(bybitSubscriptionBufferLimit, len(buffered))
	if err != nil {
		return err
	}
	stream, streamErr := b.startTradeReader(streamCtx, conn, backlog)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, buffered, recoveryErr := b.recoverWithWebSocketOverlap(
		recoveryCtx,
		buffered,
		stream,
		streamErr,
	)
	cancelRecovery()
	if recoveryErr != nil {
		cancelStream()
		return recoveryErr
	}
	for _, trade := range recovered {
		if err := emitSeenTrade(ctx, trades, seen, b.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	for _, trade := range buffered {
		err := emitSeenTrade(ctx, trades, seen, b.lastUIDs, trade)
		backlog.release()
		if err != nil {
			cancelStream()
			return err
		}
	}
	for trade := range stream {
		err := emitSeenTrade(ctx, trades, seen, b.lastUIDs, trade)
		backlog.release()
		if err != nil {
			cancelStream()
			return err
		}
	}
	cancelStream()
	if heartbeatErr := takeBybitHeartbeatError(heartbeatErr); heartbeatErr != nil {
		return heartbeatErr
	}
	err = <-streamErr
	if isNormalWebSocketClose(err) {
		return nil
	}
	if err == nil {
		return nil
	}
	return err
}

func (b *Bybit) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
	backlog *tradeBacklog,
) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent, bybitSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read bybit websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}

			isAck, err := parseBybitSubscriptionResponse(data)
			if err != nil {
				errs <- err
				return
			}
			if isAck {
				continue
			}
			parsedTrades, err := b.ParseTradeMessage(data, time.Now().UTC())
			if err != nil {
				errs <- err
				return
			}
			for _, trade := range parsedTrades {
				if !backlog.reserve() {
					errs <- fmt.Errorf(
						"bybit websocket trade backlog exceeded %d events",
						bybitSubscriptionBufferLimit,
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
				default:
					backlog.release()
					errs <- fmt.Errorf(
						"bybit websocket trade buffer exceeded %d events",
						bybitSubscriptionBufferLimit,
					)
					_ = conn.CloseNow()
					return
				}
			}
		}
	}()
	return trades, errs
}

func (b *Bybit) awaitSubscription(
	ctx context.Context,
	conn *websocket.Conn,
) ([]quanttick.TradeEvent, error) {
	ackCtx, cancel := context.WithTimeout(ctx, b.SubscriptionTimeout)
	defer cancel()

	buffered := make([]quanttick.TradeEvent, 0)
	for {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("bybit subscription acknowledgement timed out after %s", b.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read bybit subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		isAck, err := parseBybitSubscriptionResponse(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			return buffered, nil
		}

		parsedTrades, err := b.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if len(buffered)+len(parsedTrades) > bybitSubscriptionBufferLimit {
			return nil, fmt.Errorf("bybit subscription trade buffer exceeded %d events", bybitSubscriptionBufferLimit)
		}
		buffered = append(buffered, parsedTrades...)
	}
}

func (b *Bybit) startHeartbeats(ctx context.Context, conn *websocket.Conn) (<-chan error, <-chan struct{}) {
	errs := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if b.HeartbeatInterval <= 0 {
			return
		}

		ticker := time.NewTicker(b.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.Write(ctx, websocket.MessageText, []byte("{\"op\":\"ping\"}")); err != nil {
					select {
					case errs <- fmt.Errorf("send bybit heartbeat: %w", err):
					default:
					}
					_ = conn.CloseNow()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return errs, done
}

func takeBybitHeartbeatError(errs <-chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func (b *Bybit) hasSymbol(symbol string) bool {
	for _, configured := range b.Symbols {
		if configured == symbol {
			return true
		}
	}
	return false
}
