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
	HyperliquidName                      = "hyperliquid"
	HyperliquidURL                       = "wss://api.hyperliquid.xyz/ws"
	HyperliquidRESTURL                   = "https://api.hyperliquid.xyz"
	hyperliquidSeenTradeLimit            = 10000
	hyperliquidSubscriptionBufferLimit   = 10000
	hyperliquidHeartbeatInterval         = 30 * time.Second
	hyperliquidRateLimitWeightPerMinute  = 1200
	hyperliquidRecentTradesBaseWeight    = 20
	hyperliquidRecentTradesRowsPerWeight = 20
	hyperliquidRecentTradesMaxRows       = 10
	hyperliquidRecentTradesMaxWeight     = hyperliquidRecentTradesBaseWeight +
		(hyperliquidRecentTradesMaxRows+hyperliquidRecentTradesRowsPerWeight-1)/
			hyperliquidRecentTradesRowsPerWeight
)

var _ quanttick.Exchange = (*Hyperliquid)(nil)

type Hyperliquid struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	HeartbeatInterval   time.Duration
	lastUIDs            map[string]string
	recoveryLimiter     *requestWindowLimiter
}

type HyperliquidOption func(*Hyperliquid)

func NewHyperliquid(symbols []string, options ...HyperliquidOption) *Hyperliquid {
	exchange := &Hyperliquid{
		Symbols:             append([]string(nil), symbols...),
		URL:                 HyperliquidURL,
		RESTURL:             HyperliquidRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		HeartbeatInterval:   hyperliquidHeartbeatInterval,
		lastUIDs:            make(map[string]string),
		recoveryLimiter:     newRequestWindowLimiter(hyperliquidRateLimitWeightPerMinute, time.Minute),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithHyperliquidURL(url string) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.URL = url
	}
}

func WithHyperliquidRESTURL(url string) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.RESTURL = url
	}
}

func WithHyperliquidHTTPClient(client *http.Client) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.HTTPClient = client
	}
}

func WithHyperliquidReconnectDelay(delay time.Duration) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.ReconnectDelay = delay
	}
}

func WithHyperliquidSubscriptionTimeout(timeout time.Duration) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.SubscriptionTimeout = timeout
	}
}

func WithHyperliquidHeartbeatInterval(interval time.Duration) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.HeartbeatInterval = interval
	}
}

func (h *Hyperliquid) Name() string {
	return HyperliquidName
}

func (h *Hyperliquid) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)
	seen := newSeenTradeIDs(hyperliquidSeenTradeLimit)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(h.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := h.runWithErrors(ctx, trades, seen, errs); err != nil {
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

func (h *Hyperliquid) SubscriptionMessages() []map[string]any {
	messages := make([]map[string]any, 0, len(h.Symbols))
	for _, symbol := range h.Symbols {
		messages = append(messages, map[string]any{
			"method": "subscribe",
			"subscription": map[string]any{
				"type": "trades",
				"coin": symbol,
			},
		})
	}
	return messages
}

func (h *Hyperliquid) run(ctx context.Context, trades chan<- quanttick.TradeEvent, seen *seenTradeIDs) error {
	return h.runWithErrors(ctx, trades, seen, nil)
}

func (h *Hyperliquid) runWithErrors(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	seen *seenTradeIDs,
	errs chan<- error,
) error {
	conn, _, err := websocket.Dial(ctx, h.URL, nil)
	if err != nil {
		return fmt.Errorf("dial hyperliquid websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range h.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal hyperliquid subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send hyperliquid subscription: %w", err)
		}
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatErr, heartbeatDone := h.startHeartbeats(heartbeatCtx, conn)
	defer func() {
		cancelHeartbeat()
		<-heartbeatDone
	}()

	buffered, err := h.awaitSubscriptions(ctx, conn)
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, streamErr := h.startTradeReader(streamCtx, conn)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, recoveryErr := h.recoverTrades(recoveryCtx)
	cancelRecovery()
	for _, trade := range recovered {
		if err := emitSeenTrade(ctx, trades, seen, h.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, trade := range buffered {
		if err := emitSeenTrade(ctx, trades, seen, h.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	for trade := range stream {
		if err := emitSeenTrade(ctx, trades, seen, h.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	cancelStream()
	if heartbeatErr := takeHyperliquidHeartbeatError(heartbeatErr); heartbeatErr != nil {
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

func (h *Hyperliquid) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent, hyperliquidSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read hyperliquid websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}

			if err := parseHyperliquidProtocolError(data); err != nil {
				errs <- err
				return
			}
			parsedTrades, err := h.ParseTradeMessage(data, time.Now().UTC())
			if err != nil {
				errs <- err
				return
			}
			for _, trade := range parsedTrades {
				select {
				case trades <- trade:
				case <-ctx.Done():
					errs <- ctx.Err()
					return
				default:
					errs <- fmt.Errorf(
						"hyperliquid websocket trade buffer exceeded %d events",
						hyperliquidSubscriptionBufferLimit,
					)
					_ = conn.CloseNow()
					return
				}
			}
		}
	}()
	return trades, errs
}

func (h *Hyperliquid) awaitSubscriptions(ctx context.Context, conn *websocket.Conn) ([]quanttick.TradeEvent, error) {
	expected := make(map[string]struct{}, len(h.Symbols))
	for _, symbol := range h.Symbols {
		expected[symbol] = struct{}{}
	}
	if len(expected) == 0 {
		return nil, nil
	}

	ackCtx, cancel := context.WithTimeout(ctx, h.SubscriptionTimeout)
	defer cancel()

	acked := make(map[string]struct{}, len(expected))
	buffered := make([]quanttick.TradeEvent, 0)
	for len(acked) < len(expected) {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("hyperliquid subscription acknowledgement timed out after %s", h.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read hyperliquid subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		coin, isAck, err := parseHyperliquidSubscriptionAck(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			if _, ok := expected[coin]; !ok {
				return nil, fmt.Errorf("hyperliquid acknowledged unexpected trades subscription %q", coin)
			}
			acked[coin] = struct{}{}
			continue
		}

		parsedTrades, err := h.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if len(buffered)+len(parsedTrades) > hyperliquidSubscriptionBufferLimit {
			return nil, fmt.Errorf("hyperliquid subscription trade buffer exceeded %d events", hyperliquidSubscriptionBufferLimit)
		}
		buffered = append(buffered, parsedTrades...)
	}
	return buffered, nil
}

func (h *Hyperliquid) startHeartbeats(ctx context.Context, conn *websocket.Conn) (<-chan error, <-chan struct{}) {
	errs := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if h.HeartbeatInterval <= 0 {
			return
		}

		ticker := time.NewTicker(h.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.Write(ctx, websocket.MessageText, []byte("{\"method\":\"ping\"}")); err != nil {
					select {
					case errs <- fmt.Errorf("send hyperliquid heartbeat: %w", err):
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

func takeHyperliquidHeartbeatError(errs <-chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}
