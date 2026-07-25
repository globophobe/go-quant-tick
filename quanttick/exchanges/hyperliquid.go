package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	HyperliquidName                    = "hyperliquid"
	HyperliquidURL                     = "wss://api.hyperliquid.xyz/ws"
	hyperliquidSeenTradeLimit          = 10000
	hyperliquidSubscriptionBufferLimit = 10000
	hyperliquidHeartbeatInterval       = 30 * time.Second
)

var _ quanttick.Exchange = (*Hyperliquid)(nil)

type Hyperliquid struct {
	Symbols             []string
	URL                 string
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	HeartbeatInterval   time.Duration
}

type HyperliquidOption func(*Hyperliquid)

func NewHyperliquid(symbols []string, options ...HyperliquidOption) *Hyperliquid {
	exchange := &Hyperliquid{
		Symbols:             append([]string(nil), symbols...),
		URL:                 HyperliquidURL,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		HeartbeatInterval:   hyperliquidHeartbeatInterval,
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
			if err := h.run(ctx, trades, seen); err != nil {
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

func (h *Hyperliquid) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel != "trades" {
		return nil, nil
	}

	var rawTrades []hyperliquidTrade
	if err := json.Unmarshal(envelope.Data, &rawTrades); err != nil {
		return nil, fmt.Errorf("parse hyperliquid trades: %w", err)
	}

	trades := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		price, err := quanttick.ParseDecimal(rawTrade.Price)
		if err != nil {
			return nil, fmt.Errorf("parse hyperliquid price: %w", err)
		}
		notional, err := quanttick.ParseDecimal(rawTrade.Size)
		if err != nil {
			return nil, fmt.Errorf("parse hyperliquid size: %w", err)
		}

		tickRule, err := hyperliquidTickRule(rawTrade.Side)
		if err != nil {
			return nil, err
		}

		trades = append(trades, quanttick.NewTradeEvent(quanttick.TradeEventInput{
			Exchange:   HyperliquidName,
			UID:        strconv.FormatInt(rawTrade.Time, 10) + ":" + rawTrade.Coin + ":" + strconv.FormatInt(rawTrade.TradeID, 10),
			Symbol:     rawTrade.Coin,
			Timestamp:  time.UnixMilli(rawTrade.Time).UTC(),
			ReceivedAt: receivedAt,
			Price:      price,
			Notional:   notional,
			TickRule:   tickRule,
		}))
	}

	return trades, nil
}

func (h *Hyperliquid) run(ctx context.Context, trades chan<- quanttick.TradeEvent, seen *seenTradeIDs) error {
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
	for _, trade := range buffered {
		if !seen.Add(trade.Symbol, trade.UID) {
			continue
		}
		if err := sendTrade(ctx, trades, trade); err != nil {
			return err
		}
	}

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			if heartbeatErr := takeHyperliquidHeartbeatError(heartbeatErr); heartbeatErr != nil {
				return heartbeatErr
			}
			if isNormalWebSocketClose(err) {
				return nil
			}
			return fmt.Errorf("read hyperliquid websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		if err := parseHyperliquidProtocolError(data); err != nil {
			return err
		}
		parsedTrades, err := h.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return err
		}
		for _, trade := range parsedTrades {
			if !seen.Add(trade.Symbol, trade.UID) {
				continue
			}
			if err := sendTrade(ctx, trades, trade); err != nil {
				return err
			}
		}
	}
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

func parseHyperliquidSubscriptionAck(data []byte) (string, bool, error) {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", false, fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel == "error" {
		return "", false, hyperliquidProtocolError(envelope.Data)
	}
	if envelope.Channel != "subscriptionResponse" {
		return "", false, nil
	}

	var response hyperliquidSubscriptionResponse
	if err := json.Unmarshal(envelope.Data, &response); err != nil {
		return "", false, fmt.Errorf("parse hyperliquid subscription acknowledgement: %w", err)
	}
	if response.Method != "subscribe" || response.Subscription.Type != "trades" || response.Subscription.Coin == "" {
		return "", false, fmt.Errorf("invalid hyperliquid subscription acknowledgement: %s", string(data))
	}
	return response.Subscription.Coin, true, nil
}

func parseHyperliquidProtocolError(data []byte) error {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel != "error" {
		return nil
	}
	return hyperliquidProtocolError(envelope.Data)
}

func hyperliquidProtocolError(data json.RawMessage) error {
	var message string
	if err := json.Unmarshal(data, &message); err == nil {
		return fmt.Errorf("hyperliquid websocket error: %s", message)
	}
	return fmt.Errorf("hyperliquid websocket error: %s", string(data))
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

func hyperliquidTickRule(side string) (int, error) {
	switch strings.ToLower(side) {
	case "b", "buy":
		return 1, nil
	case "a", "sell":
		return -1, nil
	default:
		return 0, fmt.Errorf("invalid hyperliquid side %q", side)
	}
}

func sendTrade(ctx context.Context, trades chan<- quanttick.TradeEvent, trade quanttick.TradeEvent) error {
	select {
	case trades <- trade:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type hyperliquidEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type hyperliquidTrade struct {
	Coin    string `json:"coin"`
	Side    string `json:"side"`
	Price   string `json:"px"`
	Size    string `json:"sz"`
	Time    int64  `json:"time"`
	TradeID int64  `json:"tid"`
}

type hyperliquidSubscriptionResponse struct {
	Method       string `json:"method"`
	Subscription struct {
		Type string `json:"type"`
		Coin string `json:"coin"`
	} `json:"subscription"`
}
