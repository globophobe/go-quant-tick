package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BybitName                    = "bybit"
	BybitURL                     = "wss://stream.bybit.com/v5/public/linear"
	bybitSubscriptionRequestID   = "trades"
	bybitSeenTradeLimit          = 10000
	bybitSubscriptionBufferLimit = 10000
	bybitHeartbeatInterval       = 20 * time.Second
)

var _ quanttick.Exchange = (*Bybit)(nil)

type Bybit struct {
	Symbols             []string
	URL                 string
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	HeartbeatInterval   time.Duration
}

type BybitOption func(*Bybit)

func NewBybit(symbols []string, options ...BybitOption) *Bybit {
	exchange := &Bybit{
		Symbols:             append([]string(nil), symbols...),
		URL:                 BybitURL,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		HeartbeatInterval:   bybitHeartbeatInterval,
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

func (b *Bybit) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var envelope bybitEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse bybit message: %w", err)
	}
	const topicPrefix = "publicTrade."
	if !strings.HasPrefix(envelope.Topic, topicPrefix) {
		return nil, nil
	}
	if envelope.Type != "snapshot" {
		return nil, fmt.Errorf("invalid bybit trade message type %q", envelope.Type)
	}

	topicSymbol := strings.TrimPrefix(envelope.Topic, topicPrefix)
	if !b.hasSymbol(topicSymbol) {
		return nil, fmt.Errorf("bybit trade message has unexpected topic %q", envelope.Topic)
	}

	var rawTrades []bybitTrade
	if err := json.Unmarshal(envelope.Data, &rawTrades); err != nil {
		return nil, fmt.Errorf("parse bybit trades: %w", err)
	}

	trades := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		if rawTrade.Symbol != topicSymbol {
			return nil, fmt.Errorf(
				"bybit trade symbol %q does not match topic %q",
				rawTrade.Symbol,
				envelope.Topic,
			)
		}
		if rawTrade.TradeID == "" {
			return nil, fmt.Errorf("bybit trade for %q has no trade id", rawTrade.Symbol)
		}

		price, err := quanttick.ParseDecimal(rawTrade.Price)
		if err != nil {
			return nil, fmt.Errorf("parse bybit price: %w", err)
		}
		notional, err := quanttick.ParseDecimal(rawTrade.Size)
		if err != nil {
			return nil, fmt.Errorf("parse bybit size: %w", err)
		}
		tickRule, err := bybitTickRule(rawTrade.Side)
		if err != nil {
			return nil, err
		}

		trades = append(trades, quanttick.NewTradeEvent(quanttick.TradeEventInput{
			Exchange:   BybitName,
			UID:        rawTrade.TradeID,
			Symbol:     rawTrade.Symbol,
			Timestamp:  time.UnixMilli(rawTrade.TradeTime).UTC(),
			ReceivedAt: receivedAt,
			Price:      price,
			Notional:   notional,
			TickRule:   tickRule,
		}))
	}
	return trades, nil
}

func (b *Bybit) run(ctx context.Context, trades chan<- quanttick.TradeEvent, seen *seenTradeIDs) error {
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
			if heartbeatErr := takeBybitHeartbeatError(heartbeatErr); heartbeatErr != nil {
				return heartbeatErr
			}
			if isNormalWebSocketClose(err) {
				return nil
			}
			return fmt.Errorf("read bybit websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		isAck, err := parseBybitSubscriptionResponse(data)
		if err != nil {
			return err
		}
		if isAck {
			continue
		}
		parsedTrades, err := b.ParseTradeMessage(data, time.Now().UTC())
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

func parseBybitSubscriptionResponse(data []byte) (bool, error) {
	var envelope bybitEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("parse bybit message: %w", err)
	}
	if envelope.Success != nil && !*envelope.Success {
		return false, fmt.Errorf("bybit websocket %s failed: %s", envelope.Op, envelope.ReturnMessage)
	}
	if envelope.Op != "subscribe" {
		return false, nil
	}
	if envelope.Success == nil || !*envelope.Success {
		return false, fmt.Errorf("invalid bybit subscription response: %s", string(data))
	}
	if envelope.RequestID != bybitSubscriptionRequestID {
		return false, fmt.Errorf("bybit subscription response has unexpected request id %q", envelope.RequestID)
	}
	return true, nil
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

func bybitTickRule(side string) (int, error) {
	switch strings.ToLower(side) {
	case "buy":
		return 1, nil
	case "sell":
		return -1, nil
	default:
		return 0, fmt.Errorf("invalid bybit side %q", side)
	}
}

type bybitEnvelope struct {
	Topic         string          `json:"topic"`
	Type          string          `json:"type"`
	Data          json.RawMessage `json:"data"`
	Success       *bool           `json:"success"`
	ReturnMessage string          `json:"ret_msg"`
	RequestID     string          `json:"req_id"`
	Op            string          `json:"op"`
}

type bybitTrade struct {
	TradeTime int64  `json:"T"`
	Symbol    string `json:"s"`
	Side      string `json:"S"`
	Size      string `json:"v"`
	Price     string `json:"p"`
	TradeID   string `json:"i"`
	Sequence  int64  `json:"seq"`
}
