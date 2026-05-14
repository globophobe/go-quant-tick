package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BitfinexName = "bitfinex"
	BitfinexURL  = "wss://api-pub.bitfinex.com/ws/2"
)

var _ quanttick.Exchange = (*Bitfinex)(nil)

type Bitfinex struct {
	Symbols        []string
	URL            string
	ReconnectDelay time.Duration

	channelSymbols map[int64]string
	lastIDs        map[string]int64
}

type BitfinexOption func(*Bitfinex)

func NewBitfinex(symbols []string, options ...BitfinexOption) *Bitfinex {
	exchange := &Bitfinex{
		Symbols:        append([]string(nil), symbols...),
		URL:            BitfinexURL,
		ReconnectDelay: time.Second,
		channelSymbols: make(map[int64]string),
		lastIDs:        make(map[string]int64),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBitfinexURL(url string) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.URL = url
	}
}

func WithBitfinexReconnectDelay(delay time.Duration) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.ReconnectDelay = delay
	}
}

func (b *Bitfinex) Name() string {
	return BitfinexName
}

func (b *Bitfinex) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(trades)
		defer close(errs)

		for {
			if err := b.run(ctx, trades); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				sendError(ctx, errs, err)
			}

			if err := sleepContext(ctx, b.ReconnectDelay); err != nil {
				return
			}
		}
	}()

	return trades, errs
}

func (b *Bitfinex) SubscriptionMessages() []map[string]any {
	messages := make([]map[string]any, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		messages = append(messages, map[string]any{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  b.APISymbol(symbol),
		})
	}
	return messages
}

func (b *Bitfinex) APISymbol(symbol string) string {
	if strings.HasPrefix(symbol, "t") {
		return symbol
	}
	return "t" + symbol
}

func (b *Bitfinex) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}

	switch data[0] {
	case '{':
		return b.parseEventMessage(data)
	case '[':
		return b.parseArrayMessage(data, receivedAt)
	default:
		return nil, nil
	}
}

func (b *Bitfinex) parseEventMessage(data []byte) ([]quanttick.TradeEvent, error) {
	var msg bitfinexEventMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("parse bitfinex event: %w", err)
	}
	if msg.Event == "subscribed" {
		channelID, err := parseRawInt64(msg.ChannelID)
		if err != nil {
			return nil, fmt.Errorf("parse bitfinex subscribed channel id: %w", err)
		}
		b.channelSymbols[channelID] = msg.Symbol
	}
	return nil, nil
}

func (b *Bitfinex) parseArrayMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var msg []json.RawMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("parse bitfinex array: %w", err)
	}
	if len(msg) < 2 {
		return nil, nil
	}

	channelID, err := parseRawInt64(msg[0])
	if err != nil {
		return nil, fmt.Errorf("parse bitfinex channel id: %w", err)
	}

	symbol := b.channelSymbols[channelID]
	if symbol == "" {
		return nil, nil
	}

	tag, ok, err := parseBitfinexTag(msg[1])
	if err != nil {
		return nil, err
	}
	if !ok || tag == "hb" || tag == "te" {
		return nil, nil
	}
	if tag != "tu" || len(msg) < 3 {
		return nil, nil
	}

	var rawTrade []json.RawMessage
	if err := json.Unmarshal(msg[2], &rawTrade); err != nil {
		return nil, fmt.Errorf("parse bitfinex trade: %w", err)
	}
	if len(rawTrade) < 4 {
		return nil, nil
	}

	tradeID, err := parseRawInt64(rawTrade[0])
	if err != nil {
		return nil, fmt.Errorf("parse bitfinex trade id: %w", err)
	}
	timestampMillis, err := parseRawInt64(rawTrade[1])
	if err != nil {
		return nil, fmt.Errorf("parse bitfinex timestamp: %w", err)
	}
	notional, err := parseRawDecimal(rawTrade[2])
	if err != nil {
		return nil, fmt.Errorf("parse bitfinex notional: %w", err)
	}
	price, err := parseRawDecimal(rawTrade[3])
	if err != nil {
		return nil, fmt.Errorf("parse bitfinex price: %w", err)
	}

	prevID, hadPrevID := b.lastIDs[symbol]
	b.lastIDs[symbol] = tradeID
	tickRule := 1
	if notional.IsNegative() {
		tickRule = -1
		notional = notional.Abs()
	}

	return []quanttick.TradeEvent{
		quanttick.NewTradeEvent(quanttick.TradeEventInput{
			Exchange:     BitfinexName,
			UID:          strconv.FormatInt(tradeID, 10),
			Symbol:       symbol,
			Timestamp:    time.UnixMilli(timestampMillis).UTC(),
			ReceivedAt:   receivedAt,
			Price:        price,
			Notional:     notional,
			TickRule:     tickRule,
			IsSequential: hadPrevID && tradeID > prevID,
		}),
	}, nil
}

func (b *Bitfinex) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, b.URL, nil)
	if err != nil {
		return fmt.Errorf("dial bitfinex websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal bitfinex subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send bitfinex subscription: %w", err)
		}
	}

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			if isNormalWebSocketClose(err) {
				return nil
			}
			return fmt.Errorf("read bitfinex websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		parsedTrades, err := b.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return err
		}
		for _, trade := range parsedTrades {
			select {
			case trades <- trade:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

type bitfinexEventMessage struct {
	Event     string          `json:"event"`
	ChannelID json.RawMessage `json:"chanId"`
	Symbol    string          `json:"symbol"`
}

func parseBitfinexTag(raw json.RawMessage) (string, bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return "", false, nil
	}
	if raw[0] == '[' {
		return "", false, nil
	}

	var tag string
	if err := json.Unmarshal(raw, &tag); err != nil {
		return "", false, fmt.Errorf("parse bitfinex tag: %w", err)
	}
	return tag, true, nil
}

func parseRawInt64(raw json.RawMessage) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing integer value")
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return 0, err
		}
		return strconv.ParseInt(value, 10, 64)
	}
	return strconv.ParseInt(string(raw), 10, 64)
}
