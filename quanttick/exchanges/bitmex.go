package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BitmexName = "bitmex"
	BitmexURL  = "wss://ws.bitmex.com/realtime"
)

var _ quanttick.Exchange = (*Bitmex)(nil)

type Bitmex struct {
	Symbols        []string
	URL            string
	ReconnectDelay time.Duration
}

type BitmexOption func(*Bitmex)

func NewBitmex(symbols []string, options ...BitmexOption) *Bitmex {
	exchange := &Bitmex{
		Symbols:        append([]string(nil), symbols...),
		URL:            BitmexURL,
		ReconnectDelay: time.Second,
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBitmexURL(url string) BitmexOption {
	return func(exchange *Bitmex) {
		exchange.URL = url
	}
}

func WithBitmexReconnectDelay(delay time.Duration) BitmexOption {
	return func(exchange *Bitmex) {
		exchange.ReconnectDelay = delay
	}
}

func (b *Bitmex) Name() string {
	return BitmexName
}

func (b *Bitmex) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
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

func (b *Bitmex) SubscriptionMessages() []map[string]any {
	args := make([]string, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		args = append(args, "trade:"+symbol)
	}

	return []map[string]any{
		{
			"op":   "subscribe",
			"args": args,
		},
	}
}

func (b *Bitmex) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var msg bitmexTradeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("parse bitmex message: %w", err)
	}
	if msg.Table != "trade" || msg.Action != "insert" {
		return nil, nil
	}

	trades := make([]quanttick.TradeEvent, 0, len(msg.Data))
	for _, rawTrade := range msg.Data {
		price, err := parseRawDecimal(rawTrade.Price)
		if err != nil {
			return nil, fmt.Errorf("parse bitmex price: %w", err)
		}

		notional, err := parseBitmexNotional(rawTrade, price)
		if err != nil {
			return nil, err
		}

		timestamp, err := time.Parse(time.RFC3339Nano, rawTrade.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse bitmex timestamp: %w", err)
		}

		tickRule := -1
		if strings.ToLower(rawTrade.Side) == "buy" {
			tickRule = 1
		}

		trades = append(trades, quanttick.NewTradeEvent(quanttick.TradeEventInput{
			Exchange:   BitmexName,
			UID:        rawTrade.TradeMatchID,
			Symbol:     rawTrade.Symbol,
			Timestamp:  timestamp.UTC(),
			ReceivedAt: receivedAt,
			Price:      price,
			Notional:   notional,
			TickRule:   tickRule,
		}))
	}

	return trades, nil
}

func (b *Bitmex) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, b.URL, nil)
	if err != nil {
		return fmt.Errorf("dial bitmex websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal bitmex subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send bitmex subscription: %w", err)
		}
	}

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read bitmex websocket: %w", err)
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

type bitmexTradeMessage struct {
	Table  string             `json:"table"`
	Action string             `json:"action"`
	Data   []bitmexTradeEntry `json:"data"`
}

type bitmexTradeEntry struct {
	TradeMatchID    string          `json:"trdMatchID"`
	Symbol          string          `json:"symbol"`
	Timestamp       string          `json:"timestamp"`
	Side            string          `json:"side"`
	Price           json.RawMessage `json:"price"`
	HomeNotional    json.RawMessage `json:"homeNotional"`
	ForeignNotional json.RawMessage `json:"foreignNotional"`
}

func parseBitmexNotional(rawTrade bitmexTradeEntry, price quanttick.Decimal) (quanttick.Decimal, error) {
	if len(rawTrade.HomeNotional) != 0 && !bytes.Equal(rawTrade.HomeNotional, []byte("null")) {
		notional, err := parseRawDecimal(rawTrade.HomeNotional)
		if err != nil {
			return quanttick.Decimal{}, fmt.Errorf("parse bitmex home notional: %w", err)
		}
		return notional, nil
	}

	foreignNotional, err := parseRawDecimal(rawTrade.ForeignNotional)
	if err != nil {
		return quanttick.Decimal{}, fmt.Errorf("parse bitmex foreign notional: %w", err)
	}
	return foreignNotional.Div(price), nil
}

func parseRawDecimal(raw json.RawMessage) (quanttick.Decimal, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return quanttick.Decimal{}, fmt.Errorf("missing decimal value")
	}
	if bytes.Equal(raw, []byte("null")) {
		return quanttick.Decimal{}, fmt.Errorf("null decimal value")
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return quanttick.Decimal{}, err
		}
		return quanttick.ParseDecimal(value)
	}
	return quanttick.ParseDecimal(string(raw))
}
