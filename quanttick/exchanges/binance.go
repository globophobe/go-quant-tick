package exchanges

import (
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
	BinanceName = "binance"
	BinanceURL  = "wss://stream.binance.com:9443/ws"
)

var _ quanttick.Exchange = (*Binance)(nil)

type Binance struct {
	Symbols        []string
	URL            string
	ReconnectDelay time.Duration

	lastIDs map[string]int64
}

type BinanceOption func(*Binance)

func NewBinance(symbols []string, options ...BinanceOption) *Binance {
	exchange := &Binance{
		Symbols:        append([]string(nil), symbols...),
		URL:            BinanceURL,
		ReconnectDelay: time.Second,
		lastIDs:        make(map[string]int64),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBinanceURL(url string) BinanceOption {
	return func(exchange *Binance) {
		exchange.URL = url
	}
}

func WithBinanceReconnectDelay(delay time.Duration) BinanceOption {
	return func(exchange *Binance) {
		exchange.ReconnectDelay = delay
	}
}

func (b *Binance) Name() string {
	return BinanceName
}

func (b *Binance) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
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

func (b *Binance) SubscriptionMessages() []map[string]any {
	params := make([]string, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		params = append(params, strings.ToLower(symbol)+"@trade")
	}

	return []map[string]any{
		{
			"method": "SUBSCRIBE",
			"params": params,
			"id":     1,
		},
	}
}

func (b *Binance) ParseTradeMessage(data []byte, receivedAt time.Time) (quanttick.TradeEvent, bool, error) {
	eventType, ok, err := binanceEventType(data)
	if err != nil {
		return quanttick.TradeEvent{}, false, err
	}
	if !ok || eventType != "trade" {
		return quanttick.TradeEvent{}, false, nil
	}

	var msg binanceTradeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return quanttick.TradeEvent{}, false, fmt.Errorf("parse binance message: %w", err)
	}

	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return quanttick.TradeEvent{}, false, fmt.Errorf("parse binance price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Quantity)
	if err != nil {
		return quanttick.TradeEvent{}, false, fmt.Errorf("parse binance quantity: %w", err)
	}

	prevID, hadPrevID := b.lastIDs[msg.Symbol]
	b.lastIDs[msg.Symbol] = msg.TradeID
	tickRule := 1
	if msg.BuyerIsMaker {
		tickRule = -1
	}

	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     BinanceName,
		UID:          strconv.FormatInt(msg.TradeID, 10),
		Symbol:       msg.Symbol,
		Timestamp:    time.UnixMilli(msg.TradeTime).UTC(),
		ReceivedAt:   receivedAt,
		Price:        price,
		Notional:     notional,
		TickRule:     tickRule,
		IsSequential: !hadPrevID || msg.TradeID == prevID+1,
	}), true, nil
}

func (b *Binance) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, b.URL, nil)
	if err != nil {
		return fmt.Errorf("dial binance websocket: %w", err)
	}
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal binance subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send binance subscription: %w", err)
		}
	}

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read binance websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		trade, ok, err := b.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		select {
		case trades <- trade:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type binanceTradeMessage struct {
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	TradeTime    int64  `json:"T"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	BuyerIsMaker bool   `json:"m"`
}

func binanceEventType(data []byte) (string, bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", false, fmt.Errorf("parse binance envelope: %w", err)
	}
	eventRaw, ok := envelope["e"]
	if !ok || len(eventRaw) == 0 {
		return "", false, nil
	}

	var eventType string
	if err := json.Unmarshal(eventRaw, &eventType); err != nil {
		return "", false, nil
	}
	return eventType, true, nil
}

func sendError(ctx context.Context, errs chan<- error, err error) {
	select {
	case errs <- err:
	case <-ctx.Done():
	default:
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
