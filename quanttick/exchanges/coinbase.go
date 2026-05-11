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
	CoinbaseName = "coinbase"
	CoinbaseURL  = "wss://ws-feed.exchange.coinbase.com"
)

var _ quanttick.Exchange = (*Coinbase)(nil)

type Coinbase struct {
	Symbols        []string
	URL            string
	ReconnectDelay time.Duration

	lastIDs map[string]int64
}

type CoinbaseOption func(*Coinbase)

func NewCoinbase(symbols []string, options ...CoinbaseOption) *Coinbase {
	exchange := &Coinbase{
		Symbols:        append([]string(nil), symbols...),
		URL:            CoinbaseURL,
		ReconnectDelay: time.Second,
		lastIDs:        make(map[string]int64),
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

func WithCoinbaseReconnectDelay(delay time.Duration) CoinbaseOption {
	return func(exchange *Coinbase) {
		exchange.ReconnectDelay = delay
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

		for {
			if err := c.run(ctx, trades); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				sendError(ctx, errs, err)
			}

			if err := sleepContext(ctx, c.ReconnectDelay); err != nil {
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

func (c *Coinbase) ParseTradeMessage(data []byte, receivedAt time.Time) (quanttick.TradeEvent, bool, error) {
	msg, ok, err := parseCoinbaseMatchMessage(data)
	if err != nil || !ok {
		return quanttick.TradeEvent{}, ok, err
	}

	price, notional, timestamp, err := parseCoinbaseTradeFields(msg)
	if err != nil {
		return quanttick.TradeEvent{}, false, err
	}

	prevID, hadPrevID := c.lastIDs[msg.ProductID]
	c.lastIDs[msg.ProductID] = msg.TradeID
	tickRule := 1
	if strings.ToLower(msg.Side) == "sell" {
		tickRule = -1
	}

	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     CoinbaseName,
		UID:          strconv.FormatInt(msg.TradeID, 10),
		Symbol:       msg.ProductID,
		Timestamp:    timestamp.UTC(),
		ReceivedAt:   receivedAt,
		Price:        price,
		Notional:     notional,
		TickRule:     tickRule,
		IsSequential: !hadPrevID || msg.TradeID == prevID+1,
	}), true, nil
}

func parseCoinbaseMatchMessage(data []byte) (coinbaseMatchMessage, bool, error) {
	var msg coinbaseMatchMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return coinbaseMatchMessage{}, false, fmt.Errorf("parse coinbase message: %w", err)
	}
	if msg.Type != "match" && msg.Type != "last_match" {
		return coinbaseMatchMessage{}, false, nil
	}
	return msg, true, nil
}

func parseCoinbaseTradeFields(msg coinbaseMatchMessage) (quanttick.Decimal, quanttick.Decimal, time.Time, error) {
	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return quanttick.Decimal{}, quanttick.Decimal{}, time.Time{}, fmt.Errorf("parse coinbase price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Size)
	if err != nil {
		return quanttick.Decimal{}, quanttick.Decimal{}, time.Time{}, fmt.Errorf("parse coinbase size: %w", err)
	}
	timestamp, err := time.Parse(time.RFC3339Nano, msg.Time)
	if err != nil {
		return quanttick.Decimal{}, quanttick.Decimal{}, time.Time{}, fmt.Errorf("parse coinbase time: %w", err)
	}
	return price, notional, timestamp.UTC(), nil
}

func (c *Coinbase) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, c.URL, nil)
	if err != nil {
		return fmt.Errorf("dial coinbase websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
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

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			if isNormalWebSocketClose(err) {
				return nil
			}
			return fmt.Errorf("read coinbase websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		trade, ok, err := c.ParseTradeMessage(data, time.Now().UTC())
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

type coinbaseMatchMessage struct {
	Type      string `json:"type"`
	TradeID   int64  `json:"trade_id"`
	ProductID string `json:"product_id"`
	Time      string `json:"time"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
}
