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
	CoinbaseName         = "coinbase"
	CoinbaseAdvancedName = "coinbase-advanced"
	CoinbaseURL          = "wss://ws-feed.exchange.coinbase.com"
	CoinbaseAdvancedURL  = "wss://advanced-trade-ws.coinbase.com"
)

var _ quanttick.Exchange = (*Coinbase)(nil)
var _ quanttick.Exchange = (*CoinbaseAdvanced)(nil)

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

type CoinbaseAdvanced struct {
	Symbols        []string
	name           string
	URL            string
	ReconnectDelay time.Duration
}

type CoinbaseAdvancedOption func(*CoinbaseAdvanced)

func NewCoinbaseAdvanced(symbols []string, options ...CoinbaseAdvancedOption) *CoinbaseAdvanced {
	exchange := &CoinbaseAdvanced{
		Symbols:        append([]string(nil), symbols...),
		name:           CoinbaseAdvancedName,
		URL:            CoinbaseAdvancedURL,
		ReconnectDelay: time.Second,
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithCoinbaseAdvancedURL(url string) CoinbaseAdvancedOption {
	return func(exchange *CoinbaseAdvanced) {
		exchange.URL = url
	}
}

func WithCoinbaseAdvancedReconnectDelay(delay time.Duration) CoinbaseAdvancedOption {
	return func(exchange *CoinbaseAdvanced) {
		exchange.ReconnectDelay = delay
	}
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

func (c *CoinbaseAdvanced) Name() string {
	return c.name
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

func (c *CoinbaseAdvanced) SubscriptionMessages() []map[string]any {
	return []map[string]any{
		{
			"type":        "subscribe",
			"product_ids": append([]string(nil), c.Symbols...),
			"channel":     "market_trades",
		},
	}
}

func (c *Coinbase) ParseTradeMessage(data []byte, receivedAt time.Time) (quanttick.TradeEvent, bool, error) {
	var msg coinbaseMatchMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return quanttick.TradeEvent{}, false, fmt.Errorf("parse coinbase message: %w", err)
	}
	if msg.Type != "match" && msg.Type != "last_match" {
		return quanttick.TradeEvent{}, false, nil
	}

	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return quanttick.TradeEvent{}, false, fmt.Errorf("parse coinbase price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Size)
	if err != nil {
		return quanttick.TradeEvent{}, false, fmt.Errorf("parse coinbase size: %w", err)
	}
	timestamp, err := time.Parse(time.RFC3339Nano, msg.Time)
	if err != nil {
		return quanttick.TradeEvent{}, false, fmt.Errorf("parse coinbase time: %w", err)
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

func (c *CoinbaseAdvanced) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var msg coinbaseAdvancedMarketTradesMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("parse coinbase advanced message: %w", err)
	}
	if msg.Channel != "market_trades" {
		return nil, nil
	}

	trades := make([]quanttick.TradeEvent, 0)
	for _, event := range msg.Events {
		if event.Type != "snapshot" && event.Type != "update" {
			continue
		}
		for _, rawTrade := range event.Trades {
			price, err := quanttick.ParseDecimal(rawTrade.Price)
			if err != nil {
				return nil, fmt.Errorf("parse coinbase advanced price: %w", err)
			}
			notional, err := quanttick.ParseDecimal(rawTrade.Size)
			if err != nil {
				return nil, fmt.Errorf("parse coinbase advanced size: %w", err)
			}
			timestamp, err := time.Parse(time.RFC3339Nano, rawTrade.Time)
			if err != nil {
				return nil, fmt.Errorf("parse coinbase advanced time: %w", err)
			}

			tickRule := 1
			if strings.EqualFold(rawTrade.Side, "BUY") {
				tickRule = -1
			}

			trades = append(trades, quanttick.NewTradeEvent(quanttick.TradeEventInput{
				Exchange:   c.name,
				UID:        rawTrade.TradeID,
				Symbol:     rawTrade.ProductID,
				Timestamp:  timestamp.UTC(),
				ReceivedAt: receivedAt,
				Price:      price,
				Notional:   notional,
				TickRule:   tickRule,
			}))
		}
	}

	return trades, nil
}

func (c *Coinbase) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, c.URL, nil)
	if err != nil {
		return fmt.Errorf("dial coinbase websocket: %w", err)
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

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
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

func (c *CoinbaseAdvanced) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
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

func (c *CoinbaseAdvanced) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, c.URL, nil)
	if err != nil {
		return fmt.Errorf("dial coinbase advanced websocket: %w", err)
	}
	defer conn.CloseNow()

	for _, message := range c.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal coinbase advanced subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send coinbase advanced subscription: %w", err)
		}
	}

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read coinbase advanced websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		parsedTrades, err := c.ParseTradeMessage(data, time.Now().UTC())
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

type coinbaseMatchMessage struct {
	Type      string `json:"type"`
	TradeID   int64  `json:"trade_id"`
	ProductID string `json:"product_id"`
	Time      string `json:"time"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
}

type coinbaseAdvancedMarketTradesMessage struct {
	Channel string                        `json:"channel"`
	Events  []coinbaseAdvancedTradesEvent `json:"events"`
}

type coinbaseAdvancedTradesEvent struct {
	Type   string                  `json:"type"`
	Trades []coinbaseAdvancedTrade `json:"trades"`
}

type coinbaseAdvancedTrade struct {
	TradeID   string `json:"trade_id"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
	Time      string `json:"time"`
}
