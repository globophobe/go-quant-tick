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
	CoinbaseName           = "coinbase"
	CoinbaseURL            = "wss://ws-feed.exchange.coinbase.com"
	coinbaseSeenTradeLimit = 10000
)

var _ quanttick.Exchange = (*Coinbase)(nil)

type Coinbase struct {
	Symbols        []string
	URL            string
	ReconnectDelay time.Duration

	lastIDs    map[string]int64
	seen       *seenTradeIDs
	subscribed bool
}

type CoinbaseOption func(*Coinbase)

func NewCoinbase(symbols []string, options ...CoinbaseOption) *Coinbase {
	exchange := &Coinbase{
		Symbols:        append([]string(nil), symbols...),
		URL:            CoinbaseURL,
		ReconnectDelay: time.Second,
		lastIDs:        make(map[string]int64),
		seen:           newSeenTradeIDs(coinbaseSeenTradeLimit),
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
		backoff := newReconnectBackoff(c.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := c.run(ctx, trades); err != nil {
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
	handled, err := c.parseControlMessage(data)
	if err != nil || handled {
		return quanttick.TradeEvent{}, false, err
	}

	msg, ok, err := parseCoinbaseMatchMessage(data)
	if err != nil || !ok {
		return quanttick.TradeEvent{}, ok, err
	}

	price, notional, timestamp, nanoseconds, err := parseCoinbaseTradeFields(msg)
	if err != nil {
		return quanttick.TradeEvent{}, false, err
	}

	uid := strconv.FormatInt(msg.TradeID, 10)
	if c.seen == nil {
		c.seen = newSeenTradeIDs(coinbaseSeenTradeLimit)
	}
	if !c.seen.Add(msg.ProductID, uid) {
		return quanttick.TradeEvent{}, false, nil
	}

	prevID, hadPrevID := c.lastIDs[msg.ProductID]
	if !hadPrevID || msg.TradeID > prevID {
		c.lastIDs[msg.ProductID] = msg.TradeID
	}
	tickRule := -1
	if strings.ToLower(msg.Side) == "sell" {
		tickRule = 1
	}

	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     CoinbaseName,
		UID:          uid,
		Symbol:       msg.ProductID,
		Timestamp:    timestamp,
		Nanoseconds:  nanoseconds,
		ReceivedAt:   receivedAt,
		Price:        price,
		Notional:     notional,
		TickRule:     tickRule,
		IsSequential: hadPrevID && msg.TradeID == prevID+1,
	}), true, nil
}

func (c *Coinbase) parseControlMessage(data []byte) (bool, error) {
	var msg coinbaseControlMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return false, fmt.Errorf("parse coinbase message: %w", err)
	}

	switch msg.Type {
	case "error":
		if msg.Reason != "" {
			return true, fmt.Errorf("coinbase protocol error: %s (%s)", msg.Message, msg.Reason)
		}
		return true, fmt.Errorf("coinbase protocol error: %s", msg.Message)
	case "subscriptions":
		if err := c.acceptSubscriptions(msg.Channels); err != nil {
			return true, err
		}
		return true, nil
	default:
		return false, nil
	}
}

func (c *Coinbase) acceptSubscriptions(channels []coinbaseSubscriptionChannel) error {
	expected := make(map[string]struct{}, len(c.Symbols))
	for _, symbol := range c.Symbols {
		expected[symbol] = struct{}{}
	}

	var products []string
	found := false
	for _, channel := range channels {
		if channel.Name == "matches" {
			products = channel.ProductIDs
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("coinbase subscriptions missing matches channel")
	}

	actual := make(map[string]struct{}, len(products))
	for _, product := range products {
		actual[product] = struct{}{}
	}
	for product := range expected {
		if _, ok := actual[product]; !ok {
			return fmt.Errorf("coinbase subscriptions missing product %q", product)
		}
	}
	for product := range actual {
		if _, ok := expected[product]; !ok {
			return fmt.Errorf("coinbase subscriptions include unexpected product %q", product)
		}
	}
	c.subscribed = true
	return nil
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

func parseCoinbaseTradeFields(msg coinbaseMatchMessage) (quanttick.Decimal, quanttick.Decimal, time.Time, int, error) {
	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return quanttick.Decimal{}, quanttick.Decimal{}, time.Time{}, 0, fmt.Errorf("parse coinbase price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Size)
	if err != nil {
		return quanttick.Decimal{}, quanttick.Decimal{}, time.Time{}, 0, fmt.Errorf("parse coinbase size: %w", err)
	}
	timestamp, err := time.Parse(time.RFC3339Nano, msg.Time)
	if err != nil {
		return quanttick.Decimal{}, quanttick.Decimal{}, time.Time{}, 0, fmt.Errorf("parse coinbase time: %w", err)
	}
	timestamp, nanoseconds := splitEventTimestamp(timestamp)
	return price, notional, timestamp, nanoseconds, nil
}

func (c *Coinbase) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	c.subscribed = false
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

	readCtx, cancelRead := context.WithTimeout(ctx, websocketSubscriptionTimeout)
	defer cancelRead()
	awaitingSubscription := true

	for {
		messageType, data, err := conn.Read(readCtx)
		if err != nil {
			if awaitingSubscription && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("await coinbase subscriptions: %w", err)
			}
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
		if awaitingSubscription && c.subscribed {
			cancelRead()
			readCtx = ctx
			awaitingSubscription = false
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

type coinbaseControlMessage struct {
	Type     string                        `json:"type"`
	Message  string                        `json:"message"`
	Reason   string                        `json:"reason"`
	Channels []coinbaseSubscriptionChannel `json:"channels"`
}

type coinbaseSubscriptionChannel struct {
	Name       string   `json:"name"`
	ProductIDs []string `json:"product_ids"`
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
