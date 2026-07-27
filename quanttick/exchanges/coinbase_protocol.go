package exchanges

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

type coinbaseParsedTrade struct {
	event   quanttick.TradeEvent
	tradeID int64
}

func (c *Coinbase) ParseTradeMessage(data []byte, receivedAt time.Time) (quanttick.TradeEvent, bool, error) {
	handled, err := c.parseControlMessage(data)
	if err != nil || handled {
		return quanttick.TradeEvent{}, false, err
	}
	parsed, ok, err := parseCoinbaseTradeMessage(data, receivedAt)
	if err != nil || !ok {
		return quanttick.TradeEvent{}, ok, err
	}
	if c.seen == nil {
		c.seen = newSeenTradeIDs(coinbaseSeenTradeLimit)
	}
	if !c.seen.Add(parsed.event.Symbol, parsed.event.UID) {
		return quanttick.TradeEvent{}, false, nil
	}
	previousID, hadPreviousID := c.lastIDs[parsed.event.Symbol]
	parsed.event.IsSequential = hadPreviousID && parsed.tradeID == previousID+1
	if !hadPreviousID || parsed.tradeID > previousID {
		c.lastIDs[parsed.event.Symbol] = parsed.tradeID
	}
	return parsed.event, true, nil
}

func parseCoinbaseTradeMessage(data []byte, receivedAt time.Time) (coinbaseParsedTrade, bool, error) {
	msg, ok, err := parseCoinbaseMatchMessage(data)
	if err != nil || !ok {
		return coinbaseParsedTrade{}, ok, err
	}
	parsed, err := parseCoinbaseTrade(msg, receivedAt)
	return parsed, err == nil, err
}

func parseCoinbaseTrade(msg coinbaseMatchMessage, receivedAt time.Time) (coinbaseParsedTrade, error) {
	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return coinbaseParsedTrade{}, fmt.Errorf("parse coinbase price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Size)
	if err != nil {
		return coinbaseParsedTrade{}, fmt.Errorf("parse coinbase size: %w", err)
	}
	timestamp, err := time.Parse(time.RFC3339Nano, msg.Time)
	if err != nil {
		return coinbaseParsedTrade{}, fmt.Errorf("parse coinbase time: %w", err)
	}
	timestamp, nanoseconds := splitEventTimestamp(timestamp)
	tickRule := -1
	if strings.ToLower(msg.Side) == "sell" {
		tickRule = 1
	}
	event := quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:    CoinbaseName,
		UID:         strconv.FormatInt(msg.TradeID, 10),
		Symbol:      msg.ProductID,
		Timestamp:   timestamp,
		Nanoseconds: nanoseconds,
		ReceivedAt:  receivedAt,
		Price:       price,
		Notional:    notional,
		TickRule:    tickRule,
	})
	return coinbaseParsedTrade{event: event, tradeID: msg.TradeID}, nil
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
