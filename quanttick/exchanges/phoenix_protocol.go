package exchanges

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

type phoenixFill struct {
	Symbol      string `json:"marketSymbol"`
	BaseQty     string `json:"baseQty"`
	QuoteQty    string `json:"quoteQty"`
	Price       string `json:"price"`
	Timestamp   string `json:"timestamp"`
	Signature   string `json:"transactionSignature"`
	Instruction string `json:"instructionType"`
}

type phoenixEnvelope struct {
	Type         string `json:"type"`
	Channel      string `json:"channel"`
	Status       string `json:"status"`
	Symbol       string `json:"symbol"`
	Error        string `json:"error"`
	Message      string `json:"message"`
	Subscription struct {
		Channel      string `json:"channel"`
		MarketSymbol string `json:"marketSymbol"`
		Symbol       string `json:"symbol"`
	} `json:"subscription"`
	Fills []phoenixFill `json:"fills"`
}

// Phoenix publishes individual fills without a fill ID. An occurrence number
// preserves identical fills within one transaction, including across frames or
// REST pages. Each WebSocket session and complete REST window counts separately;
// matching IDs then remove their overlap. Numeric fields are canonicalized so
// REST/WS formatting differences do not create new trades.
type phoenixFillCounter struct {
	counts map[string]int
	order  []string
}

func (c *phoenixFillCounter) identify(trade quanttick.TradeEvent, fill phoenixFill) string {
	key := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s|%d|%s",
		trade.Symbol, fill.Signature, trade.Timestamp.Format(time.RFC3339Nano),
		trade.Nanoseconds, trade.Price, trade.Volume, trade.Notional, trade.TickRule, fill.Instruction)
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
	if c.counts == nil {
		c.counts = make(map[string]int)
	}
	if c.counts[digest] == 0 {
		c.order = append(c.order, digest)
	}
	c.counts[digest]++
	uid := digest + ":" + strconv.Itoa(c.counts[digest])
	if len(c.order) > phoenixTradeLimit {
		delete(c.counts, c.order[0])
		c.order = c.order[1:]
	}
	return uid
}

func parsePhoenixFill(fill phoenixFill, receivedAt time.Time) (quanttick.TradeEvent, error) {
	price, err := quanttick.ParseDecimal(fill.Price)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse phoenix price: %w", err)
	}
	base, err := quanttick.ParseDecimal(fill.BaseQty)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse phoenix base quantity: %w", err)
	}
	quote, err := quanttick.ParseDecimal(fill.QuoteQty)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse phoenix quote quantity: %w", err)
	}
	if !price.IsPositive() || base.Sign()*quote.Sign() != -1 {
		return quanttick.TradeEvent{}, fmt.Errorf("phoenix fill has invalid price or signed quantities")
	}
	if fill.Signature == "" || fill.Symbol == "" {
		return quanttick.TradeEvent{}, fmt.Errorf("phoenix fill lacks a transaction signature or symbol")
	}
	timestamp, err := time.Parse(time.RFC3339Nano, fill.Timestamp)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse phoenix timestamp: %w", err)
	}
	timestamp, nanoseconds := splitEventTimestamp(timestamp.UTC())
	volume := quote.Abs()
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:    PhoenixName,
		Symbol:      fill.Symbol,
		Timestamp:   timestamp,
		Nanoseconds: nanoseconds,
		ReceivedAt:  receivedAt,
		Price:       price,
		Volume:      &volume,
		Notional:    base.Abs(),
		TickRule:    base.Sign(),
	}), nil
}

func (p *Phoenix) parseMessage(data []byte, receivedAt time.Time, counter *phoenixFillCounter) (string, []quanttick.TradeEvent, error) {
	var msg phoenixEnvelope
	if err := json.Unmarshal(data, &msg); err != nil {
		return "", nil, fmt.Errorf("parse phoenix message: %w", err)
	}
	if msg.Channel == "error" || msg.Type == "subscriptionError" {
		return "", nil, fmt.Errorf("phoenix websocket error: %s %s", msg.Error, msg.Message)
	}
	if msg.Channel == "subscriptionStatus" || msg.Type == "subscriptionConfirmed" {
		if msg.Channel == "subscriptionStatus" && msg.Status != "subscribed" {
			return "", nil, fmt.Errorf("phoenix subscription status %q", msg.Status)
		}
		symbol := msg.Subscription.MarketSymbol
		if symbol == "" {
			symbol = msg.Subscription.Symbol
		}
		if msg.Subscription.Channel != "fills" || !slices.Contains(p.Symbols, symbol) {
			return "", nil, fmt.Errorf("phoenix acknowledged unexpected subscription: %s", data)
		}
		return symbol, nil, nil
	}
	if msg.Channel != "fills" {
		return "", nil, nil
	}
	if !slices.Contains(p.Symbols, msg.Symbol) || msg.Fills == nil {
		return "", nil, fmt.Errorf("phoenix fill message has unexpected symbol or missing fills")
	}
	trades := make([]quanttick.TradeEvent, 0, len(msg.Fills))
	for _, fill := range msg.Fills {
		if fill.Symbol != msg.Symbol {
			return "", nil, fmt.Errorf("phoenix fill symbol %q does not match channel %q", fill.Symbol, msg.Symbol)
		}
		trade, err := parsePhoenixFill(fill, receivedAt)
		if err != nil {
			return "", nil, err
		}
		trade.UID = counter.identify(trade, fill)
		trades = append(trades, trade)
	}
	return "", trades, nil
}
