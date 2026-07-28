package exchanges

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

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
		trade, err := b.parseTrade(rawTrade, receivedAt)
		if err != nil {
			return nil, err
		}
		trades = append(trades, trade)
	}
	return trades, nil
}

func (b *Bybit) parseTrade(rawTrade bybitTrade, receivedAt time.Time) (quanttick.TradeEvent, error) {
	if rawTrade.TradeID == "" {
		return quanttick.TradeEvent{}, fmt.Errorf("bybit trade for %q has no trade id", rawTrade.Symbol)
	}
	price, err := quanttick.ParseDecimal(rawTrade.Price)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse bybit price: %w", err)
	}
	amount, err := quanttick.ParseDecimal(rawTrade.Size)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse bybit size: %w", err)
	}
	tickRule, err := bybitTickRule(rawTrade.Side)
	if err != nil {
		return quanttick.TradeEvent{}, err
	}
	input := quanttick.TradeEventInput{
		Exchange:   b.name,
		UID:        rawTrade.TradeID,
		Symbol:     rawTrade.Symbol,
		Timestamp:  time.UnixMilli(rawTrade.TradeTime).UTC(),
		ReceivedAt: receivedAt,
		Price:      price,
		Notional:   amount,
		TickRule:   tickRule,
	}
	if b.category == "inverse" {
		if !price.IsPositive() {
			return quanttick.TradeEvent{}, fmt.Errorf("invalid bybit inverse price %s", price)
		}
		input.Volume = &amount
		input.Notional = amount.Div(price)
	}
	return quanttick.NewTradeEvent(input), nil
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

type bybitRecentTradesResponse struct {
	ReturnCode    int    `json:"retCode"`
	ReturnMessage string `json:"retMsg"`
	Result        struct {
		List []struct {
			TradeID   string `json:"execId"`
			Symbol    string `json:"symbol"`
			Price     string `json:"price"`
			Size      string `json:"size"`
			Side      string `json:"side"`
			TradeTime string `json:"time"`
		} `json:"list"`
	} `json:"result"`
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
