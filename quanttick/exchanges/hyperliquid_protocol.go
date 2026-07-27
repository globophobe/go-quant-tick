package exchanges

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func (h *Hyperliquid) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel != "trades" {
		return nil, nil
	}

	var rawTrades []hyperliquidTrade
	if err := json.Unmarshal(envelope.Data, &rawTrades); err != nil {
		return nil, fmt.Errorf("parse hyperliquid trades: %w", err)
	}

	trades := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		trade, err := parseHyperliquidTrade(rawTrade, receivedAt)
		if err != nil {
			return nil, err
		}
		trades = append(trades, trade)
	}

	return trades, nil
}

func parseHyperliquidTrade(
	rawTrade hyperliquidTrade,
	receivedAt time.Time,
) (quanttick.TradeEvent, error) {
	price, err := quanttick.ParseDecimal(rawTrade.Price)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse hyperliquid price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(rawTrade.Size)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse hyperliquid size: %w", err)
	}
	tickRule, err := hyperliquidTickRule(rawTrade.Side)
	if err != nil {
		return quanttick.TradeEvent{}, err
	}
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:   HyperliquidName,
		UID:        strconv.FormatInt(rawTrade.Time, 10) + ":" + rawTrade.Coin + ":" + strconv.FormatInt(rawTrade.TradeID, 10),
		Symbol:     rawTrade.Coin,
		Timestamp:  time.UnixMilli(rawTrade.Time).UTC(),
		ReceivedAt: receivedAt,
		Price:      price,
		Notional:   notional,
		TickRule:   tickRule,
	}), nil
}

func parseHyperliquidSubscriptionAck(data []byte) (string, bool, error) {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", false, fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel == "error" {
		return "", false, hyperliquidProtocolError(envelope.Data)
	}
	if envelope.Channel != "subscriptionResponse" {
		return "", false, nil
	}

	var response hyperliquidSubscriptionResponse
	if err := json.Unmarshal(envelope.Data, &response); err != nil {
		return "", false, fmt.Errorf("parse hyperliquid subscription acknowledgement: %w", err)
	}
	if response.Method != "subscribe" || response.Subscription.Type != "trades" || response.Subscription.Coin == "" {
		return "", false, fmt.Errorf("invalid hyperliquid subscription acknowledgement: %s", string(data))
	}
	return response.Subscription.Coin, true, nil
}

func parseHyperliquidProtocolError(data []byte) error {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel != "error" {
		return nil
	}
	return hyperliquidProtocolError(envelope.Data)
}

func hyperliquidProtocolError(data json.RawMessage) error {
	var message string
	if err := json.Unmarshal(data, &message); err == nil {
		return fmt.Errorf("hyperliquid websocket error: %s", message)
	}
	return fmt.Errorf("hyperliquid websocket error: %s", string(data))
}

func hyperliquidTickRule(side string) (int, error) {
	switch strings.ToLower(side) {
	case "b", "buy":
		return 1, nil
	case "a", "sell":
		return -1, nil
	default:
		return 0, fmt.Errorf("invalid hyperliquid side %q", side)
	}
}

type hyperliquidEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type hyperliquidTrade struct {
	Coin    string `json:"coin"`
	Side    string `json:"side"`
	Price   string `json:"px"`
	Size    string `json:"sz"`
	Time    int64  `json:"time"`
	TradeID int64  `json:"tid"`
}

type hyperliquidSubscriptionResponse struct {
	Method       string `json:"method"`
	Subscription struct {
		Type string `json:"type"`
		Coin string `json:"coin"`
	} `json:"subscription"`
}
