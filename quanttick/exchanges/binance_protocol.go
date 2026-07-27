package exchanges

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func (b *Binance) ParseTradeMessage(data []byte, receivedAt time.Time) (quanttick.TradeEvent, bool, error) {
	parsed, ok, err := b.parseTradeMessage(data, receivedAt, b.lastIDs)
	return parsed.event, ok, err
}

func (b *Binance) parseTradeMessage(
	data []byte,
	receivedAt time.Time,
	lastIDs map[string]int64,
) (binanceParsedTrade, bool, error) {
	payload, combinedStream, err := b.unwrapTradeMessage(data)
	if err != nil {
		return binanceParsedTrade{}, false, err
	}
	eventType, ok, err := binanceEventType(payload)
	if err != nil {
		return binanceParsedTrade{}, false, err
	}
	if !ok || eventType != b.stream {
		return binanceParsedTrade{}, false, nil
	}

	var msg binanceTradeMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance message: %w", err)
	}
	if combinedStream != "" {
		expectedStream := strings.ToLower(msg.Symbol) + "@" + b.stream
		if combinedStream != expectedStream {
			return binanceParsedTrade{}, false, fmt.Errorf(
				"binance combined stream %q does not match trade %q",
				combinedStream,
				expectedStream,
			)
		}
	}

	buyerIsMaker, err := binanceBuyerIsMaker(payload)
	if err != nil {
		return binanceParsedTrade{}, false, err
	}
	return b.buildParsedTrade(msg, buyerIsMaker, receivedAt, lastIDs)
}

func (b *Binance) buildParsedTrade(
	msg binanceTradeMessage,
	buyerIsMaker bool,
	receivedAt time.Time,
	lastIDs map[string]int64,
) (binanceParsedTrade, bool, error) {
	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Quantity)
	if err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance quantity: %w", err)
	}

	tradeID := msg.TradeID
	ticks := 1
	if b.stream == "aggTrade" {
		tradeID = msg.AggregateTradeID
		ticks = int(msg.LastTradeID - msg.FirstTradeID + 1)
	}
	prevID, hadPrevID := lastIDs[msg.Symbol]
	if !hadPrevID || tradeID > prevID {
		lastIDs[msg.Symbol] = tradeID
	}
	tickRule := 1
	if buyerIsMaker {
		tickRule = -1
	}

	event := quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     b.name,
		UID:          strconv.FormatInt(tradeID, 10),
		Symbol:       msg.Symbol,
		Timestamp:    time.UnixMilli(msg.TradeTime).UTC(),
		ReceivedAt:   receivedAt,
		Price:        price,
		Notional:     notional,
		TickRule:     tickRule,
		Ticks:        ticks,
		IsSequential: hadPrevID && tradeID == prevID+1,
	})
	return binanceParsedTrade{event: event, tradeID: tradeID}, true, nil
}

func (b *Binance) unwrapTradeMessage(data []byte) ([]byte, string, error) {
	if b.name != BinanceFuturesName {
		return data, "", nil
	}

	var envelope binanceCombinedStreamMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, "", fmt.Errorf("parse binance message: %w", err)
	}
	if envelope.Stream == "" {
		return data, "", nil
	}
	if len(envelope.Data) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Data), []byte("null")) {
		return nil, "", fmt.Errorf("binance combined stream %q has no data", envelope.Stream)
	}
	return envelope.Data, envelope.Stream, nil
}

func parseBinanceSubscriptionResponse(data []byte) (bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("parse binance message: %w", err)
	}

	if rawEvent, ok := envelope["e"]; ok && bytes.Equal(bytes.TrimSpace(rawEvent), []byte(`"serverShutdown"`)) {
		return false, errBinanceServerShutdown
	}

	if rawCode, ok := envelope["code"]; ok {
		var code int
		if err := json.Unmarshal(rawCode, &code); err != nil {
			return false, fmt.Errorf("parse binance websocket error code: %w", err)
		}
		var message string
		if rawMessage, ok := envelope["msg"]; ok {
			_ = json.Unmarshal(rawMessage, &message)
		}
		return false, fmt.Errorf("binance websocket error %d: %s", code, message)
	}

	rawResult, hasResult := envelope["result"]
	rawID, hasID := envelope["id"]
	if !hasResult && !hasID {
		return false, nil
	}
	if !hasResult || !hasID {
		return false, fmt.Errorf("invalid binance subscription response: %s", string(data))
	}

	var id int64
	if err := json.Unmarshal(rawID, &id); err != nil {
		return false, fmt.Errorf("parse binance subscription response id: %w", err)
	}
	if id != binanceSubscriptionRequestID {
		return false, fmt.Errorf("binance subscription response has unexpected id %d", id)
	}
	if string(bytes.TrimSpace(rawResult)) != "null" {
		return false, fmt.Errorf("binance subscription request failed: %s", string(data))
	}
	return true, nil
}

type binanceCombinedStreamMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type binanceTradeMessage struct {
	Symbol           string `json:"s"`
	TradeID          int64  `json:"t"`
	AggregateTradeID int64  `json:"a"`
	FirstTradeID     int64  `json:"f"`
	LastTradeID      int64  `json:"l"`
	TradeTime        int64  `json:"T"`
	Price            string `json:"p"`
	Quantity         string `json:"q"`
}

func binanceBuyerIsMaker(data []byte) (bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("parse binance buyer maker envelope: %w", err)
	}
	raw, ok := envelope["m"]
	if !ok {
		return false, fmt.Errorf("parse binance buyer maker: missing m")
	}

	// encoding/json matches field tags case-insensitively, so read exact "m"
	// to avoid Binance's "M" flag overwriting buyer-is-maker.
	var buyerIsMaker bool
	if err := json.Unmarshal(raw, &buyerIsMaker); err != nil {
		return false, fmt.Errorf("parse binance buyer maker: %w", err)
	}
	return buyerIsMaker, nil
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
