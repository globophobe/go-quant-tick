package exchanges

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

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

func parseMarketTrade(exchange, symbol, uid, millis, priceText, sizeText, side string, receivedAt time.Time) (quanttick.TradeEvent, error) {
	id, idErr := strconv.ParseInt(uid, 10, 64)
	timestamp, timeErr := strconv.ParseInt(millis, 10, 64)
	price, priceErr := quanttick.ParseDecimal(priceText)
	size, sizeErr := quanttick.ParseDecimal(sizeText)
	if idErr != nil || id <= 0 || timeErr != nil || timestamp <= 0 || priceErr != nil || !price.IsPositive() || sizeErr != nil || !size.IsPositive() {
		return quanttick.TradeEvent{}, fmt.Errorf("invalid %s trade %q", exchange, uid)
	}
	tickRule := 1
	switch side {
	case "buy":
	case "sell":
		tickRule = -1
	default:
		return quanttick.TradeEvent{}, fmt.Errorf("invalid %s trade side %q", exchange, side)
	}
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange: exchange, Symbol: symbol, UID: uid, Timestamp: time.UnixMilli(timestamp).UTC(),
		ReceivedAt: receivedAt, Price: price, Notional: size, TickRule: tickRule,
	}), nil
}
