package exchanges

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

type deribitParsedTrade struct {
	event    quanttick.TradeEvent
	sequence int64
}

func deribitTradeChannel(symbol string) string {
	return "trades." + symbol + ".100ms"
}

func (d *Deribit) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	parsed, err := d.parseTradeMessage(data, receivedAt)
	if err != nil {
		return nil, err
	}
	trades := make([]quanttick.TradeEvent, 0, len(parsed))
	for _, trade := range parsed {
		if d.seen == nil {
			d.seen = newSeenTradeIDs(deribitSeenTradeLimit)
		}
		if !d.seen.Add(trade.event.Symbol, trade.event.UID) {
			continue
		}
		previous, hadPrevious := d.lastSequences[trade.event.Symbol]
		trade.event.IsSequential = hadPrevious && trade.sequence == previous+1
		if !hadPrevious || trade.sequence > previous {
			d.lastSequences[trade.event.Symbol] = trade.sequence
		}
		trades = append(trades, trade.event)
	}
	return trades, nil
}

func (d *Deribit) parseTradeMessage(data []byte, receivedAt time.Time) ([]deribitParsedTrade, error) {
	var envelope deribitEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse deribit message: %w", err)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("deribit websocket error %d: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if envelope.Method != "subscription" {
		return nil, nil
	}
	const prefix = "trades."
	const suffix = ".100ms"
	if !strings.HasPrefix(envelope.Params.Channel, prefix) || !strings.HasSuffix(envelope.Params.Channel, suffix) {
		return nil, nil
	}
	symbol := strings.TrimSuffix(strings.TrimPrefix(envelope.Params.Channel, prefix), suffix)
	if !d.hasSymbol(symbol) {
		return nil, fmt.Errorf("deribit trade message has unexpected channel %q", envelope.Params.Channel)
	}
	var rawTrades []deribitTrade
	if err := json.Unmarshal(envelope.Params.Data, &rawTrades); err != nil {
		return nil, fmt.Errorf("parse deribit trades: %w", err)
	}
	parsed := make([]deribitParsedTrade, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		if rawTrade.InstrumentName != symbol {
			return nil, fmt.Errorf(
				"deribit trade instrument %q does not match channel %q",
				rawTrade.InstrumentName,
				envelope.Params.Channel,
			)
		}
		trade, err := parseDeribitTrade(rawTrade, receivedAt)
		if err != nil {
			return nil, err
		}
		parsed = append(parsed, trade)
	}
	sort.SliceStable(parsed, func(left, right int) bool {
		return parsed[left].sequence < parsed[right].sequence
	})
	return parsed, nil
}

func parseDeribitTrade(rawTrade deribitTrade, receivedAt time.Time) (deribitParsedTrade, error) {
	if rawTrade.TradeSequence <= 0 {
		return deribitParsedTrade{}, fmt.Errorf("deribit trade for %q has invalid sequence %d", rawTrade.InstrumentName, rawTrade.TradeSequence)
	}
	price, err := parseRawDecimal(rawTrade.Price)
	if err != nil {
		return deribitParsedTrade{}, fmt.Errorf("parse deribit price: %w", err)
	}
	amount, err := parseRawDecimal(rawTrade.Amount)
	if err != nil {
		return deribitParsedTrade{}, fmt.Errorf("parse deribit amount: %w", err)
	}
	tickRule, err := deribitTickRule(rawTrade.Direction)
	if err != nil {
		return deribitParsedTrade{}, err
	}
	timestamp := time.UnixMilli(rawTrade.Timestamp).UTC()
	if rawTrade.StarbaseTimestamp > 0 {
		timestamp = time.Unix(0, rawTrade.StarbaseTimestamp).UTC()
	}
	timestamp, nanoseconds := splitEventTimestamp(timestamp)
	input := quanttick.TradeEventInput{
		Exchange:    DeribitName,
		UID:         strconv.FormatInt(rawTrade.TradeSequence, 10),
		Symbol:      rawTrade.InstrumentName,
		Timestamp:   timestamp,
		Nanoseconds: nanoseconds,
		ReceivedAt:  receivedAt,
		Price:       price,
		Notional:    amount,
		TickRule:    tickRule,
	}
	if !deribitAmountIsBase(rawTrade.InstrumentName) {
		volume := amount
		input.Volume = &volume
		input.Notional = amount.Div(price)
	}
	return deribitParsedTrade{
		event:    quanttick.NewTradeEvent(input),
		sequence: rawTrade.TradeSequence,
	}, nil
}

func (d *Deribit) parseSubscriptionResponse(data []byte) (bool, error) {
	var envelope deribitEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("parse deribit message: %w", err)
	}
	if envelope.Error != nil {
		return false, fmt.Errorf("deribit websocket error %d: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if envelope.ID == 0 && len(envelope.Result) == 0 {
		return false, nil
	}
	if envelope.ID != deribitSubscriptionRequestID {
		return false, fmt.Errorf("deribit subscription response has unexpected id %d", envelope.ID)
	}
	var channels []string
	if err := json.Unmarshal(envelope.Result, &channels); err != nil {
		return false, fmt.Errorf("parse deribit subscription response: %w", err)
	}
	expected := make(map[string]struct{}, len(d.Symbols))
	for _, symbol := range d.Symbols {
		expected[deribitTradeChannel(symbol)] = struct{}{}
	}
	actual := make(map[string]struct{}, len(channels))
	for _, channel := range channels {
		actual[channel] = struct{}{}
	}
	for channel := range expected {
		if _, ok := actual[channel]; !ok {
			return false, fmt.Errorf("deribit subscription response missing channel %q", channel)
		}
	}
	for channel := range actual {
		if _, ok := expected[channel]; !ok {
			return false, fmt.Errorf("deribit subscription response includes unexpected channel %q", channel)
		}
	}
	return true, nil
}

func (d *Deribit) hasSymbol(symbol string) bool {
	for _, candidate := range d.Symbols {
		if candidate == symbol {
			return true
		}
	}
	return false
}

func deribitAmountIsBase(symbol string) bool {
	if strings.Contains(symbol, "_") {
		return true
	}
	parts := strings.Split(symbol, "-")
	if len(parts) != 4 {
		return false
	}
	return parts[3] == "C" || parts[3] == "P"
}

func deribitTickRule(direction string) (int, error) {
	switch strings.ToLower(direction) {
	case "buy":
		return 1, nil
	case "sell":
		return -1, nil
	default:
		return 0, fmt.Errorf("invalid deribit direction %q", direction)
	}
}

type deribitEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Method  string          `json:"method"`
	Result  json.RawMessage `json:"result"`
	Params  struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	} `json:"params"`
	Error *deribitError `json:"error"`
}

type deribitError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type deribitTrade struct {
	TradeSequence     int64           `json:"trade_seq"`
	Timestamp         int64           `json:"timestamp"`
	StarbaseTimestamp int64           `json:"starbase_timestamp"`
	Price             json.RawMessage `json:"price"`
	Amount            json.RawMessage `json:"amount"`
	Direction         string          `json:"direction"`
	InstrumentName    string          `json:"instrument_name"`
}
