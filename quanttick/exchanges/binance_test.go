package exchanges

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestBinanceSubscriptionMessages(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT", "ETHUSDT"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"method": "SUBSCRIBE",
			"params": []string{"btcusdt@trade", "ethusdt@trade"},
			"id":     1,
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
}

func TestBinanceParseTradeMessages(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"e":"trade","s":"BTCUSDT","t":100,"T":1775606400000,"p":"100","q":"1","m":false}`,
		`{"e":"trade","s":"BTCUSDT","t":101,"T":1775606401000,"p":"101","q":"2","m":true}`,
		`{"e":"trade","s":"BTCUSDT","t":103,"T":1775606402000,"p":"102","q":"3","m":false}`,
	}

	trades := make([]quanttick.TradeEvent, 0, len(messages))
	for _, message := range messages {
		trade, ok, err := exchange.ParseTradeMessage([]byte(message), receivedAt)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected trade message to parse: %s", message)
		}
		trades = append(trades, trade)
	}

	assertStrings(t, tradeUIDs(trades), []string{"100", "101", "103"})
	assertInts(t, tradeTicks(trades), []int{1, 1, 1})
	assertBools(t, tradeSequential(trades), []bool{true, true, false})
	assertStrings(t, tradeExchanges(trades), []string{BinanceName, BinanceName, BinanceName})
	assertStrings(t, tradeSymbols(trades), []string{"BTCUSDT", "BTCUSDT", "BTCUSDT"})
	assertInts(t, tradeTickRules(trades), []int{1, -1, 1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})

	wantTimes := []time.Time{
		time.Unix(1775606400, 0).UTC(),
		time.Unix(1775606401, 0).UTC(),
		time.Unix(1775606402, 0).UTC(),
	}
	for i, want := range wantTimes {
		if !trades[i].Timestamp.Equal(want) {
			t.Fatalf("timestamp[%d] = %s, want %s", i, trades[i].Timestamp, want)
		}
		if !trades[i].ReceivedAt.Equal(receivedAt) {
			t.Fatalf("receivedAt[%d] = %s, want %s", i, trades[i].ReceivedAt, receivedAt)
		}
	}
}

func TestBinanceParseLiveTradeShape(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})
	data := []byte(`{"e":"trade","E":1777807004059,"s":"BTCUSDT","t":6268487624,"p":"78530.40000000","q":"0.00039000","T":1777807004059,"m":true,"M":true}`)

	trade, ok, err := exchange.ParseTradeMessage(
		data,
		time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected live trade shape to parse")
	}
	if trade.UID != "6268487624" {
		t.Fatalf("uid = %s, want 6268487624", trade.UID)
	}
}

func TestBinanceParseIgnoresNonTradeMessages(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})

	messages := []string{
		`{"result":null,"id":1}`,
		`{"e":1,"E":1775606400000}`,
		`{"e":"aggTrade","s":"BTCUSDT"}`,
	}
	for _, message := range messages {
		trade, ok, err := exchange.ParseTradeMessage([]byte(message), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("expected non-trade message to be ignored, got %#v", trade)
		}
	}
}

func TestBinanceTradeEventJSONShape(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})
	trade, ok, err := exchange.ParseTradeMessage(
		[]byte(`{"e":"trade","s":"BTCUSDT","t":100,"T":1775606400000,"p":"100","q":"1","m":false}`),
		time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected trade message")
	}

	data, err := json.Marshal(trade)
	if err != nil {
		t.Fatal(err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}

	if _, ok := payload["receivedAt"]; ok {
		t.Fatalf("receivedAt should not be exported in payload: %s", string(data))
	}
	for _, key := range []string{"price", "volume", "notional"} {
		if _, ok := payload[key].(string); !ok {
			t.Fatalf("%s should be encoded as JSON string in %s", key, string(data))
		}
	}
}

func tradeUIDs(trades []quanttick.TradeEvent) []string {
	values := make([]string, len(trades))
	for i, trade := range trades {
		values[i] = trade.UID
	}
	return values
}

func tradeExchanges(trades []quanttick.TradeEvent) []string {
	values := make([]string, len(trades))
	for i, trade := range trades {
		values[i] = trade.Exchange
	}
	return values
}

func tradeSymbols(trades []quanttick.TradeEvent) []string {
	values := make([]string, len(trades))
	for i, trade := range trades {
		values[i] = trade.Symbol
	}
	return values
}

func tradeTicks(trades []quanttick.TradeEvent) []int {
	values := make([]int, len(trades))
	for i, trade := range trades {
		values[i] = trade.Ticks
	}
	return values
}

func tradeTickRules(trades []quanttick.TradeEvent) []int {
	values := make([]int, len(trades))
	for i, trade := range trades {
		values[i] = trade.TickRule
	}
	return values
}

func tradeSequential(trades []quanttick.TradeEvent) []bool {
	values := make([]bool, len(trades))
	for i, trade := range trades {
		values[i] = trade.IsSequential
	}
	return values
}

func tradePrices(trades []quanttick.TradeEvent) []quanttick.Decimal {
	values := make([]quanttick.Decimal, len(trades))
	for i, trade := range trades {
		values[i] = trade.Price
	}
	return values
}

func tradeNotionals(trades []quanttick.TradeEvent) []quanttick.Decimal {
	values := make([]quanttick.Decimal, len(trades))
	for i, trade := range trades {
		values[i] = trade.Notional
	}
	return values
}

func tradeVolumes(trades []quanttick.TradeEvent) []quanttick.Decimal {
	values := make([]quanttick.Decimal, len(trades))
	for i, trade := range trades {
		values[i] = trade.Volume
	}
	return values
}

func assertStrings(t *testing.T, got []string, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("strings = %#v, want %#v", got, want)
	}
}

func assertInts(t *testing.T, got []int, want []int) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ints = %#v, want %#v", got, want)
	}
}

func assertBools(t *testing.T, got []bool, want []bool) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bools = %#v, want %#v", got, want)
	}
}

func assertDecimals(t *testing.T, got []quanttick.Decimal, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decimal length = %d, want %d", len(got), len(want))
	}
	for i := range got {
		expected := quanttick.MustDecimal(want[i])
		if !got[i].Equal(expected) {
			t.Fatalf("decimal[%d] = %s, want %s", i, got[i], expected)
		}
	}
}
