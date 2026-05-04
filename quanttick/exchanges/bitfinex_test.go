package exchanges

import (
	"reflect"
	"testing"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestBitfinexSubscriptionMessages(t *testing.T) {
	exchange := NewBitfinex([]string{"BTCUSD", "tETHUSD"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  "tBTCUSD",
		},
		{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  "tETHUSD",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
}

func TestBitfinexParseTradeMessages(t *testing.T) {
	exchange := NewBitfinex([]string{"tBTCUSD"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"event":"subscribed","chanId":1,"symbol":"tBTCUSD"}`,
		`[1,"hb"]`,
		`[1,"tu",[999,1775557139000,1,99]]`,
		`[1,"te",[100,1775557140000,1,100]]`,
		`[1,"te",[102,1775557141000,-2,101]]`,
		`[1,"te",[101,1775557142000,3,102]]`,
	}

	trades := parseBitfinexFixture(t, exchange, messages, receivedAt)

	assertStrings(t, tradeUIDs(trades), []string{"100", "102", "101"})
	assertBools(t, tradeSequential(trades), []bool{true, true, false})
	assertStrings(t, tradeExchanges(trades), []string{BitfinexName, BitfinexName, BitfinexName})
	assertStrings(t, tradeSymbols(trades), []string{"tBTCUSD", "tBTCUSD", "tBTCUSD"})
	assertInts(t, tradeTickRules(trades), []int{1, -1, 1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})

	wantTimes := []time.Time{
		time.Unix(1775557140, 0).UTC(),
		time.Unix(1775557141, 0).UTC(),
		time.Unix(1775557142, 0).UTC(),
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

func TestBitfinexParseIgnoresUnknownChannelAndSnapshots(t *testing.T) {
	exchange := NewBitfinex([]string{"tBTCUSD"})

	messages := []string{
		`[1,"te",[100,1775557140000,1,100]]`,
		`{"event":"subscribed","chanId":1,"symbol":"tBTCUSD"}`,
		`[1,[[100,1775557140000,1,100]]]`,
		`{"event":"error","msg":"bad symbol"}`,
	}

	trades := parseBitfinexFixture(t, exchange, messages, time.Now().UTC())
	if len(trades) != 0 {
		t.Fatalf("expected messages to be ignored, got %#v", trades)
	}
}

func TestBitfinexParsesStringNumericFields(t *testing.T) {
	exchange := NewBitfinex([]string{"BTCUSD"})
	receivedAt := time.Now().UTC()
	messages := []string{
		`{"event":"subscribed","chanId":"1","symbol":"tBTCUSD"}`,
		`["1","te",["100","1775557140000","-1.5","100.5"]]`,
	}

	trades := parseBitfinexFixture(t, exchange, messages, receivedAt)
	assertStrings(t, tradeUIDs(trades), []string{"100"})
	assertInts(t, tradeTickRules(trades), []int{-1})
	assertDecimals(t, tradePrices(trades), []string{"100.5"})
	assertDecimals(t, tradeNotionals(trades), []string{"1.5"})
	assertDecimals(t, tradeVolumes(trades), []string{"150.75"})
}

func parseBitfinexFixture(t *testing.T, exchange *Bitfinex, messages []string, receivedAt time.Time) []quanttick.TradeEvent {
	t.Helper()

	var trades []quanttick.TradeEvent
	for _, message := range messages {
		parsedTrades, err := exchange.ParseTradeMessage([]byte(message), receivedAt)
		if err != nil {
			t.Fatal(err)
		}
		trades = append(trades, parsedTrades...)
	}
	return trades
}
