package exchanges

import (
	"reflect"
	"testing"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestBitmexSubscriptionMessages(t *testing.T) {
	exchange := NewBitmex([]string{"XBTUSD", "ETHUSD"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"op":   "subscribe",
			"args": []string{"trade:XBTUSD", "trade:ETHUSD"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
}

func TestBitmexSpotSymbolSubscriptionMessages(t *testing.T) {
	exchange := NewBitmex([]string{"XBT_USDT"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"op":   "subscribe",
			"args": []string{"trade:XBT_USDT"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
	if exchange.Name() != BitmexName {
		t.Fatalf("name = %s, want %s", exchange.Name(), BitmexName)
	}
}

func TestBitmexParseTradeMessages(t *testing.T) {
	exchange := NewBitmex([]string{"XBTUSD"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"table": "trade",
			"action": "insert",
			"data": [
				{
					"trdMatchID": "a",
					"symbol": "XBTUSD",
					"timestamp": "2026-04-08T00:00:00.123456789Z",
					"side": "Buy",
					"price": 100.0,
					"homeNotional": "1.499999",
					"foreignNotional": "150.0"
				},
				{
					"trdMatchID": "b",
					"symbol": "XBTUSD",
					"timestamp": "2026-04-08T00:00:01.000Z",
					"side": "Sell",
					"price": 200.0,
					"foreignNotional": "400.0"
				}
			]
		}`),
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}

	assertStrings(t, tradeUIDs(trades), []string{"a", "b"})
	assertStrings(t, tradeExchanges(trades), []string{BitmexName, BitmexName})
	assertStrings(t, tradeSymbols(trades), []string{"XBTUSD", "XBTUSD"})
	assertInts(t, tradeTickRules(trades), []int{1, -1})
	assertInts(t, tradeNanoseconds(trades), []int{789, 0})
	assertDecimals(t, tradePrices(trades), []string{"100.0", "200.0"})
	assertDecimals(t, tradeNotionals(trades), []string{"1.5", "2"})
	assertDecimals(t, tradeVolumes(trades), []string{"150.0", "400.0"})

	wantTimes := []time.Time{
		time.Date(2026, 4, 8, 0, 0, 0, 123456000, time.UTC),
		time.Date(2026, 4, 8, 0, 0, 1, 0, time.UTC),
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

func TestBitmexParsesPartialTradeSnapshotChronologically(t *testing.T) {
	exchange := NewBitmex([]string{"XBTUSD"})
	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"table": "trade",
			"action": "partial",
			"data": [
				{
					"trdMatchID": "later",
					"symbol": "XBTUSD",
					"timestamp": "2026-04-08T00:00:02.000Z",
					"side": "Buy",
					"price": 100.0,
					"homeNotional": "1"
				},
				{
					"trdMatchID": "earlier",
					"symbol": "XBTUSD",
					"timestamp": "2026-04-08T00:00:01.000Z",
					"side": "Sell",
					"price": 100.0,
					"homeNotional": "1"
				}
			]
		}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertStrings(t, tradeUIDs(trades), []string{"earlier", "later"})
}

func TestBitmexSeenTradesDeduplicatesWithinLimit(t *testing.T) {
	seen := newBitmexSeenTrades(2)
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	first := bitmexTestTrade("first", timestamp)
	second := bitmexTestTrade("second", timestamp.Add(time.Second))
	third := bitmexTestTrade("third", timestamp.Add(2*time.Second))

	if !seen.Add(first) {
		t.Fatal("first trade should be new")
	}
	if seen.Add(first) {
		t.Fatal("duplicate first trade should be rejected")
	}
	if !seen.Add(second) || !seen.Add(third) {
		t.Fatal("second and third trades should be new")
	}
	if !seen.Add(first) {
		t.Fatal("first trade should be accepted after it leaves the bounded cache")
	}
}

func TestBitmexSpotSymbolParseTradeMessages(t *testing.T) {
	exchange := NewBitmex([]string{"XBT_USDT"})
	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"table": "trade",
			"action": "insert",
			"data": [
				{
					"trdMatchID": "a",
					"symbol": "XBT_USDT",
					"timestamp": "2026-04-08T00:00:00.000Z",
					"side": "Buy",
					"price": "100.0",
					"homeNotional": "1.5"
				}
			]
		}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertStrings(t, tradeExchanges(trades), []string{BitmexName})
	assertStrings(t, tradeSymbols(trades), []string{"XBT_USDT"})
}

func TestBitmexParseIgnoresNonTradeMessages(t *testing.T) {
	exchange := NewBitmex([]string{"XBTUSD"})

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{"table":"instrument","action":"partial","data":[]}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 0 {
		t.Fatalf("expected non-trade message to be ignored, got %#v", trades)
	}
}

func TestBitmexParsesStringNumericFields(t *testing.T) {
	exchange := NewBitmex([]string{"XBTUSD"})

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"table": "trade",
			"action": "insert",
			"data": [
				{
					"trdMatchID": "a",
					"symbol": "XBTUSD",
					"timestamp": "2026-04-08T00:00:00.000Z",
					"side": "Buy",
					"price": "100.0",
					"homeNotional": 1.499999,
					"foreignNotional": 150.0
				}
			]
		}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDecimals(t, tradePrices(trades), []string{"100.0"})
	assertDecimals(t, tradeNotionals(trades), []string{"1.5"})
	assertDecimals(t, tradeVolumes(trades), []string{"150.0"})
}

func TestBitmexFallsBackToHomeNotionalWhenForeignNotionalIsMissing(t *testing.T) {
	exchange := NewBitmex([]string{"XBTUSD"})

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"table": "trade",
			"action": "insert",
			"data": [
				{
					"trdMatchID": "a",
					"symbol": "XBTUSD",
					"timestamp": "2026-04-08T00:00:00.000Z",
					"side": "Buy",
					"price": "100.0",
					"homeNotional": 1.5
				}
			]
		}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDecimals(t, tradeNotionals(trades), []string{"1.5"})
	assertDecimals(t, tradeVolumes(trades), []string{"150.00"})
}

func bitmexTestTrade(uid string, timestamp time.Time) quanttick.TradeEvent {
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     BitmexName,
		UID:          uid,
		Symbol:       "XBTUSD",
		Timestamp:    timestamp,
		ReceivedAt:   timestamp,
		Price:        quanttick.MustDecimal("100"),
		Notional:     quanttick.MustDecimal("1"),
		TickRule:     1,
		IsSequential: true,
	})
}
