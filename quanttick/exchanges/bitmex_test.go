package exchanges

import (
	"reflect"
	"testing"
	"time"
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
					"homeNotional": "1.5"
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
	assertDecimals(t, tradeVolumes(trades), []string{"150.00", "400.0"})

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

func TestBitmexParsesStringPrice(t *testing.T) {
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
	assertDecimals(t, tradePrices(trades), []string{"100.0"})
	assertDecimals(t, tradeNotionals(trades), []string{"1.5"})
	assertDecimals(t, tradeVolumes(trades), []string{"150.00"})
}
