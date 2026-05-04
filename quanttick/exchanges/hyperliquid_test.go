package exchanges

import (
	"reflect"
	"testing"
	"time"
)

func TestHyperliquidSubscriptionMessages(t *testing.T) {
	exchange := NewHyperliquid([]string{"BTC", "ETH"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"method": "subscribe",
			"subscription": map[string]any{
				"type": "trades",
				"coin": "BTC",
			},
		},
		{
			"method": "subscribe",
			"subscription": map[string]any{
				"type": "trades",
				"coin": "ETH",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
}

func TestHyperliquidSpotSymbolSubscriptionMessages(t *testing.T) {
	exchange := NewHyperliquid([]string{"PURR/USDC"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"method": "subscribe",
			"subscription": map[string]any{
				"type": "trades",
				"coin": "PURR/USDC",
			},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
	if exchange.Name() != HyperliquidName {
		t.Fatalf("name = %s, want %s", exchange.Name(), HyperliquidName)
	}
}

func TestHyperliquidParseTradeMessages(t *testing.T) {
	exchange := NewHyperliquid([]string{"BTC"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"channel": "trades",
			"data": [
				{
					"coin": "BTC",
					"side": "B",
					"px": "100",
					"sz": "1.5",
					"hash": "0xabc",
					"time": 1775606400000,
					"tid": 10,
					"users": ["0x1", "0x2"]
				},
				{
					"coin": "BTC",
					"side": "A",
					"px": "101",
					"sz": "2.5",
					"hash": "0xdef",
					"time": 1775606401000,
					"tid": 11,
					"users": ["0x3", "0x4"]
				}
			]
		}`),
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}

	assertStrings(t, tradeUIDs(trades), []string{"1775606400000:BTC:10", "1775606401000:BTC:11"})
	assertStrings(t, tradeExchanges(trades), []string{HyperliquidName, HyperliquidName})
	assertStrings(t, tradeSymbols(trades), []string{"BTC", "BTC"})
	assertInts(t, tradeTickRules(trades), []int{1, -1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101"})
	assertDecimals(t, tradeNotionals(trades), []string{"1.5", "2.5"})
	assertDecimals(t, tradeVolumes(trades), []string{"150.0", "252.5"})
	assertBools(t, tradeSequential(trades), []bool{false, false})

	wantTimes := []time.Time{
		time.Unix(1775606400, 0).UTC(),
		time.Unix(1775606401, 0).UTC(),
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

func TestHyperliquidSpotSymbolParseTradeMessages(t *testing.T) {
	exchange := NewHyperliquid([]string{"PURR/USDC"})
	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"channel": "trades",
			"data": [
				{"coin": "PURR/USDC", "side": "B", "px": "100", "sz": "2", "time": 1775606400000, "tid": 1}
			]
		}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertStrings(t, tradeExchanges(trades), []string{HyperliquidName})
	assertStrings(t, tradeSymbols(trades), []string{"PURR/USDC"})
}

func TestHyperliquidParseIgnoresNonTradeMessages(t *testing.T) {
	exchange := NewHyperliquid([]string{"BTC"})

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"channel": "subscriptionResponse",
			"data": {
				"method": "subscribe",
				"subscription": {"type": "trades", "coin": "BTC"}
			}
		}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 0 {
		t.Fatalf("expected non-trade message to be ignored, got %#v", trades)
	}
}

func TestHyperliquidAcceptsBuySideWord(t *testing.T) {
	exchange := NewHyperliquid([]string{"BTC"})

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"channel": "trades",
			"data": [
				{"coin":"BTC","side":"buy","px":"100","sz":"1","time":1775606400000,"tid":10}
			]
		}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertInts(t, tradeTickRules(trades), []int{1})
}
