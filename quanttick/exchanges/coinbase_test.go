package exchanges

import (
	"reflect"
	"testing"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestCoinbaseSubscriptionMessages(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD", "ETH-USD"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"type":        "subscribe",
			"product_ids": []string{"BTC-USD", "ETH-USD"},
			"channels":    []string{"matches"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
}

func TestCoinbaseParseTradeMessages(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"type":"match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`,
		`{"type":"match","trade_id":101,"product_id":"BTC-USD","time":"2026-04-08T00:00:01Z","price":"101","size":"2","side":"sell"}`,
		`{"type":"match","trade_id":103,"product_id":"BTC-USD","time":"2026-04-08T00:00:02Z","price":"102","size":"3","side":"buy"}`,
	}

	trades := parseCoinbaseFixture(t, exchange, messages, receivedAt)

	assertStrings(t, tradeUIDs(trades), []string{"100", "101", "103"})
	assertBools(t, tradeSequential(trades), []bool{true, true, false})
	assertStrings(t, tradeExchanges(trades), []string{CoinbaseName, CoinbaseName, CoinbaseName})
	assertStrings(t, tradeSymbols(trades), []string{"BTC-USD", "BTC-USD", "BTC-USD"})
	assertInts(t, tradeTickRules(trades), []int{-1, 1, -1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})

	wantTimes := []time.Time{
		time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 8, 0, 0, 1, 0, time.UTC),
		time.Date(2026, 4, 8, 0, 0, 2, 0, time.UTC),
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

func TestCoinbaseAcceptsLastMatchMessages(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD"})

	trade, ok, err := exchange.ParseTradeMessage(
		[]byte(`{"type":"last_match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected last_match message to parse")
	}
	if trade.UID != "100" {
		t.Fatalf("uid = %s, want 100", trade.UID)
	}
}

func TestCoinbaseParseIgnoresNonTradeMessages(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD"})

	trade, ok, err := exchange.ParseTradeMessage([]byte(`{"type":"subscriptions","channels":[]}`), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected non-trade message to be ignored, got %#v", trade)
	}
}

func parseCoinbaseFixture(t *testing.T, exchange *Coinbase, messages []string, receivedAt time.Time) []quanttick.TradeEvent {
	t.Helper()

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
	return trades
}
