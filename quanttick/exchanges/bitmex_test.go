package exchanges

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

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

func TestBitmexRunWaitsForAcksAndPartialsAndDropsPrePartialInserts(t *testing.T) {
	preReadyMessagesSent := make(chan struct{})
	sendFinalReadiness := make(chan struct{})

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		messages := []string{
			`{"success":true,"subscribe":"trade:XBTUSD","request":{"op":"subscribe","args":["trade:XBTUSD","trade:ETHUSD"]}}`,
			`{"table":"trade","action":"insert","data":[{"trdMatchID":"drop-before-partial","symbol":"XBTUSD","timestamp":"2026-04-08T00:00:00Z","side":"Buy","price":100,"homeNotional":1}]}`,
			`{"table":"trade","action":"partial","filter":{"symbol":"XBTUSD"},"data":[{"trdMatchID":"xbt-snapshot","symbol":"XBTUSD","timestamp":"2026-04-08T00:00:01Z","side":"Buy","price":100,"homeNotional":1}]}`,
			`{"table":"trade","action":"insert","data":[{"trdMatchID":"xbt-after-partial","symbol":"XBTUSD","timestamp":"2026-04-08T00:00:02Z","side":"Buy","price":100,"homeNotional":1}]}`,
		}
		for _, message := range messages {
			if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
				return err
			}
		}
		close(preReadyMessagesSent)
		select {
		case <-sendFinalReadiness:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"success":true,"subscribe":"trade:ETHUSD","request":{"op":"subscribe","args":["trade:XBTUSD","trade:ETHUSD"]}}`,
		); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"table":"trade","action":"partial","filter":{"symbol":"ETHUSD"},"data":[{"trdMatchID":"eth-snapshot","symbol":"ETHUSD","timestamp":"2026-04-08T00:00:03Z","side":"Sell","price":100,"homeNotional":1}]}`,
		); err != nil {
			return err
		}
		return conn.Close(websocket.StatusNormalClosure, "")
	})

	exchange := NewBitmex(
		[]string{"XBTUSD", "ETHUSD"},
		WithBitmexURL(url),
		WithBitmexSubscriptionTimeout(time.Second),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 3)
	done := make(chan error, 1)
	go func() {
		done <- exchange.run(ctx, trades, newBitmexSeenTrades(bitmexSeenTradeLimit))
	}()

	select {
	case <-preReadyMessagesSent:
	case <-ctx.Done():
		t.Fatal("timed out waiting for pre-readiness messages")
	}
	select {
	case trade := <-trades:
		t.Fatalf("trade emitted before all acknowledgements and partials: %#v", trade)
	case <-time.After(20 * time.Millisecond):
	}
	close(sendFinalReadiness)

	gotUIDs := make([]string, 0, 3)
	for len(gotUIDs) < 3 {
		select {
		case trade := <-trades:
			gotUIDs = append(gotUIDs, trade.UID)
		case <-ctx.Done():
			t.Fatal("timed out waiting for ready trades")
		}
	}
	wantUIDs := []string{"xbt-snapshot", "xbt-after-partial", "eth-snapshot"}
	if !reflect.DeepEqual(gotUIDs, wantUIDs) {
		t.Fatalf("trade uids = %#v, want %#v", gotUIDs, wantUIDs)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("bitmex run did not finish")
	}
}

func TestBitmexSubscriptionReadinessRequiresAckAndPartial(t *testing.T) {
	tests := []struct {
		name     string
		messages []string
		want     string
	}{
		{
			name: "ack without partial",
			messages: []string{
				`{"success":true,"subscribe":"trade:XBTUSD","request":{"op":"subscribe","args":["trade:XBTUSD"]}}`,
			},
			want: "missing partial snapshots: XBTUSD",
		},
		{
			name: "partial without ack",
			messages: []string{
				`{"table":"trade","action":"partial","filter":{"symbol":"XBTUSD"},"data":[]}`,
			},
			want: "missing acknowledgements: XBTUSD",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				for _, message := range tc.messages {
					if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
						return err
					}
				}
				_, _ = readExchangeWebSocketMessage(ctx, conn)
				return nil
			})

			exchange := NewBitmex(
				[]string{"XBTUSD"},
				WithBitmexURL(url),
				WithBitmexSubscriptionTimeout(30*time.Millisecond),
			)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := exchange.run(ctx, make(chan quanttick.TradeEvent, 1), newBitmexSeenTrades(bitmexSeenTradeLimit))
			if err == nil || !strings.Contains(err.Error(), "subscription readiness timed out") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("readiness error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseBitmexSubscriptionResponseRejectsProtocolError(t *testing.T) {
	_, _, err := parseBitmexSubscriptionResponse(
		[]byte(`{"status":429,"error":"Rate limit exceeded","request":{"op":"subscribe","args":"trade:XBTUSD"}}`),
	)
	if err == nil || !strings.Contains(err.Error(), "Rate limit exceeded") {
		t.Fatalf("protocol error = %v", err)
	}
}

func TestBitmexRejectsNonPositivePrice(t *testing.T) {
	exchange := NewBitmex([]string{"XBTUSD"})
	_, err := exchange.ParseTradeMessage(
		[]byte(`{"table":"trade","action":"insert","data":[{"trdMatchID":"a","symbol":"XBTUSD","timestamp":"2026-04-08T00:00:00Z","side":"Buy","price":0,"foreignNotional":1}]}`),
		time.Now().UTC(),
	)
	if err == nil {
		t.Fatal("zero price should fail instead of dividing by zero")
	}
}
