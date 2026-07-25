package exchanges

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

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
		`{"type":"match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00.123456789Z","price":"100","size":"1","side":"buy"}`,
		`{"type":"match","trade_id":101,"product_id":"BTC-USD","time":"2026-04-08T00:00:01Z","price":"101","size":"2","side":"sell"}`,
		`{"type":"match","trade_id":103,"product_id":"BTC-USD","time":"2026-04-08T00:00:02Z","price":"102","size":"3","side":"buy"}`,
	}

	trades := parseCoinbaseFixture(t, exchange, messages, receivedAt)

	assertStrings(t, tradeUIDs(trades), []string{"100", "101", "103"})
	assertBools(t, tradeSequential(trades), []bool{false, true, false})
	assertStrings(t, tradeExchanges(trades), []string{CoinbaseName, CoinbaseName, CoinbaseName})
	assertStrings(t, tradeSymbols(trades), []string{"BTC-USD", "BTC-USD", "BTC-USD"})
	assertInts(t, tradeTickRules(trades), []int{-1, 1, -1})
	assertInts(t, tradeNanoseconds(trades), []int{789, 0, 0})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})

	wantTimes := []time.Time{
		time.Date(2026, 4, 8, 0, 0, 0, 123456000, time.UTC),
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

	trade, ok, err := exchange.ParseTradeMessage([]byte(`{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]}`), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected non-trade message to be ignored, got %#v", trade)
	}
}

func TestCoinbaseDeduplicatesReconnectReplay(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD", "ETH-USD"})
	receivedAt := time.Now().UTC()
	message := []byte(`{"type":"last_match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`)

	if _, ok, err := exchange.ParseTradeMessage(message, receivedAt); err != nil || !ok {
		t.Fatalf("first last_match = ok %v, err %v; want accepted", ok, err)
	}
	if trade, ok, err := exchange.ParseTradeMessage(message, receivedAt); err != nil || ok {
		t.Fatalf("replayed last_match = %#v, ok %v, err %v; want ignored", trade, ok, err)
	}

	otherProduct := []byte(`{"type":"last_match","trade_id":100,"product_id":"ETH-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`)
	if _, ok, err := exchange.ParseTradeMessage(otherProduct, receivedAt); err != nil || !ok {
		t.Fatalf("same ID on another product = ok %v, err %v; want accepted", ok, err)
	}
}

func TestCoinbaseAcceptsLateUnseenTradeWithoutRegressingSequence(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD"})
	messages := []string{
		`{"type":"match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`,
		`{"type":"match","trade_id":102,"product_id":"BTC-USD","time":"2026-04-08T00:00:02Z","price":"100","size":"1","side":"buy"}`,
		`{"type":"match","trade_id":101,"product_id":"BTC-USD","time":"2026-04-08T00:00:01Z","price":"100","size":"1","side":"buy"}`,
		`{"type":"match","trade_id":103,"product_id":"BTC-USD","time":"2026-04-08T00:00:03Z","price":"100","size":"1","side":"buy"}`,
	}
	trades := parseCoinbaseFixture(t, exchange, messages, time.Now().UTC())
	assertStrings(t, tradeUIDs(trades), []string{"100", "102", "101", "103"})
	assertBools(t, tradeSequential(trades), []bool{false, false, false, true})
}

func TestCoinbaseSubscriptionAndProtocolFailuresSurface(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD", "ETH-USD"})

	_, _, err := exchange.ParseTradeMessage(
		[]byte(`{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]}`),
		time.Now().UTC(),
	)
	if err == nil {
		t.Fatal("incomplete subscription acknowledgement should fail")
	}
	if exchange.subscribed {
		t.Fatal("incomplete subscription acknowledgement should not mark the session ready")
	}

	_, _, err = exchange.ParseTradeMessage(
		[]byte(`{"type":"error","message":"Failed to subscribe","reason":"unknown product"}`),
		time.Now().UTC(),
	)
	if err == nil {
		t.Fatal("protocol error should surface")
	}
}

func TestCoinbaseInvalidTradeIsNotRememberedAsSeen(t *testing.T) {
	exchange := NewCoinbase([]string{"BTC-USD"})
	invalid := []byte(`{"type":"match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"bad","size":"1","side":"buy"}`)
	if _, _, err := exchange.ParseTradeMessage(invalid, time.Now().UTC()); err == nil {
		t.Fatal("invalid trade should fail")
	}

	valid := []byte(`{"type":"match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`)
	if _, ok, err := exchange.ParseTradeMessage(valid, time.Now().UTC()); err != nil || !ok {
		t.Fatalf("corrected trade = ok %v, err %v; want accepted", ok, err)
	}
}

func TestCoinbaseTradesReconnectDeduplicatesLastMatchReplay(t *testing.T) {
	var connections atomic.Int32
	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		connection := connections.Add(1)
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return fmt.Errorf("read subscription on connection %d: %w", connection, err)
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]}`,
		); err != nil {
			return err
		}

		switch connection {
		case 1:
			for _, message := range []string{
				`{"type":"last_match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`,
			} {
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
			}
			return conn.Close(websocket.StatusGoingAway, "test reconnect")
		case 2:
			for _, message := range []string{
				`{"type":"last_match","trade_id":100,"product_id":"BTC-USD","time":"2026-04-08T00:00:00Z","price":"100","size":"1","side":"buy"}`,
				`{"type":"match","trade_id":101,"product_id":"BTC-USD","time":"2026-04-08T00:00:01Z","price":"101","size":"1","side":"sell"}`,
			} {
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
			}
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	exchange := NewCoinbase(
		[]string{"BTC-USD"},
		WithCoinbaseURL(url),
		WithCoinbaseReconnectDelay(0),
	)
	trades, errs := exchange.Trades(ctx)

	var uids []string
	for len(uids) < 2 {
		select {
		case trade, ok := <-trades:
			if !ok {
				t.Fatal("trade channel closed before both reconnected trades arrived")
			}
			uids = append(uids, trade.UID)
		case err, ok := <-errs:
			if !ok {
				t.Fatal("error channel closed before both reconnected trades arrived")
			}
			if err == nil {
				t.Fatal("received nil collector error")
			}
		case <-ctx.Done():
			t.Fatalf("wait for reconnected trades: %v", ctx.Err())
		}
	}
	cancel()
	for trade := range trades {
		uids = append(uids, trade.UID)
	}

	assertStrings(t, uids, []string{"100", "101"})
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want 2", got)
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
