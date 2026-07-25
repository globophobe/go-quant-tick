package exchanges

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
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

func TestHyperliquidRunBuffersUntilAllAcksAndSendsHeartbeat(t *testing.T) {
	preAckTradeSent := make(chan struct{})
	sendFinalAck := make(chan struct{})

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		for range 2 {
			if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
				return err
			}
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}`,
		); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"channel":"trades","data":[{"coin":"ETH","side":"B","px":"100","sz":"1","time":1775606400000,"tid":10}]}`,
		); err != nil {
			return err
		}
		close(preAckTradeSent)
		select {
		case <-sendFinalAck:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"ETH"}}}`,
		); err != nil {
			return err
		}

		heartbeat, err := readExchangeWebSocketMessage(ctx, conn)
		if err != nil {
			return err
		}
		if string(heartbeat) != `{"method":"ping"}` {
			return fmt.Errorf("heartbeat = %s", heartbeat)
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, `{"channel":"pong"}`); err != nil {
			return err
		}
		return conn.Close(websocket.StatusNormalClosure, "")
	})

	exchange := NewHyperliquid(
		[]string{"BTC", "ETH"},
		WithHyperliquidURL(url),
		WithHyperliquidSubscriptionTimeout(time.Second),
		WithHyperliquidHeartbeatInterval(100*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 1)
	done := make(chan error, 1)
	go func() {
		done <- exchange.run(ctx, trades, newSeenTradeIDs(hyperliquidSeenTradeLimit))
	}()

	select {
	case <-preAckTradeSent:
	case <-ctx.Done():
		t.Fatal("timed out waiting for pre-ack trade")
	}
	select {
	case trade := <-trades:
		t.Fatalf("trade emitted before all acknowledgements: %#v", trade)
	case <-time.After(20 * time.Millisecond):
	}
	close(sendFinalAck)

	select {
	case trade := <-trades:
		if trade.UID != "1775606400000:ETH:10" {
			t.Fatalf("trade uid = %s", trade.UID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for buffered trade")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("hyperliquid run did not finish")
	}
}

func TestHyperliquidTradesDeduplicatesReconnectReplay(t *testing.T) {
	var connections atomic.Int32
	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}`,
		); err != nil {
			return err
		}

		connection := connections.Add(1)
		switch connection {
		case 1:
			if err := writeExchangeWebSocketMessage(
				ctx,
				conn,
				`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"100","sz":"1","time":1775606400000,"tid":10}]}`,
			); err != nil {
				return err
			}
			return conn.Close(websocket.StatusNormalClosure, "")
		case 2:
			if err := writeExchangeWebSocketMessage(
				ctx,
				conn,
				`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"100","sz":"1","time":1775606400000,"tid":10},{"coin":"BTC","side":"A","px":"101","sz":"1","time":1775606401000,"tid":11}]}`,
			); err != nil {
				return err
			}
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	})

	exchange := NewHyperliquid(
		[]string{"BTC"},
		WithHyperliquidURL(url),
		WithHyperliquidReconnectDelay(time.Millisecond),
		WithHyperliquidSubscriptionTimeout(time.Second),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, _ := exchange.Trades(ctx)

	var uids []string
	for len(uids) < 2 {
		select {
		case trade := <-trades:
			uids = append(uids, trade.UID)
		case <-ctx.Done():
			t.Fatal("timed out waiting for reconnect trades")
		}
	}
	cancel()

	want := []string{"1775606400000:BTC:10", "1775606401000:BTC:11"}
	if !reflect.DeepEqual(uids, want) {
		t.Fatalf("trade uids = %#v, want %#v", uids, want)
	}
	if got := connections.Load(); got < 2 {
		t.Fatalf("connections = %d, want at least 2", got)
	}
}

func TestHyperliquidProtocolErrorSurfaces(t *testing.T) {
	err := parseHyperliquidProtocolError(
		[]byte(`{"channel":"error","data":"Invalid subscription"}`),
	)
	if err == nil || !strings.Contains(err.Error(), "Invalid subscription") {
		t.Fatalf("protocol error = %v", err)
	}
}

func TestHyperliquidRejectsUnknownTradeSide(t *testing.T) {
	exchange := NewHyperliquid([]string{"BTC"})
	_, err := exchange.ParseTradeMessage(
		[]byte(`{"channel":"trades","data":[{"coin":"BTC","side":"wat","px":"100","sz":"1","time":1775606400000,"tid":10}]}`),
		time.Now().UTC(),
	)
	if err == nil {
		t.Fatal("unknown trade side should fail")
	}
}
