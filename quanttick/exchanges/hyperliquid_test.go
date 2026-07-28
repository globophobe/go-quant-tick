package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

func TestHyperliquidRecoversReconnectGapBeforeSnapshotReplay(t *testing.T) {
	var recoveryRequests atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recoveryRequests.Add(1)
		if r.URL.Path != "/info" {
			t.Errorf("recovery path = %s", r.URL.Path)
		}
		var request map[string]string
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode recovery request: %v", err)
		}
		if request["type"] != "recentTrades" || request["coin"] != "BTC" {
			t.Errorf("recovery request = %#v", request)
		}
		_, _ = fmt.Fprint(w, `[
			{"coin":"BTC","side":"A","px":"101","sz":"2","time":1775606401000,"tid":11},
			{"coin":"BTC","side":"B","px":"100","sz":"1","time":1775606400000,"tid":10}
		]`)
	}))
	t.Cleanup(restServer.Close)

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

		switch connections.Add(1) {
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
				`{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"101","sz":"2","time":1775606401000,"tid":11},{"coin":"BTC","side":"B","px":"102","sz":"3","time":1775606402000,"tid":12}]}`,
			); err != nil {
				return err
			}
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		default:
			return fmt.Errorf("unexpected connection %d", connections.Load())
		}
	})

	exchange := NewHyperliquid(
		[]string{"BTC"},
		WithHyperliquidURL(url),
		WithHyperliquidRESTURL(restServer.URL),
		WithHyperliquidReconnectDelay(time.Millisecond),
		WithHyperliquidSubscriptionTimeout(time.Second),
		WithHyperliquidHeartbeatInterval(0),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, errs := exchange.Trades(ctx)

	var uids []string
	for len(uids) < 3 {
		select {
		case trade := <-trades:
			uids = append(uids, trade.UID)
		case err := <-errs:
			t.Fatalf("recovery error = %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for recovered Hyperliquid trades")
		}
	}
	cancel()

	want := []string{
		"1775606400000:BTC:10",
		"1775606401000:BTC:11",
		"1775606402000:BTC:12",
	}
	if !reflect.DeepEqual(uids, want) {
		t.Fatalf("trade uids = %#v, want %#v", uids, want)
	}
	if got := recoveryRequests.Load(); got != 1 {
		t.Fatalf("recovery requests = %d, want 1", got)
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

func TestHyperliquidRecentTradesExtraWeightUsesResponseRows(t *testing.T) {
	tests := []struct {
		rows int
		want int
	}{
		{rows: 0, want: 0},
		{rows: 1, want: 1},
		{rows: 20, want: 1},
		{rows: 21, want: 2},
	}
	for _, test := range tests {
		if got := hyperliquidRecentTradesExtraWeight(test.rows); got != test.want {
			t.Errorf("rows=%d extra weight=%d, want %d", test.rows, got, test.want)
		}
	}
}

func TestHyperliquidRecentTradesReservesResponseWeightBeforeRequest(t *testing.T) {
	limiter := newRequestWindowLimiter(hyperliquidRateLimitWeightPerMinute, time.Minute)
	if err := limiter.wait(context.Background(), 1180); err != nil {
		t.Fatalf("fill recovery rate window: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := limiter.reserve(ctx, hyperliquidRecentTradesMaxWeight); err == nil {
		t.Fatal("recentTrades request should wait when its response weight exceeds capacity")
	}
}

func TestHyperliquidRejectsUnexpectedRecentTradesResponseSize(t *testing.T) {
	rows := make([]string, hyperliquidRecentTradesMaxRows+1)
	for index := range rows {
		rows[index] = fmt.Sprintf(
			`{"coin":"BTC","side":"A","px":"101","sz":"2","time":1775606401000,"tid":%d}`,
			index,
		)
	}
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "[%s]", strings.Join(rows, ","))
	}))
	t.Cleanup(restServer.Close)

	exchange := NewHyperliquid(
		[]string{"BTC"},
		WithHyperliquidRESTURL(restServer.URL),
	)
	_, err := exchange.recoverSymbol(context.Background(), "BTC", "unused")
	if err == nil || !strings.Contains(err.Error(), "expected at most 10") {
		t.Fatalf("recovery error = %v", err)
	}
}

func TestHyperliquidUsesWebSocketSnapshotWhenRESTCursorIsAbsent(t *testing.T) {
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `[
			{"coin":"BTC","side":"A","px":"99","sz":"1","time":1775606399000,"tid":9}
		]`)
	}))
	t.Cleanup(restServer.Close)

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		for _, message := range []string{
			`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}`,
			`{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"101","sz":"2","time":1775606401000,"tid":11},{"coin":"BTC","side":"B","px":"102","sz":"3","time":1775606402000,"tid":12}]}`,
		} {
			if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
				return err
			}
		}
		_, _ = readExchangeWebSocketMessage(ctx, conn)
		return nil
	})

	exchange := NewHyperliquid(
		[]string{"BTC"},
		WithHyperliquidURL(url),
		WithHyperliquidRESTURL(restServer.URL),
		WithHyperliquidSubscriptionTimeout(time.Second),
		WithHyperliquidHeartbeatInterval(0),
	)
	exchange.lastUIDs["BTC"] = "1775606400000:BTC:10"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 2)
	done := make(chan error, 1)
	go func() {
		done <- exchange.run(ctx, trades, newSeenTradeIDs(hyperliquidSeenTradeLimit))
	}()

	var uids []string
	for len(uids) < 2 {
		select {
		case trade := <-trades:
			uids = append(uids, trade.UID)
		case err := <-done:
			t.Fatalf("run ended before snapshot replay: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for Hyperliquid reconnect snapshot")
		}
	}
	cancel()
	want := []string{"1775606401000:BTC:11", "1775606402000:BTC:12"}
	if !reflect.DeepEqual(uids, want) {
		t.Fatalf("trade uids = %#v, want %#v", uids, want)
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
