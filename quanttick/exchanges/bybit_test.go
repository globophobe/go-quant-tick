package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestBybitSubscriptionMessages(t *testing.T) {
	exchange := NewBybitLinear([]string{"BTCUSDT", "ETHUSDT"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"req_id": "trades",
			"op":     "subscribe",
			"args":   []string{"publicTrade.BTCUSDT", "publicTrade.ETHUSDT"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
	if exchange.Name() != BybitLinearName {
		t.Fatalf("name = %s, want %s", exchange.Name(), BybitLinearName)
	}
	if exchange.URL != BybitLinearURL {
		t.Fatalf("url = %s, want %s", exchange.URL, BybitLinearURL)
	}
}

func TestBybitProductConstructors(t *testing.T) {
	tests := []struct {
		name     string
		exchange *Bybit
		wantName string
		category string
		url      string
	}{
		{"spot", NewBybitSpot([]string{"BTCUSDT"}), BybitSpotName, "spot", BybitSpotURL},
		{"linear", NewBybitLinear([]string{"BTCUSDT"}), BybitLinearName, "linear", BybitLinearURL},
		{"inverse", NewBybitInverse([]string{"BTCUSD"}), BybitInverseName, "inverse", BybitInverseURL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.exchange.Name() != test.wantName {
				t.Fatalf("name = %s, want %s", test.exchange.Name(), test.wantName)
			}
			if test.exchange.category != test.category {
				t.Fatalf("category = %s, want %s", test.exchange.category, test.category)
			}
			if test.exchange.URL != test.url {
				t.Fatalf("URL = %s, want %s", test.exchange.URL, test.url)
			}
			if test.exchange.recoveryIPLimiter != sharedBybitRecoveryIPLimiter {
				t.Fatal("Bybit adapters must share the IP request limiter")
			}
			if test.exchange.recoveryIPThrottle != sharedBybitRecoveryIPThrottle {
				t.Fatal("Bybit adapters must share the IP embargo throttle")
			}
		})
	}
}

func TestBybitParseTradeMessages(t *testing.T) {
	exchange := NewBybitLinear([]string{"BTCUSDT"})
	receivedAt := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)

	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"topic":"publicTrade.BTCUSDT",
			"type":"snapshot",
			"ts":1784937600124,
			"data":[
				{"T":1784937600000,"s":"BTCUSDT","S":"Buy","v":"0.042","p":"6698.5","L":"PlusTick","i":"019trade-a","BT":false,"seq":123},
				{"T":1784937600001,"s":"BTCUSDT","S":"Sell","v":"0.072","p":"6698","L":"MinusTick","i":"019trade-b","BT":false,"seq":123}
			]
		}`),
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}

	assertStrings(t, tradeUIDs(trades), []string{"019trade-a", "019trade-b"})
	assertStrings(t, tradeExchanges(trades), []string{BybitLinearName, BybitLinearName})
	assertStrings(t, tradeSymbols(trades), []string{"BTCUSDT", "BTCUSDT"})
	assertInts(t, tradeTickRules(trades), []int{1, -1})
	assertDecimals(t, tradePrices(trades), []string{"6698.5", "6698"})
	assertDecimals(t, tradeNotionals(trades), []string{"0.042", "0.072"})
	assertDecimals(t, tradeVolumes(trades), []string{"281.3370", "482.256"})
	assertBools(t, tradeSequential(trades), []bool{false, false})

	wantTimes := []time.Time{
		time.UnixMilli(1784937600000).UTC(),
		time.UnixMilli(1784937600001).UTC(),
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

func TestBybitInverseTradeNormalizesUSDContractsToBTC(t *testing.T) {
	exchange := NewBybitInverse([]string{"BTCUSD"})
	trades, err := exchange.ParseTradeMessage(
		[]byte(`{
			"topic":"publicTrade.BTCUSD",
			"type":"snapshot",
			"data":[
				{"T":1784937600000,"s":"BTCUSD","S":"Buy","v":"1000","p":"50000","i":"inverse-trade","seq":123}
			]
		}`),
		time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertStrings(t, tradeExchanges(trades), []string{BybitInverseName})
	assertDecimals(t, tradeNotionals(trades), []string{"0.02"})
	assertDecimals(t, tradeVolumes(trades), []string{"1000"})
}

func TestBybitParseIgnoresNonTradeMessages(t *testing.T) {
	exchange := NewBybitLinear([]string{"BTCUSDT"})
	messages := []string{
		`{"success":true,"ret_msg":"","req_id":"trades","op":"subscribe"}`,
		`{"success":true,"ret_msg":"pong","op":"ping"}`,
		`{"topic":"tickers.BTCUSDT","type":"snapshot","data":{}}`,
	}
	for _, message := range messages {
		trades, err := exchange.ParseTradeMessage([]byte(message), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if len(trades) != 0 {
			t.Fatalf("expected non-trade message to be ignored, got %#v", trades)
		}
	}
}

func TestBybitRejectsUnexpectedTopicSymbolAndSide(t *testing.T) {
	exchange := NewBybitLinear([]string{"BTCUSDT"})
	cases := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "unconfigured topic",
			message: `{"topic":"publicTrade.ETHUSDT","type":"snapshot","data":[]}`,
			want:    "unexpected topic",
		},
		{
			name:    "mismatched symbol",
			message: `{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1,"s":"ETHUSDT","S":"Buy","v":"1","p":"100","i":"a"}]}`,
			want:    "does not match topic",
		},
		{
			name:    "unknown side",
			message: `{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1,"s":"BTCUSDT","S":"Unknown","v":"1","p":"100","i":"a"}]}`,
			want:    "invalid bybit side",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exchange.ParseTradeMessage([]byte(tc.message), time.Now().UTC())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestBybitRunBuffersUntilAckAndSendsHeartbeat(t *testing.T) {
	preAckTradeSent := make(chan struct{})
	sendAck := make(chan struct{})

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		subscription, err := readExchangeWebSocketMessage(ctx, conn)
		if err != nil {
			return err
		}
		var request map[string]any
		if err := json.Unmarshal(subscription, &request); err != nil {
			return err
		}
		if request["op"] != "subscribe" || request["req_id"] != "trades" {
			return fmt.Errorf("unexpected subscription: %s", subscription)
		}

		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1784937600000,"s":"BTCUSDT","S":"Buy","v":"1","p":"100","i":"a","seq":1}]}`,
		); err != nil {
			return err
		}
		close(preAckTradeSent)
		select {
		case <-sendAck:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"success":true,"ret_msg":"","conn_id":"test","req_id":"trades","op":"subscribe"}`,
		); err != nil {
			return err
		}

		heartbeat, err := readExchangeWebSocketMessage(ctx, conn)
		if err != nil {
			return err
		}
		if string(heartbeat) != `{"op":"ping"}` {
			return fmt.Errorf("heartbeat = %s", heartbeat)
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, `{"success":true,"ret_msg":"pong","op":"ping"}`); err != nil {
			return err
		}
		return conn.Close(websocket.StatusNormalClosure, "")
	})

	exchange := NewBybitLinear(
		[]string{"BTCUSDT"},
		WithBybitURL(url),
		WithBybitSubscriptionTimeout(time.Second),
		WithBybitHeartbeatInterval(100*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 1)
	done := make(chan error, 1)
	go func() {
		done <- exchange.run(ctx, trades, newSeenTradeIDs(bybitSeenTradeLimit))
	}()

	select {
	case <-preAckTradeSent:
	case <-ctx.Done():
		t.Fatal("timed out waiting for pre-ack trade")
	}
	select {
	case trade := <-trades:
		t.Fatalf("trade emitted before acknowledgement: %#v", trade)
	case <-time.After(20 * time.Millisecond):
	}
	close(sendAck)

	select {
	case trade := <-trades:
		if trade.UID != "a" {
			t.Fatalf("trade uid = %s, want a", trade.UID)
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
		t.Fatal("bybit run did not finish")
	}
}

func TestBybitRecoversReconnectGapBeforeWebSocketReplay(t *testing.T) {
	var recoveryRequests atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := recoveryRequests.Add(1)
		if r.URL.Path != "/v5/market/recent-trade" {
			t.Errorf("recovery path = %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("category"); got != "linear" {
			t.Errorf("recovery category = %s", got)
		}
		if got := r.URL.Query().Get("symbol"); got != "BTCUSDT" {
			t.Errorf("recovery symbol = %s", got)
		}
		if requestNumber == 1 {
			_, _ = fmt.Fprint(w, `{
				"retCode":0,
				"retMsg":"OK",
				"result":{"list":[
					{"execId":"b","symbol":"BTCUSDT","price":"101","size":"2","side":"Sell","time":"1784937600001"},
					{"execId":"a","symbol":"BTCUSDT","price":"100","size":"1","side":"Buy","time":"1784937600000"}
				]}
			}`)
			return
		}
		_, _ = fmt.Fprint(w, `{
			"retCode":0,
			"retMsg":"OK",
			"result":{"list":[
				{"execId":"c","symbol":"BTCUSDT","price":"102","size":"3","side":"Buy","time":"1784937600002"},
				{"execId":"b","symbol":"BTCUSDT","price":"101","size":"2","side":"Sell","time":"1784937600001"},
				{"execId":"a","symbol":"BTCUSDT","price":"100","size":"1","side":"Buy","time":"1784937600000"}
			]}
		}`)
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
			`{"success":true,"ret_msg":"","conn_id":"test","req_id":"trades","op":"subscribe"}`,
		); err != nil {
			return err
		}

		switch connections.Add(1) {
		case 1:
			if err := writeExchangeWebSocketMessage(
				ctx,
				conn,
				`{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1784937600000,"s":"BTCUSDT","S":"Buy","v":"1","p":"100","i":"a","seq":1}]}`,
			); err != nil {
				return err
			}
			return conn.Close(websocket.StatusNormalClosure, "")
		case 2:
			if err := writeExchangeWebSocketMessage(
				ctx,
				conn,
				`{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1784937600002,"s":"BTCUSDT","S":"Buy","v":"3","p":"102","i":"c","seq":3},{"T":1784937600003,"s":"BTCUSDT","S":"Sell","v":"4","p":"103","i":"d","seq":4}]}`,
			); err != nil {
				return err
			}
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		default:
			return fmt.Errorf("unexpected connection %d", connections.Load())
		}
	})

	exchange := NewBybitLinear(
		[]string{"BTCUSDT"},
		WithBybitURL(url),
		WithBybitRESTURL(restServer.URL),
		WithBybitReconnectDelay(time.Millisecond),
		WithBybitSubscriptionTimeout(time.Second),
		WithBybitHeartbeatInterval(0),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, errs := exchange.Trades(ctx)

	var uids []string
	for len(uids) < 4 {
		select {
		case trade := <-trades:
			uids = append(uids, trade.UID)
		case err := <-errs:
			t.Fatalf("recovery error = %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for recovered Bybit trades")
		}
	}
	cancel()

	want := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(uids, want) {
		t.Fatalf("trade uids = %#v, want %#v", uids, want)
	}
	if got := recoveryRequests.Load(); got != 2 {
		t.Fatalf("recovery requests = %d, want 2", got)
	}
}

func TestBybitRejectsRecoveryWithoutRESTToWebSocketOverlap(t *testing.T) {
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
			"retCode":0,
			"retMsg":"OK",
			"result":{"list":[
				{"execId":"b","symbol":"BTCUSDT","price":"101","size":"2","side":"Sell","time":"1784937600001"},
				{"execId":"a","symbol":"BTCUSDT","price":"100","size":"1","side":"Buy","time":"1784937600000"}
			]}
		}`)
	}))
	t.Cleanup(restServer.Close)

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		for _, message := range []string{
			`{"success":true,"ret_msg":"","conn_id":"test","req_id":"trades","op":"subscribe"}`,
			`{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1784937600003,"s":"BTCUSDT","S":"Sell","v":"4","p":"103","i":"d","seq":4}]}`,
		} {
			if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
				return err
			}
		}
		_, _ = readExchangeWebSocketMessage(ctx, conn)
		return nil
	})

	exchange := NewBybitLinear(
		[]string{"BTCUSDT"},
		WithBybitURL(url),
		WithBybitRESTURL(restServer.URL),
		WithBybitSubscriptionTimeout(time.Second),
		WithBybitHeartbeatInterval(0),
	)
	exchange.lastUIDs["BTCUSDT"] = "a"

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 4)
	done := make(chan error, 1)
	go func() {
		done <- exchange.run(ctx, trades, newSeenTradeIDs(bybitSeenTradeLimit))
	}()

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("run error = %v, want overlap failure", err)
	}
	select {
	case trade := <-trades:
		t.Fatalf("unproven handoff emitted trade %#v", trade)
	default:
	}
}

func TestBybitQuietSymbolDoesNotBlockActiveSymbolRecovery(t *testing.T) {
	var btcRequests atomic.Int32
	var quietRequests atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("symbol") {
		case "BTCUSDT":
			if btcRequests.Add(1) > 1 {
				http.Error(w, "BTC cursor fell outside the recent-trade window", http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprint(w, `{
				"retCode":0,
				"retMsg":"OK",
				"result":{"list":[
					{"execId":"c","symbol":"BTCUSDT","price":"102","size":"3","side":"Buy","time":"1784937600002"},
					{"execId":"b","symbol":"BTCUSDT","price":"101","size":"2","side":"Sell","time":"1784937600001"},
					{"execId":"a","symbol":"BTCUSDT","price":"100","size":"1","side":"Buy","time":"1784937600000"}
				]}
			}`)
		case "QUIETUSDT":
			quietRequests.Add(1)
			_, _ = fmt.Fprint(w, `{
				"retCode":0,
				"retMsg":"OK",
				"result":{"list":[
					{"execId":"q","symbol":"QUIETUSDT","price":"10","size":"1","side":"Buy","time":"1784937600000"}
				]}
			}`)
		default:
			http.Error(w, "unexpected symbol", http.StatusBadRequest)
		}
	}))
	t.Cleanup(restServer.Close)

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		for _, message := range []string{
			`{"success":true,"ret_msg":"","conn_id":"test","req_id":"trades","op":"subscribe"}`,
			`{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1784937600002,"s":"BTCUSDT","S":"Buy","v":"3","p":"102","i":"c","seq":3},{"T":1784937600003,"s":"BTCUSDT","S":"Sell","v":"4","p":"103","i":"d","seq":4}]}`,
		} {
			if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
				return err
			}
		}
		_, _ = readExchangeWebSocketMessage(ctx, conn)
		return nil
	})

	exchange := NewBybitLinear(
		[]string{"BTCUSDT", "QUIETUSDT"},
		WithBybitURL(url),
		WithBybitRESTURL(restServer.URL),
		WithBybitSubscriptionTimeout(time.Second),
		WithBybitHeartbeatInterval(0),
	)
	exchange.lastUIDs["BTCUSDT"] = "a"
	exchange.lastUIDs["QUIETUSDT"] = "q"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 4)
	done := make(chan error, 1)
	go func() {
		done <- exchange.run(ctx, trades, newSeenTradeIDs(bybitSeenTradeLimit))
	}()

	var uids []string
	for len(uids) < 3 {
		select {
		case trade := <-trades:
			uids = append(uids, trade.UID)
		case err := <-done:
			t.Fatalf("run ended before recovery completed: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for recovered Bybit trades")
		}
	}
	cancel()
	if want := []string{"b", "c", "d"}; !reflect.DeepEqual(uids, want) {
		t.Fatalf("trade uids = %#v, want %#v", uids, want)
	}
	if got := btcRequests.Load(); got != 1 {
		t.Fatalf("BTC requests = %d, want 1", got)
	}
	if got := quietRequests.Load(); got != 1 {
		t.Fatalf("quiet requests = %d, want 1", got)
	}
}

func TestBybitRecoveryRetriesAPIRateLimit(t *testing.T) {
	var requests atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0.001")
			_, _ = fmt.Fprint(w, `{"retCode":10006,"retMsg":"Too many visits","result":{"list":[]}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{
			"retCode":0,
			"retMsg":"OK",
			"result":{"list":[
				{"execId":"b","symbol":"BTCUSDT","price":"101","size":"2","side":"Sell","time":"1784937600001"},
				{"execId":"a","symbol":"BTCUSDT","price":"100","size":"1","side":"Buy","time":"1784937600000"}
			]}
		}`)
	}))
	t.Cleanup(restServer.Close)

	exchange := NewBybitLinear([]string{"BTCUSDT"}, WithBybitRESTURL(restServer.URL))
	recovered, err := exchange.recoverSymbol(context.Background(), "BTCUSDT", "a")
	if err != nil {
		t.Fatalf("recover symbol: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
	if len(recovered) != 1 || recovered[0].UID != "b" {
		t.Fatalf("recovered = %#v", recovered)
	}
}

func TestBybitResponseDelayUsesResetTimestamp(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	headers := make(http.Header)
	headers.Set("X-Bapi-Limit-Status", "0")
	headers.Set("X-Bapi-Limit-Reset-Timestamp", strconv.FormatInt(now.Add(250*time.Millisecond).UnixMilli(), 10))
	delay, err := bybitResponseDelay(headers, now)
	if err != nil {
		t.Fatalf("response delay: %v", err)
	}
	if delay != 250*time.Millisecond {
		t.Fatalf("response delay = %s, want 250ms", delay)
	}
}

func TestBybitRecoveryPausesHTTPAfterForbiddenResponse(t *testing.T) {
	var requests atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "access too frequent", http.StatusForbidden)
	}))
	t.Cleanup(restServer.Close)

	ipThrottle := newRESTThrottle(0)
	ipLimiter := newRequestWindowLimiter(bybitIPRequestLimit, bybitIPRequestWindow)
	linear := NewBybitLinear([]string{"BTCUSDT"}, WithBybitRESTURL(restServer.URL))
	inverse := NewBybitInverse([]string{"BTCUSD"}, WithBybitRESTURL(restServer.URL))
	linear.recoveryIPThrottle = ipThrottle
	inverse.recoveryIPThrottle = ipThrottle
	linear.recoveryIPLimiter = ipLimiter
	inverse.recoveryIPLimiter = ipLimiter

	_, err := linear.recoverSymbol(context.Background(), "BTCUSDT", "a")
	if err == nil || !strings.Contains(err.Error(), "HTTP recovery paused") {
		t.Fatalf("first recovery error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = inverse.recoverSymbol(ctx, "BTCUSD", "a")
	if err == nil || !strings.Contains(err.Error(), "IP embargo") {
		t.Fatalf("cross-adapter recovery error = %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("HTTP requests = %d, want 1 during the IP-ban pause", got)
	}
	ipLimiter.mu.Lock()
	used := ipLimiter.used
	ipLimiter.mu.Unlock()
	if used != 1 {
		t.Fatalf("IP request budget used = %d, want 1", used)
	}
}

func TestBybitSubscriptionErrorSurfaces(t *testing.T) {
	isAck, err := parseBybitSubscriptionResponse(
		[]byte(`{"success":false,"ret_msg":"symbol invalid","req_id":"trades","op":"subscribe"}`),
	)
	if isAck {
		t.Fatal("failed subscription should not be acknowledged")
	}
	if err == nil || !strings.Contains(err.Error(), "symbol invalid") {
		t.Fatalf("subscription error = %v", err)
	}
}
