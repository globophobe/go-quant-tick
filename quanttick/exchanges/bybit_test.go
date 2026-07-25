package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestBybitSubscriptionMessages(t *testing.T) {
	exchange := NewBybit([]string{"BTCUSDT", "ETHUSDT"})

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
	if exchange.Name() != BybitName {
		t.Fatalf("name = %s, want %s", exchange.Name(), BybitName)
	}
	if exchange.URL != BybitURL {
		t.Fatalf("url = %s, want %s", exchange.URL, BybitURL)
	}
}

func TestBybitParseTradeMessages(t *testing.T) {
	exchange := NewBybit([]string{"BTCUSDT"})
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
	assertStrings(t, tradeExchanges(trades), []string{BybitName, BybitName})
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

func TestBybitParseIgnoresNonTradeMessages(t *testing.T) {
	exchange := NewBybit([]string{"BTCUSDT"})
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
	exchange := NewBybit([]string{"BTCUSDT"})
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

	exchange := NewBybit(
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

func TestBybitTradesDeduplicatesReconnectReplay(t *testing.T) {
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

		connection := connections.Add(1)
		switch connection {
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
				`{"topic":"publicTrade.BTCUSDT","type":"snapshot","data":[{"T":1784937600000,"s":"BTCUSDT","S":"Buy","v":"1","p":"100","i":"a","seq":1},{"T":1784937600001,"s":"BTCUSDT","S":"Sell","v":"2","p":"101","i":"b","seq":2}]}`,
			); err != nil {
				return err
			}
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		default:
			return fmt.Errorf("unexpected connection %d", connection)
		}
	})

	exchange := NewBybit(
		[]string{"BTCUSDT"},
		WithBybitURL(url),
		WithBybitReconnectDelay(time.Millisecond),
		WithBybitSubscriptionTimeout(time.Second),
		WithBybitHeartbeatInterval(0),
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

	want := []string{"a", "b"}
	if !reflect.DeepEqual(uids, want) {
		t.Fatalf("trade uids = %#v, want %#v", uids, want)
	}
	if got := connections.Load(); got < 2 {
		t.Fatalf("connections = %d, want at least 2", got)
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
