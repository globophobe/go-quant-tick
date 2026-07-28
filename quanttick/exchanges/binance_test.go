package exchanges

import (
	"context"
	"encoding/json"
	"errors"
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

func TestBinanceSubscriptionMessages(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT", "ETHUSDT"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"method": "SUBSCRIBE",
			"params": []string{"btcusdt@trade", "ethusdt@trade"},
			"id":     1,
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
}

func TestBinanceFuturesSubscriptionMessages(t *testing.T) {
	exchange := NewBinanceFutures([]string{"BTCUSDT", "ETHUSDT"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"method": "SUBSCRIBE",
			"params": []string{"btcusdt@aggTrade", "ethusdt@aggTrade"},
			"id":     1,
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
	if exchange.Name() != BinanceFuturesName {
		t.Fatalf("name = %s, want %s", exchange.Name(), BinanceFuturesName)
	}
	if exchange.URL != "wss://fstream.binance.com/market/stream" {
		t.Fatalf("url = %s, want current Binance Futures combined endpoint", exchange.URL)
	}
}

func TestBinanceParseTradeMessages(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"e":"trade","s":"BTCUSDT","t":100,"T":1775606400000,"p":"100","q":"1","m":false}`,
		`{"e":"trade","s":"BTCUSDT","t":101,"T":1775606401000,"p":"101","q":"2","m":true}`,
		`{"e":"trade","s":"BTCUSDT","t":103,"T":1775606402000,"p":"102","q":"3","m":false}`,
	}

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

	assertStrings(t, tradeUIDs(trades), []string{"100", "101", "103"})
	assertInts(t, tradeTicks(trades), []int{1, 1, 1})
	assertBools(t, tradeSequential(trades), []bool{false, true, false})
	assertStrings(t, tradeExchanges(trades), []string{BinanceName, BinanceName, BinanceName})
	assertStrings(t, tradeSymbols(trades), []string{"BTCUSDT", "BTCUSDT", "BTCUSDT"})
	assertInts(t, tradeTickRules(trades), []int{1, -1, 1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})

	wantTimes := []time.Time{
		time.Unix(1775606400, 0).UTC(),
		time.Unix(1775606401, 0).UTC(),
		time.Unix(1775606402, 0).UTC(),
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

func TestBinanceFuturesParseAggregateTradeMessages(t *testing.T) {
	exchange := NewBinanceFutures([]string{"BTCUSDT"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"e":"aggTrade","s":"BTCUSDT","a":200,"f":300,"l":302,"T":1775606400000,"p":"100","q":"1","m":false}`,
		`{"e":"aggTrade","s":"BTCUSDT","a":201,"f":303,"l":303,"T":1775606401000,"p":"101","q":"2","m":true}`,
		`{"e":"aggTrade","s":"BTCUSDT","a":203,"f":305,"l":307,"T":1775606402000,"p":"102","q":"3","m":false}`,
	}

	trades := make([]quanttick.TradeEvent, 0, len(messages))
	for _, message := range messages {
		trade, ok, err := exchange.ParseTradeMessage([]byte(message), receivedAt)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected futures aggregate trade message to parse: %s", message)
		}
		trades = append(trades, trade)
	}

	assertStrings(t, tradeUIDs(trades), []string{"200", "201", "203"})
	assertInts(t, tradeTicks(trades), []int{3, 1, 3})
	assertBools(t, tradeSequential(trades), []bool{false, true, false})
	assertStrings(t, tradeExchanges(trades), []string{BinanceFuturesName, BinanceFuturesName, BinanceFuturesName})
	assertStrings(t, tradeSymbols(trades), []string{"BTCUSDT", "BTCUSDT", "BTCUSDT"})
	assertInts(t, tradeTickRules(trades), []int{1, -1, 1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})
}

func TestBinanceFuturesParseCombinedAggregateTradeMessage(t *testing.T) {
	exchange := NewBinanceFutures([]string{"BTCUSDT"})
	receivedAt := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)

	trade, ok, err := exchange.ParseTradeMessage(
		[]byte(`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":5933014,"p":"100","q":"2","f":100,"l":105,"T":1784937600000,"m":true,"st":1}}`),
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected combined aggregate trade message")
	}
	if trade.UID != "5933014" || trade.Exchange != BinanceFuturesName || trade.Symbol != "BTCUSDT" {
		t.Fatalf("trade identity = %#v", trade)
	}
	if trade.TickRule != -1 || trade.Ticks != 6 || !trade.Notional.Equal(quanttick.MustDecimal("2")) {
		t.Fatalf("trade normalization = %#v", trade)
	}
}

func TestBinanceFuturesRejectsMismatchedCombinedStream(t *testing.T) {
	exchange := NewBinanceFutures([]string{"BTCUSDT"})

	_, _, err := exchange.ParseTradeMessage(
		[]byte(`{"stream":"ethusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"100","q":"1","T":1784937600000,"m":false}}`),
		time.Now().UTC(),
	)
	if err == nil || !strings.Contains(err.Error(), "does not match trade") {
		t.Fatalf("stream mismatch error = %v", err)
	}
}

func TestBinanceParseLiveTradeShape(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		message  string
		uid      string
		tickRule int
	}{
		{
			message:  `{"e":"trade","E":1777807004059,"s":"BTCUSDT","t":6268487624,"p":"78530.40000000","q":"0.00039000","T":1777807004059,"m":true,"M":true}`,
			uid:      "6268487624",
			tickRule: -1,
		},
		{
			message:  `{"e":"trade","E":1778656560209,"s":"BTCUSDT","t":6291886372,"p":"80153.50000000","q":"0.00024000","T":1778656560209,"m":false,"M":true}`,
			uid:      "6291886372",
			tickRule: 1,
		},
	}

	for _, tc := range cases {
		trade, ok, err := exchange.ParseTradeMessage([]byte(tc.message), receivedAt)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected live trade shape to parse: %s", tc.message)
		}
		if trade.UID != tc.uid {
			t.Fatalf("uid = %s, want %s", trade.UID, tc.uid)
		}
		if trade.TickRule != tc.tickRule {
			t.Fatalf("tick rule = %d, want %d", trade.TickRule, tc.tickRule)
		}
	}
}

func TestBinanceParseIgnoresNonTradeMessages(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})

	messages := []string{
		`{"result":null,"id":1}`,
		`{"e":1,"E":1775606400000}`,
		`{"e":"aggTrade","s":"BTCUSDT"}`,
	}
	for _, message := range messages {
		trade, ok, err := exchange.ParseTradeMessage([]byte(message), time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("expected non-trade message to be ignored, got %#v", trade)
		}
	}
}

func TestBinanceFuturesIgnoresRawTradeMessages(t *testing.T) {
	exchange := NewBinanceFutures([]string{"BTCUSDT"})

	trade, ok, err := exchange.ParseTradeMessage(
		[]byte(`{"e":"trade","s":"BTCUSDT","t":100,"T":1775606400000,"p":"100","q":"1","m":false}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected raw trade message to be ignored, got %#v", trade)
	}
}

func TestBinanceTradeEventJSONShape(t *testing.T) {
	exchange := NewBinance([]string{"BTCUSDT"})
	trade, ok, err := exchange.ParseTradeMessage(
		[]byte(`{"e":"trade","s":"BTCUSDT","t":100,"T":1775606400000,"p":"100","q":"1","m":false}`),
		time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected trade message")
	}

	data, err := json.Marshal(trade)
	if err != nil {
		t.Fatal(err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}

	if _, ok := payload["receivedAt"]; ok {
		t.Fatalf("receivedAt should not be exported in payload: %s", string(data))
	}
	for _, key := range []string{"price", "volume", "notional"} {
		if _, ok := payload[key].(string); !ok {
			t.Fatalf("%s should be encoded as JSON string in %s", key, string(data))
		}
	}
}

func tradeUIDs(trades []quanttick.TradeEvent) []string {
	values := make([]string, len(trades))
	for i, trade := range trades {
		values[i] = trade.UID
	}
	return values
}

func tradeExchanges(trades []quanttick.TradeEvent) []string {
	values := make([]string, len(trades))
	for i, trade := range trades {
		values[i] = trade.Exchange
	}
	return values
}

func tradeSymbols(trades []quanttick.TradeEvent) []string {
	values := make([]string, len(trades))
	for i, trade := range trades {
		values[i] = trade.Symbol
	}
	return values
}

func tradeTicks(trades []quanttick.TradeEvent) []int {
	values := make([]int, len(trades))
	for i, trade := range trades {
		values[i] = trade.Ticks
	}
	return values
}

func tradeTickRules(trades []quanttick.TradeEvent) []int {
	values := make([]int, len(trades))
	for i, trade := range trades {
		values[i] = trade.TickRule
	}
	return values
}

func tradeNanoseconds(trades []quanttick.TradeEvent) []int {
	values := make([]int, len(trades))
	for i, trade := range trades {
		values[i] = trade.Nanoseconds
	}
	return values
}

func tradeSequential(trades []quanttick.TradeEvent) []bool {
	values := make([]bool, len(trades))
	for i, trade := range trades {
		values[i] = trade.IsSequential
	}
	return values
}

func tradePrices(trades []quanttick.TradeEvent) []quanttick.Decimal {
	values := make([]quanttick.Decimal, len(trades))
	for i, trade := range trades {
		values[i] = trade.Price
	}
	return values
}

func tradeNotionals(trades []quanttick.TradeEvent) []quanttick.Decimal {
	values := make([]quanttick.Decimal, len(trades))
	for i, trade := range trades {
		values[i] = trade.Notional
	}
	return values
}

func tradeVolumes(trades []quanttick.TradeEvent) []quanttick.Decimal {
	values := make([]quanttick.Decimal, len(trades))
	for i, trade := range trades {
		values[i] = trade.Volume
	}
	return values
}

func assertStrings(t *testing.T, got []string, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("strings = %#v, want %#v", got, want)
	}
}

func assertInts(t *testing.T, got []int, want []int) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ints = %#v, want %#v", got, want)
	}
}

func assertBools(t *testing.T, got []bool, want []bool) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bools = %#v, want %#v", got, want)
	}
}

func assertDecimals(t *testing.T, got []quanttick.Decimal, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decimal length = %d, want %d", len(got), len(want))
	}
	for i := range got {
		expected := quanttick.MustDecimal(want[i])
		if !got[i].Equal(expected) {
			t.Fatalf("decimal[%d] = %s, want %s", i, got[i], expected)
		}
	}
}

func TestBinanceTradesReconnectsAfterSubscriptionErrorAndBuffersUntilAck(t *testing.T) {
	var connections atomic.Int32
	preAckTradeSent := make(chan struct{})
	sendAck := make(chan struct{})

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}

		if connections.Add(1) == 1 {
			return writeExchangeWebSocketMessage(
				ctx,
				conn,
				`{"code":2,"msg":"Invalid request: bad symbol","id":1}`,
			)
		}

		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"e":"trade","s":"BTCUSDT","t":100,"T":1775606400000,"p":"100","q":"1","m":false}`,
		); err != nil {
			return err
		}
		close(preAckTradeSent)
		select {
		case <-sendAck:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, `{"result":null,"id":1}`); err != nil {
			return err
		}
		_, _ = readExchangeWebSocketMessage(ctx, conn)
		return nil
	})

	exchange := NewBinance(
		[]string{"BTCUSDT"},
		WithBinanceURL(url),
		WithBinanceReconnectDelay(time.Millisecond),
		WithBinanceSubscriptionTimeout(time.Second),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, errs := exchange.Trades(ctx)

	select {
	case err := <-errs:
		if err == nil || !strings.Contains(err.Error(), "binance websocket error 2") {
			t.Fatalf("subscription error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for subscription error")
	}

	select {
	case <-preAckTradeSent:
	case <-ctx.Done():
		t.Fatal("timed out waiting for pre-ack trade")
	}
	select {
	case trade := <-trades:
		t.Fatalf("trade emitted before subscription acknowledgement: %#v", trade)
	case <-time.After(20 * time.Millisecond):
	}
	close(sendAck)

	select {
	case trade := <-trades:
		if trade.UID != "100" {
			t.Fatalf("trade uid = %s, want 100", trade.UID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for buffered trade")
	}

	cancel()
	select {
	case _, ok := <-trades:
		if ok {
			t.Fatal("unexpected trade after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("trade stream did not stop after cancellation")
	}
	if got := connections.Load(); got < 2 {
		t.Fatalf("connections = %d, want at least 2", got)
	}
}

func TestBinanceFuturesRecoversReconnectGapBeforeWebSocketReplay(t *testing.T) {
	recoveryStarted := make(chan struct{})
	replaySent := make(chan struct{})
	var recoveryRequests atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recoveryRequests.Add(1)
		close(recoveryStarted)
		select {
		case <-replaySent:
		case <-r.Context().Done():
			return
		}
		if r.URL.Path != "/aggTrades" {
			t.Errorf("recovery path = %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("symbol"); got != "BTCUSDT" {
			t.Errorf("recovery symbol = %s", got)
		}
		if got := r.URL.Query().Get("fromId"); got != "101" {
			t.Errorf("recovery fromId = %s", got)
		}
		_, _ = fmt.Fprint(w, `[
			{"a":101,"p":"101","q":"1","f":1001,"l":1001,"T":1775606401000,"m":false},
			{"a":102,"p":"102","q":"2","f":1002,"l":1003,"T":1775606402000,"m":true}
		]`)
	}))
	t.Cleanup(restServer.Close)

	var connections atomic.Int32
	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, `{"result":null,"id":1}`); err != nil {
			return err
		}

		switch connections.Add(1) {
		case 1:
			if err := writeExchangeWebSocketMessage(
				ctx,
				conn,
				`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":100,"p":"100","q":"1","f":1000,"l":1000,"T":1775606400000,"m":false}}`,
			); err != nil {
				return err
			}
			return conn.Close(websocket.StatusNormalClosure, "")
		case 2:
			select {
			case <-recoveryStarted:
			case <-ctx.Done():
				return ctx.Err()
			}
			for _, message := range []string{
				`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":102,"p":"102","q":"2","f":1002,"l":1003,"T":1775606402000,"m":true}}`,
				`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":103,"p":"103","q":"1","f":1004,"l":1004,"T":1775606403000,"m":false}}`,
			} {
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
			}
			close(replaySent)
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		default:
			return fmt.Errorf("unexpected connection %d", connections.Load())
		}
	})

	exchange := NewBinanceFutures(
		[]string{"BTCUSDT"},
		WithBinanceURL(url),
		WithBinanceFuturesRESTURL(restServer.URL),
		WithBinanceReconnectDelay(time.Millisecond),
		WithBinanceSubscriptionTimeout(time.Second),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, errs := exchange.Trades(ctx)

	var recovered []quanttick.TradeEvent
	for len(recovered) < 4 {
		select {
		case trade := <-trades:
			recovered = append(recovered, trade)
		case err := <-errs:
			t.Fatalf("recovery error = %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for recovered Binance Futures trades")
		}
	}
	cancel()

	assertStrings(t, tradeUIDs(recovered), []string{"100", "101", "102", "103"})
	assertInts(t, tradeTicks(recovered), []int{1, 1, 2, 1})
	assertBools(t, tradeSequential(recovered), []bool{false, true, true, true})
	if got := recoveryRequests.Load(); got != 1 {
		t.Fatalf("recovery requests = %d, want 1", got)
	}
}

func TestBinanceFuturesRecoveryFailureDoesNotBlockWebSocket(t *testing.T) {
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(restServer.Close)

	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		for _, message := range []string{
			`{"result":null,"id":1}`,
			`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":101,"p":"101","q":"1","f":1001,"l":1001,"T":1775606401000,"m":false}}`,
		} {
			if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
				return err
			}
		}
		_, _ = readExchangeWebSocketMessage(ctx, conn)
		return nil
	})

	exchange := NewBinanceFutures(
		[]string{"BTCUSDT"},
		WithBinanceURL(url),
		WithBinanceFuturesRESTURL(restServer.URL),
		WithBinanceSubscriptionTimeout(time.Second),
	)
	exchange.lastIDs["BTCUSDT"] = 100

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 1)
	errs := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- exchange.run(ctx, trades, errs)
	}()

	select {
	case err := <-errs:
		if err == nil || !strings.Contains(err.Error(), "HTTP 503") {
			t.Fatalf("recovery error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for recovery error")
	}
	select {
	case trade := <-trades:
		if trade.UID != "101" || !trade.IsSequential {
			t.Fatalf("websocket trade = %#v", trade)
		}
	case <-ctx.Done():
		t.Fatal("REST failure blocked websocket continuation")
	}
	cancel()
	<-done
}

func TestBinanceFuturesRecoveryRetriesRateLimitResponse(t *testing.T) {
	var requests atomic.Int32
	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0.001")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		_, _ = fmt.Fprint(w, `[
			{"a":101,"p":"101","q":"1","f":1001,"l":1001,"T":1775606401000,"m":false}
		]`)
	}))
	t.Cleanup(restServer.Close)

	exchange := NewBinanceFutures(
		[]string{"BTCUSDT"},
		WithBinanceFuturesRESTURL(restServer.URL),
	)
	recovered, err := exchange.recoverFuturesSymbol(
		context.Background(),
		"BTCUSDT",
		101,
		map[string]int64{"BTCUSDT": 100},
	)
	if err != nil {
		t.Fatalf("recover futures symbol: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
	if len(recovered) != 1 || recovered[0].tradeID != 101 {
		t.Fatalf("recovered = %#v", recovered)
	}
}

func TestBinanceResponseDelayUsesWeightWindow(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 34, 45, 0, time.UTC)
	headers := make(http.Header)
	headers.Set("X-MBX-USED-WEIGHT-1M", "1200")
	delay, err := binanceResponseDelay(headers, now, 1200)
	if err != nil {
		t.Fatalf("response delay: %v", err)
	}
	if delay != 15*time.Second {
		t.Fatalf("response delay = %s, want 15s", delay)
	}
}

func TestBinanceDiscardedPreAckTradeDoesNotAdvanceSequence(t *testing.T) {
	var connections atomic.Int32
	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}

		switch connections.Add(1) {
		case 1:
			for _, message := range []string{
				`{"e":"trade","s":"BTCUSDT","t":100,"T":1775606400000,"p":"100","q":"1","m":false}`,
				`{"code":2,"msg":"Invalid request after trade","id":1}`,
			} {
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
			}
			return nil
		case 2:
			for _, message := range []string{
				`{"result":null,"id":1}`,
				`{"e":"trade","s":"BTCUSDT","t":101,"T":1775606401000,"p":"101","q":"1","m":false}`,
			} {
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
			}
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		default:
			return fmt.Errorf("unexpected connection")
		}
	})

	exchange := NewBinance(
		[]string{"BTCUSDT"},
		WithBinanceURL(url),
		WithBinanceReconnectDelay(time.Millisecond),
		WithBinanceSubscriptionTimeout(time.Second),
	)
	exchange.lastIDs["BTCUSDT"] = 99

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, errs := exchange.Trades(ctx)

	select {
	case err := <-errs:
		if err == nil || !strings.Contains(err.Error(), "binance websocket error 2") {
			t.Fatalf("subscription error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for subscription error")
	}

	select {
	case trade := <-trades:
		if trade.UID != "101" {
			t.Fatalf("trade uid = %s, want 101", trade.UID)
		}
		if trade.IsSequential {
			t.Fatal("trade 101 must expose discarded pre-ack trade 100 as a sequence gap")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnected trade")
	}

	cancel()
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want 2", got)
	}
}

func TestBinanceServerShutdownReconnectsImmediately(t *testing.T) {
	var connections atomic.Int32
	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		connection := connections.Add(1)
		if err := writeExchangeWebSocketMessage(ctx, conn, `{"result":null,"id":1}`); err != nil {
			return err
		}
		if connection == 1 {
			if err := writeExchangeWebSocketMessage(ctx, conn, `{"e":"serverShutdown","E":1775606400000}`); err != nil {
				return err
			}
			_, _ = readExchangeWebSocketMessage(ctx, conn)
			return nil
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"e":"trade","s":"BTCUSDT","t":200,"T":1775606401000,"p":"101","q":"1","m":false}`,
		); err != nil {
			return err
		}
		_, _ = readExchangeWebSocketMessage(ctx, conn)
		return nil
	})

	exchange := NewBinance(
		[]string{"BTCUSDT"},
		WithBinanceURL(url),
		WithBinanceReconnectDelay(5*time.Second),
		WithBinanceSubscriptionTimeout(time.Second),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, errs := exchange.Trades(ctx)

	select {
	case trade := <-trades:
		if trade.UID != "200" {
			t.Fatalf("trade uid = %s, want 200", trade.UID)
		}
	case <-ctx.Done():
		t.Fatal("server shutdown did not trigger an immediate replacement connection")
	}
	select {
	case err := <-errs:
		t.Fatalf("routine server shutdown reached the error channel: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want 2", got)
	}
}

func TestParseBinanceSubscriptionResponseDetectsServerShutdown(t *testing.T) {
	_, err := parseBinanceSubscriptionResponse([]byte(`{"e":"serverShutdown","E":1775606400000}`))
	if !errors.Is(err, errBinanceServerShutdown) {
		t.Fatalf("server shutdown error = %v", err)
	}
}

func TestParseBinanceSubscriptionResponseRejectsProtocolFailures(t *testing.T) {
	tests := []string{
		`{"code":2,"msg":"Invalid request","id":1}`,
		`{"result":null,"id":2}`,
		`{"result":[],"id":1}`,
	}
	for _, message := range tests {
		if _, err := parseBinanceSubscriptionResponse([]byte(message)); err == nil {
			t.Fatalf("expected subscription failure for %s", message)
		}
	}
}

func TestBinanceSpotRecoversPublicRawTrades(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if got := request.URL.Path; got != "/historicalTrades" {
			t.Fatalf("path = %q, want /historicalTrades", got)
		}
		if got := request.URL.Query().Get("symbol"); got != "BTCUSDT" {
			t.Fatalf("symbol = %q, want BTCUSDT", got)
		}
		if got := request.URL.Query().Get("fromId"); got != "101" {
			t.Fatalf("fromId = %q, want 101", got)
		}
		if got := request.Header.Get("X-MBX-APIKEY"); got != "" {
			t.Fatalf("unexpected API key header %q", got)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`[
			{"id":101,"price":"100","qty":"2","time":1775606401000,"isBuyerMaker":false},
			{"id":102,"price":"101","qty":"3","time":1775606402000,"isBuyerMaker":true}
		]`))
	}))
	defer server.Close()

	exchange := NewBinance(
		[]string{"BTCUSDT"},
		WithBinanceRESTURL(server.URL),
	)
	recovered, err := exchange.recoverSpotSymbol(
		context.Background(),
		"BTCUSDT",
		101,
		map[string]int64{"BTCUSDT": 100},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 2 {
		t.Fatalf("recovered trades = %d, want 2", len(recovered))
	}
	if recovered[0].tradeID != 101 || recovered[1].tradeID != 102 {
		t.Fatalf("recovered IDs = %d,%d, want 101,102", recovered[0].tradeID, recovered[1].tradeID)
	}
	if recovered[0].event.Ticks != 1 || recovered[1].event.Ticks != 1 {
		t.Fatalf("raw spot ticks = %d,%d, want 1,1", recovered[0].event.Ticks, recovered[1].event.Ticks)
	}
	assertInts(t, tradeTickRules([]quanttick.TradeEvent{recovered[0].event, recovered[1].event}), []int{1, -1})
}

func TestBinanceSpotRecoverySendsConfiguredAPIKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("X-MBX-APIKEY"); got != "test-binance-key" {
			t.Fatalf("API key header = %q, want test-binance-key", got)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`[]`))
	}))
	defer server.Close()

	exchange := NewBinance(
		[]string{"BTCUSDT"},
		WithBinanceRESTURL(server.URL),
		WithBinanceAPIKey("  test-binance-key  "),
	)
	recovered, err := exchange.recoverSpotSymbol(
		context.Background(),
		"BTCUSDT",
		101,
		map[string]int64{"BTCUSDT": 100},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 0 {
		t.Fatalf("recovered trades = %d, want 0", len(recovered))
	}
}

func TestBinanceSpotRecoveryRejectsMissingRawTradeID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`[
			{"id":102,"price":"100","qty":"1","time":1775606402000,"isBuyerMaker":false}
		]`))
	}))
	defer server.Close()

	exchange := NewBinance(
		[]string{"BTCUSDT"},
		WithBinanceRESTURL(server.URL),
	)
	if _, err := exchange.recoverSpotSymbol(
		context.Background(),
		"BTCUSDT",
		101,
		map[string]int64{"BTCUSDT": 100},
	); err == nil {
		t.Fatal("missing raw trade ID should fail recovery")
	}
}
