package exchanges

import (
	"context"
	"encoding/json"
	"errors"
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
			"params": []string{"btcusdt@trade", "ethusdt@trade"},
			"id":     1,
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
	if exchange.Name() != BinanceFuturesName {
		t.Fatalf("name = %s, want %s", exchange.Name(), BinanceFuturesName)
	}
	if exchange.URL != BinanceFuturesURL {
		t.Fatalf("url = %s, want %s", exchange.URL, BinanceFuturesURL)
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

func TestBinanceFuturesParseRawTradeMessages(t *testing.T) {
	exchange := NewBinanceFutures([]string{"BTCUSDT"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"e":"trade","s":"BTCUSDT","t":200,"T":1775606400000,"p":"100","q":"1","m":false}`,
		`{"e":"trade","s":"BTCUSDT","t":201,"T":1775606401000,"p":"101","q":"2","m":true}`,
		`{"e":"trade","s":"BTCUSDT","t":203,"T":1775606402000,"p":"102","q":"3","m":false}`,
	}

	trades := make([]quanttick.TradeEvent, 0, len(messages))
	for _, message := range messages {
		trade, ok, err := exchange.ParseTradeMessage([]byte(message), receivedAt)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("expected futures raw trade message to parse: %s", message)
		}
		trades = append(trades, trade)
	}

	assertStrings(t, tradeUIDs(trades), []string{"200", "201", "203"})
	assertInts(t, tradeTicks(trades), []int{1, 1, 1})
	assertBools(t, tradeSequential(trades), []bool{false, true, false})
	assertStrings(t, tradeExchanges(trades), []string{BinanceFuturesName, BinanceFuturesName, BinanceFuturesName})
	assertStrings(t, tradeSymbols(trades), []string{"BTCUSDT", "BTCUSDT", "BTCUSDT"})
	assertInts(t, tradeTickRules(trades), []int{1, -1, 1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})
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

func TestBinanceFuturesIgnoresAggregateTradeMessages(t *testing.T) {
	exchange := NewBinanceFutures([]string{"BTCUSDT"})

	trade, ok, err := exchange.ParseTradeMessage(
		[]byte(`{"e":"aggTrade","s":"BTCUSDT","a":100,"f":100,"l":101,"T":1775606400000,"p":"100","q":"1","m":false}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected aggregate trade message to be ignored, got %#v", trade)
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
	case err := <-errs:
		if !errors.Is(err, errBinanceServerShutdown) {
			t.Fatalf("server shutdown error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for server shutdown error")
	}
	select {
	case trade := <-trades:
		if trade.UID != "200" {
			t.Fatalf("trade uid = %s, want 200", trade.UID)
		}
	case <-ctx.Done():
		t.Fatal("server shutdown did not trigger an immediate replacement connection")
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
