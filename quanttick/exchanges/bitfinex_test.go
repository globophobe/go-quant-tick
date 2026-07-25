package exchanges

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestBitfinexSubscriptionMessages(t *testing.T) {
	exchange := NewBitfinex([]string{"BTCUSD", "tETHUSD", "BTCF0:USTF0"})

	got := exchange.SubscriptionMessages()
	want := []map[string]any{
		{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  "tBTCUSD",
		},
		{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  "tETHUSD",
		},
		{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  "tBTCF0:USTF0",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
	if exchange.Name() != BitfinexName {
		t.Fatalf("name = %s, want %s", exchange.Name(), BitfinexName)
	}
}

func TestBitfinexParseTradeMessages(t *testing.T) {
	exchange := NewBitfinex([]string{"tBTCUSD"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tBTCUSD"}`,
		`[1,"hb"]`,
		`[1,"te",[100,1775557139000,1,99]]`,
		`[1,"te",[102,1775557140000,1,100]]`,
		`[1,"te",[103,1775557141000,1,101]]`,
		`[1,"tu",[102,1775557141001,-2,101]]`,
		`[1,"tu",[100,1775557140001,1,100]]`,
		`[1,"tu",[103,1775557142001,3,102]]`,
	}

	trades := parseBitfinexFixture(t, exchange, messages, receivedAt)

	assertStrings(t, tradeUIDs(trades), []string{"100", "102", "103"})
	assertBools(t, tradeSequential(trades), []bool{false, true, true})
	assertStrings(t, tradeExchanges(trades), []string{BitfinexName, BitfinexName, BitfinexName})
	assertStrings(t, tradeSymbols(trades), []string{"tBTCUSD", "tBTCUSD", "tBTCUSD"})
	assertInts(t, tradeTickRules(trades), []int{1, -1, 1})
	assertDecimals(t, tradePrices(trades), []string{"100", "101", "102"})
	assertDecimals(t, tradeNotionals(trades), []string{"1", "2", "3"})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "202", "306"})

	wantTimes := []time.Time{
		time.UnixMilli(1775557140001).UTC(),
		time.UnixMilli(1775557141001).UTC(),
		time.UnixMilli(1775557142001).UTC(),
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

func TestBitfinexParseIgnoresExecuteAndPublishesUpdate(t *testing.T) {
	exchange := NewBitfinex([]string{"tBTCUSD"})
	receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	messages := []string{
		`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tBTCUSD"}`,
		`[1,"te",[100,1775557140000,1,100]]`,
		`[1,"tu",[100,1775557140001,2,101]]`,
	}

	trades := parseBitfinexFixture(t, exchange, messages, receivedAt)
	if len(trades) != 1 {
		t.Fatalf("trades = %d, want 1", len(trades))
	}
	if trades[0].UID != "100" {
		t.Fatalf("uid = %s, want 100", trades[0].UID)
	}
	if !trades[0].Timestamp.Equal(time.UnixMilli(1775557140001).UTC()) {
		t.Fatalf("timestamp = %s, want finalized tu timestamp", trades[0].Timestamp)
	}
	assertDecimals(t, tradePrices(trades), []string{"101"})
	assertDecimals(t, tradeNotionals(trades), []string{"2"})
}

func TestBitfinexQueuesUpdatesUntilExecuteOrderIsConfirmed(t *testing.T) {
	for _, symbol := range []string{"tBTCUSD", "tBTCF0:USTF0"} {
		t.Run(symbol, func(t *testing.T) {
			exchange := NewBitfinex([]string{symbol})
			receivedAt := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

			messages := []string{
				`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"` + symbol + `"}`,
				`[1,"te",[100,1775557140000,1,100]]`,
				`[1,"te",[101,1775557140001,1,101]]`,
				`[1,"tu",[101,1775557141001,1,101]]`,
			}
			trades := parseBitfinexFixture(t, exchange, messages, receivedAt)
			if len(trades) != 0 {
				t.Fatalf("expected later update to wait for queue head, got %#v", trades)
			}

			trades = parseBitfinexFixture(
				t,
				exchange,
				[]string{`[1,"tu",[100,1775557141000,1,100]]`},
				receivedAt,
			)
			assertStrings(t, tradeUIDs(trades), []string{"100", "101"})
			assertBools(t, tradeSequential(trades), []bool{false, true})
			assertStrings(t, tradeExchanges(trades), []string{BitfinexName, BitfinexName})
			assertStrings(t, tradeSymbols(trades), []string{symbol, symbol})
			wantTimes := []time.Time{
				time.UnixMilli(1775557141000).UTC(),
				time.UnixMilli(1775557141001).UTC(),
			}
			for i, want := range wantTimes {
				if !trades[i].Timestamp.Equal(want) {
					t.Fatalf("timestamp[%d] = %s, want %s", i, trades[i].Timestamp, want)
				}
			}
		})
	}
}

func TestBitfinexParseIgnoresUnknownChannelAndSnapshots(t *testing.T) {
	exchange := NewBitfinex([]string{"tBTCUSD"})

	messages := []string{
		`[1,"tu",[100,1775557140000,1,100]]`,
		`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tBTCUSD"}`,
		`[1,[[100,1775557140000,1,100]]]`,
	}

	trades := parseBitfinexFixture(t, exchange, messages, time.Now().UTC())
	if len(trades) != 0 {
		t.Fatalf("expected messages to be ignored, got %#v", trades)
	}
}

func TestBitfinexParsesStringNumericFields(t *testing.T) {
	exchange := NewBitfinex([]string{"BTCUSD"})
	receivedAt := time.Now().UTC()
	messages := []string{
		`{"event":"subscribed","channel":"trades","chanId":"1","symbol":"tBTCUSD"}`,
		`["1","te",["100","1775557140000","-1.5","100.5"]]`,
		`["1","tu",["100","1775557140000","-1.5","100.5"]]`,
	}

	trades := parseBitfinexFixture(t, exchange, messages, receivedAt)
	assertStrings(t, tradeUIDs(trades), []string{"100"})
	assertInts(t, tradeTickRules(trades), []int{-1})
	assertDecimals(t, tradePrices(trades), []string{"100.5"})
	assertDecimals(t, tradeNotionals(trades), []string{"1.5"})
	assertDecimals(t, tradeVolumes(trades), []string{"150.75"})
}

func TestBitfinexResetSessionDropsIncompletePairingState(t *testing.T) {
	exchange := NewBitfinex([]string{"tBTCUSD"})
	receivedAt := time.Now().UTC()

	trades := parseBitfinexFixture(t, exchange, []string{
		`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tBTCUSD"}`,
		`[1,"te",[100,1775557140000,1,100]]`,
	}, receivedAt)
	if len(trades) != 0 {
		t.Fatalf("incomplete trade unexpectedly emitted: %#v", trades)
	}

	exchange.resetSessionState()
	trades = parseBitfinexFixture(t, exchange, []string{
		`[1,"tu",[100,1775557141000,1,100]]`,
		`{"event":"subscribed","channel":"trades","chanId":2,"symbol":"tBTCUSD"}`,
		`[2,"te",[101,1775557142000,1,101]]`,
		`[2,"tu",[101,1775557142001,1,101]]`,
	}, receivedAt)
	assertStrings(t, tradeUIDs(trades), []string{"101"})
}

func TestBitfinexSubscriptionCompleteness(t *testing.T) {
	exchange := NewBitfinex([]string{"BTCUSD", "ETHUSD"})
	if exchange.subscriptionsReady() {
		t.Fatal("subscriptions should not be ready before acknowledgements")
	}

	if _, err := exchange.ParseTradeMessage(
		[]byte(`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tBTCUSD"}`),
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	if exchange.subscriptionsReady() {
		t.Fatal("subscriptions should wait for every requested symbol")
	}

	if _, err := exchange.ParseTradeMessage(
		[]byte(`{"event":"subscribed","channel":"trades","chanId":2,"symbol":"tETHUSD"}`),
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	if !exchange.subscriptionsReady() {
		t.Fatal("subscriptions should be ready after every acknowledgement")
	}
}

func TestBitfinexLifecycleInfoEventsAreRecognized(t *testing.T) {
	tests := []struct {
		message string
		code    int64
	}{
		{`{"event":"info","code":20051,"msg":"reconnect"}`, 20051},
		{`{"event":"info","code":20060,"msg":"maintenance started"}`, 20060},
		{`{"event":"info","code":20061,"msg":"maintenance ended"}`, 20061},
		{`{"event":"info","platform":{"status":0}}`, 20060},
	}

	for _, test := range tests {
		exchange := NewBitfinex([]string{"BTCUSD"})
		_, err := exchange.ParseTradeMessage([]byte(test.message), time.Now().UTC())
		var lifecycle *bitfinexLifecycleEvent
		if !errors.As(err, &lifecycle) {
			t.Fatalf("message %s error = %v, want lifecycle event", test.message, err)
		}
		if lifecycle.code != test.code {
			t.Fatalf("message %s lifecycle code = %d, want %d", test.message, lifecycle.code, test.code)
		}
	}
}

func TestBitfinexProtocolFailuresSurface(t *testing.T) {
	exchange := NewBitfinex([]string{"BTCUSD"})
	messages := []string{
		`{"event":"error","code":10001,"msg":"unknown pair"}`,
		`{"event":"info","code":29999,"msg":"unknown info"}`,
		`{"event":"subscribed","channel":"book","chanId":1,"symbol":"tBTCUSD"}`,
		`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tETHUSD"}`,
	}
	for _, message := range messages {
		if _, err := exchange.ParseTradeMessage([]byte(message), time.Now().UTC()); err == nil {
			t.Fatalf("expected protocol failure for %s", message)
		}
	}

	if _, err := exchange.ParseTradeMessage(
		[]byte(`{"event":"info","version":2,"platform":{"status":1}}`),
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("operative info message should be accepted: %v", err)
	}
}

func TestBitfinexLifecycleRestartReconnectsWithoutError(t *testing.T) {
	var connections atomic.Int32
	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		connection := connections.Add(1)
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return fmt.Errorf("read subscription on connection %d: %w", connection, err)
		}

		switch connection {
		case 1:
			for _, message := range []string{
				`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tBTCUSD"}`,
				`{"event":"info","code":20051,"msg":"reconnect"}`,
			} {
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
			}
			return nil
		case 2:
			for _, message := range []string{
				`{"event":"subscribed","channel":"trades","chanId":2,"symbol":"tBTCUSD"}`,
				`[2,"te",[101,1775557141000,1,101]]`,
				`[2,"tu",[101,1775557141001,1,101]]`,
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	exchange := NewBitfinex(
		[]string{"BTCUSD"},
		WithBitfinexURL(url),
		WithBitfinexReconnectDelay(5*time.Second),
	)
	trades, errs := exchange.Trades(ctx)

	select {
	case trade := <-trades:
		if trade.UID != "101" {
			t.Fatalf("trade uid = %s, want 101", trade.UID)
		}
	case err := <-errs:
		t.Fatalf("routine restart reached the error channel: %v", err)
	case <-ctx.Done():
		t.Fatal("routine restart did not trigger an immediate replacement connection")
	}
	select {
	case err := <-errs:
		t.Fatalf("routine restart reached the error channel: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want 2", got)
	}
}

func TestBitfinexTradesReconnectDropsIncompletePairingState(t *testing.T) {
	var connections atomic.Int32
	url := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		connection := connections.Add(1)
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return fmt.Errorf("read subscription on connection %d: %w", connection, err)
		}

		switch connection {
		case 1:
			for _, message := range []string{
				`{"event":"subscribed","channel":"trades","chanId":1,"symbol":"tBTCUSD"}`,
				`[1,"te",[100,1775557140000,1,100]]`,
			} {
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
			}
			return conn.Close(websocket.StatusGoingAway, "test reconnect")
		case 2:
			for _, message := range []string{
				`{"event":"subscribed","channel":"trades","chanId":2,"symbol":"tBTCUSD"}`,
				`[2,"te",[101,1775557141000,1,101]]`,
				`[2,"tu",[101,1775557141001,1,101]]`,
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
	exchange := NewBitfinex(
		[]string{"BTCUSD"},
		WithBitfinexURL(url),
		WithBitfinexReconnectDelay(0),
	)
	trades, errs := exchange.Trades(ctx)

	var uids []string
	for len(uids) < 1 {
		select {
		case trade, ok := <-trades:
			if !ok {
				t.Fatal("trade channel closed before the reconnected trade arrived")
			}
			uids = append(uids, trade.UID)
		case err, ok := <-errs:
			if !ok {
				t.Fatal("error channel closed before the reconnected trade arrived")
			}
			if err == nil {
				t.Fatal("received nil collector error")
			}
		case <-ctx.Done():
			t.Fatalf("wait for reconnected trade: %v", ctx.Err())
		}
	}
	cancel()
	for trade := range trades {
		uids = append(uids, trade.UID)
	}

	assertStrings(t, uids, []string{"101"})
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want 2", got)
	}
}

func parseBitfinexFixture(t *testing.T, exchange *Bitfinex, messages []string, receivedAt time.Time) []quanttick.TradeEvent {
	t.Helper()

	var trades []quanttick.TradeEvent
	for _, message := range messages {
		parsedTrades, err := exchange.ParseTradeMessage([]byte(message), receivedAt)
		if err != nil {
			t.Fatal(err)
		}
		trades = append(trades, parsedTrades...)
	}
	return trades
}
