package exchanges

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestDeribitSubscriptionMessages(t *testing.T) {
	exchange := NewDeribit([]string{"BTC-PERPETUAL", "BTC_USDC"})
	want := []map[string]any{
		{
			"jsonrpc": "2.0",
			"id":      deribitSubscriptionRequestID,
			"method":  "public/subscribe",
			"params": map[string]any{
				"channels": []string{
					"trades.BTC-PERPETUAL.100ms",
					"trades.BTC_USDC.100ms",
				},
			},
		},
	}
	if got := exchange.SubscriptionMessages(); !reflect.DeepEqual(got, want) {
		t.Fatalf("subscription messages = %#v, want %#v", got, want)
	}
}

func TestDeribitParsesInverseSpotAndOptionTrades(t *testing.T) {
	receivedAt := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	inverse := NewDeribit([]string{"BTC-PERPETUAL"})
	trades, err := inverse.ParseTradeMessage(
		[]byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.100ms","data":[
			{"trade_seq":100,"timestamp":1785110400000,"price":50000,"amount":100,"direction":"buy","instrument_name":"BTC-PERPETUAL"},
			{"trade_seq":101,"timestamp":1785110400001,"price":"50000","amount":"200","direction":"sell","instrument_name":"BTC-PERPETUAL"}
		]}}`),
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertStrings(t, tradeUIDs(trades), []string{"100", "101"})
	assertBools(t, tradeSequential(trades), []bool{false, true})
	assertInts(t, tradeTickRules(trades), []int{1, -1})
	assertDecimals(t, tradeVolumes(trades), []string{"100", "200"})
	assertDecimals(t, tradeNotionals(trades), []string{"0.002", "0.004"})

	spot := NewDeribit([]string{"BTC_USDC"})
	trades, err = spot.ParseTradeMessage(
		[]byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC_USDC.100ms","data":[
			{"trade_seq":200,"timestamp":1785110400000,"price":50000,"amount":0.5,"direction":"buy","instrument_name":"BTC_USDC"}
		]}}`),
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDecimals(t, tradeNotionals(trades), []string{"0.5"})
	assertDecimals(t, tradeVolumes(trades), []string{"25000"})

	option := NewDeribit([]string{"BTC-24APR26-72000-C"})
	trades, err = option.ParseTradeMessage(
		[]byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-24APR26-72000-C.100ms","data":[
			{"trade_seq":300,"timestamp":1785110400000,"starbase_timestamp":1785110400123456789,"price":0.0525,"amount":3,"direction":"buy","instrument_name":"BTC-24APR26-72000-C"}
		]}}`),
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDecimals(t, tradeNotionals(trades), []string{"3"})
	assertDecimals(t, tradeVolumes(trades), []string{"0.1575"})
	wantTimestamp := time.Unix(0, 1785110400123456789).UTC().Truncate(time.Microsecond)
	if !trades[0].Timestamp.Equal(wantTimestamp) {
		t.Fatalf("option timestamp = %s, want %s", trades[0].Timestamp, wantTimestamp)
	}
	if trades[0].Nanoseconds != 789 {
		t.Fatalf("option residual nanoseconds = %d, want 789", trades[0].Nanoseconds)
	}
}

func TestDeribitRejectsUnexpectedChannelsAndSequenceGapsInRecovery(t *testing.T) {
	exchange := NewDeribit([]string{"BTC-PERPETUAL"})
	if _, err := exchange.ParseTradeMessage(
		[]byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.ETH-PERPETUAL.100ms","data":[]}}`),
		time.Now().UTC(),
	); err == nil {
		t.Fatal("unexpected trade channel should fail")
	}

	rest := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"jsonrpc":"2.0","result":{"trades":[
			{"trade_seq":102,"timestamp":1785110400000,"price":50000,"amount":100,"direction":"buy","instrument_name":"BTC-PERPETUAL"}
		],"has_more":false}}`))
	}))
	defer rest.Close()
	exchange.RESTURL = rest.URL
	exchange.recoveryThrottle = newRESTThrottle(0)
	if _, err := exchange.recoverSymbol(context.Background(), "BTC-PERPETUAL", 101); err == nil {
		t.Fatal("missing Deribit trade sequence should fail recovery")
	}
}

func TestDeribitRecoveryKeepsPartialRowsOnError(t *testing.T) {
	var requests atomic.Int32
	rest := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if requests.Add(1) == 1 {
			response.Header().Set("Content-Type", "application/json")
			_, _ = response.Write([]byte(`{"jsonrpc":"2.0","result":{"trades":[
				{"trade_seq":102,"timestamp":1785110400002,"price":50000,"amount":100,"direction":"buy","instrument_name":"BTC-PERPETUAL"},
				{"trade_seq":101,"timestamp":1785110400001,"price":50000,"amount":100,"direction":"buy","instrument_name":"BTC-PERPETUAL"}
			],"has_more":true}}`))
			return
		}
		if got := request.URL.Query().Get("start_seq"); got != "103" {
			t.Fatalf("start_seq = %q, want 103", got)
		}
		http.Error(response, "recovery failed", http.StatusInternalServerError)
	}))
	defer rest.Close()

	exchange := NewDeribit([]string{"BTC-PERPETUAL"}, WithDeribitRESTURL(rest.URL))
	exchange.recoveryThrottle = newRESTThrottle(0)
	exchange.lastSequences["BTC-PERPETUAL"] = 100
	recovered, err := exchange.recoverTrades(context.Background())
	if err == nil {
		t.Fatal("partial recovery should fail")
	}
	if len(recovered) != 2 {
		t.Fatalf("recovered %d partial trades, want 2", len(recovered))
	}

	parsed, err := exchange.parseTradeMessage(
		[]byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.100ms","data":[{"trade_seq":103,"timestamp":1785110400003,"price":50000,"amount":100,"direction":"buy","instrument_name":"BTC-PERPETUAL"}]}}`),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	trades := make(chan quanttick.TradeEvent, 3)
	for _, recoveredTrade := range recovered {
		if err := exchange.emitParsedTrade(context.Background(), trades, recoveredTrade); err != nil {
			t.Fatal(err)
		}
	}
	if err := exchange.emitParsedTrade(context.Background(), trades, parsed[0]); err != nil {
		t.Fatal(err)
	}
	got := []quanttick.TradeEvent{<-trades, <-trades, <-trades}
	assertStrings(t, tradeUIDs(got), []string{"101", "102", "103"})
	assertBools(t, tradeSequential(got), []bool{true, true, true})
}

func TestDeribitTradesRecoversGapBeforeBufferedWebSocketTrades(t *testing.T) {
	var connections atomic.Int32
	wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		connection := connections.Add(1)
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			`{"jsonrpc":"2.0","id":1,"result":["trades.BTC-PERPETUAL.100ms"]}`,
		); err != nil {
			return err
		}
		sequence := 100
		if connection == 2 {
			sequence = 102
		}
		if err := writeExchangeWebSocketMessage(
			ctx,
			conn,
			fmt.Sprintf(
				`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.100ms","data":[{"trade_seq":%d,"timestamp":1785110400000,"price":50000,"amount":100,"direction":"buy","instrument_name":"BTC-PERPETUAL"}]}}`,
				sequence,
			),
		); err != nil {
			return err
		}
		if connection == 1 {
			return conn.Close(websocket.StatusGoingAway, "test reconnect")
		}
		_, _ = readExchangeWebSocketMessage(ctx, conn)
		return nil
	})

	rest := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if got := request.URL.Query().Get("start_seq"); got != "101" {
			t.Fatalf("start_seq = %q, want 101", got)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"jsonrpc":"2.0","result":{"trades":[
			{"trade_seq":101,"timestamp":1785110400000,"price":50000,"amount":100,"direction":"buy","instrument_name":"BTC-PERPETUAL"}
		],"has_more":false}}`))
	}))
	defer rest.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	exchange := NewDeribit(
		[]string{"BTC-PERPETUAL"},
		WithDeribitURL(wsURL),
		WithDeribitRESTURL(rest.URL),
		WithDeribitReconnectDelay(0),
	)
	exchange.recoveryThrottle = newRESTThrottle(0)
	trades, errs := exchange.Trades(ctx)
	var got []quanttick.TradeEvent
	for len(got) < 3 {
		select {
		case trade := <-trades:
			got = append(got, trade)
		case err := <-errs:
			t.Fatalf("unexpected collector error: %v", err)
		case <-ctx.Done():
			t.Fatalf("wait for recovered deribit trades: %v", ctx.Err())
		}
	}
	cancel()
	assertStrings(t, tradeUIDs(got), []string{"100", "101", "102"})
	assertBools(t, tradeSequential(got), []bool{false, true, true})
}
