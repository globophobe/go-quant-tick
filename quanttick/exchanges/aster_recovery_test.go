package exchanges

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func asterRawTrades(ids ...int) string {
	rows := make([]string, 0, len(ids))
	for _, id := range ids {
		quantity := "1"
		if id == 104 {
			quantity = "2"
		}
		rows = append(rows, fmt.Sprintf(`{"id":%d,"price":"100","qty":%q,"time":1791870963099,"isBuyerMaker":%t}`, id, quantity, id == 104))
	}
	return "[" + strings.Join(rows, ",") + "]"
}

func TestAsterReconnectCrossesSparsePagesBeforeWebSocketReplay(t *testing.T) {
	pages := map[string]string{
		"":     asterRawTrades(2105),
		"103":  asterRawTrades(104, 106),
		"107":  asterRawTrades(),
		"1107": asterRawTrades(2104, 2105, 2106),
	}
	var requests, connections atomic.Int32
	recoveryStarted, streamSent := make(chan struct{}), make(chan struct{})
	var started sync.Once
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		started.Do(func() { close(recoveryStarted) })
		select {
		case <-streamSent:
		case <-r.Context().Done():
			return
		}
		fromID := r.URL.Query().Get("fromId")
		page, ok := pages[fromID]
		if !ok || r.URL.Path != "/historicalTrades" || r.URL.Query().Get("symbol") != "BTCUSDT" {
			t.Errorf("invalid recovery request: %s", r.URL)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		wantLimit := "1000"
		if fromID == "" {
			wantLimit = "1"
		}
		if r.URL.Query().Get("limit") != wantLimit {
			t.Errorf("limit = %s, want %s", r.URL.Query().Get("limit"), wantLimit)
		}
		fmt.Fprint(w, page)
	}))
	defer rest.Close()
	closeFirst := make(chan struct{})
	endpoint := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		send := func(id int) error {
			return writeExchangeWebSocketMessage(ctx, conn, fmt.Sprintf(`{"e":"trade","s":"BTCUSDT","t":%d,"T":1791870963099,"p":"100","q":"1","m":false}`, id))
		}
		if connections.Add(1) == 1 {
			if err := writeExchangeWebSocketMessage(ctx, conn, `{"result":null,"id":1}`); err != nil {
				return err
			}
			if err := send(102); err != nil {
				return err
			}
			select {
			case <-closeFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
			return conn.Close(websocket.StatusNormalClosure, "reconnect")
		}
		// An overlapping trade arrives before acknowledgement; another arrives
		// while REST recovery is still in flight.
		if err := send(2105); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, `{"result":null,"id":1}`); err != nil {
			return err
		}
		select {
		case <-recoveryStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := send(2106); err != nil {
			return err
		}
		close(streamSent)
		_, _, _ = conn.Read(ctx)
		return nil
	})
	exchange := NewAster([]string{"BTCUSDT"}, WithBinanceURL(endpoint), WithBinanceRESTURL(rest.URL), WithBinanceReconnectDelay(0))
	exchange.recoveryThrottle = newRESTThrottle(0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	trades, errs := exchange.Trades(ctx)
	var got []quanttick.TradeEvent
	for len(got) < 6 {
		select {
		case trade := <-trades:
			got = append(got, trade)
			if trade.Exchange != AsterName || trade.Ticks != 1 {
				t.Fatalf("trade = %+v", trade)
			}
			if len(got) == 1 {
				close(closeFirst)
			}
		case err := <-errs:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatalf("only received %v", tradeUIDs(got))
		}
	}
	cancel()
	for trade := range trades {
		got = append(got, trade)
	}
	for err := range errs {
		t.Errorf("recovery error: %v", err)
	}
	assertStrings(t, tradeUIDs(got), []string{"102", "104", "106", "2104", "2105", "2106"})
	assertBools(t, tradeSequential(got), []bool{false, false, false, false, true, true})
	assertInts(t, tradeTickRules(got), []int{1, -1, 1, 1, 1, 1})
	assertDecimals(t, tradeVolumes(got), []string{"100", "200", "100", "100", "100", "100"})
	if requests.Load() != int32(len(pages)) {
		t.Fatalf("requests = %d, want %d", requests.Load(), len(pages))
	}
}

func TestAsterRecoveryRejectsInvalidHistory(t *testing.T) {
	for _, test := range []struct {
		name    string
		latest  string
		page    string
		wantIDs []string
	}{
		{"duplicate", asterRawTrades(106), asterRawTrades(104, 104), []string{"104"}},
		{"backwards", asterRawTrades(106), asterRawTrades(104, 103), []string{"104"}},
		{"before cursor", asterRawTrades(106), asterRawTrades(102), []string{}},
		{"past missing target", asterRawTrades(106), asterRawTrades(104, 107), []string{"104"}},
		{"target disappears", asterRawTrades(106), asterRawTrades(104), []string{"104"}},
		{"no latest trade", asterRawTrades(), asterRawTrades(), []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Query().Get("fromId") {
				case "":
					fmt.Fprint(w, test.latest)
				case "103":
					fmt.Fprint(w, test.page)
				default:
					fmt.Fprint(w, "[]")
				}
			}))
			defer server.Close()
			exchange := NewAster([]string{"BTCUSDT"}, WithBinanceRESTURL(server.URL))
			exchange.recoveryThrottle = newRESTThrottle(0)
			exchange.lastIDs["BTCUSDT"] = 102
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			recovered, err := exchange.recoverTrades(ctx)
			if err == nil {
				t.Fatal("invalid history should fail recovery")
			}
			events := make([]quanttick.TradeEvent, 0, len(recovered))
			for _, row := range recovered {
				events = append(events, row.event)
			}
			assertStrings(t, tradeUIDs(events), test.wantIDs)
			if exchange.lastIDs["BTCUSDT"] != 102 {
				t.Fatalf("recovery advanced the emitted cursor to %d", exchange.lastIDs["BTCUSDT"])
			}
		})
	}
}
