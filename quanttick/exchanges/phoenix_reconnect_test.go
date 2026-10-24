package exchanges

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestPhoenixReconnectsAfterSubscriptionClose(t *testing.T) {
	for _, test := range []struct {
		name       string
		code       websocket.StatusCode
		wantErrors int
	}{
		{"service restart", websocket.StatusServiceRestart, 0},
		{"protocol failure", websocket.StatusProtocolError, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			fill := phoenixTestFill("live", "2026-09-24T00:00:01Z")
			var connections atomic.Int32
			wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				if connections.Add(1) == 1 {
					return conn.Close(test.code, test.name)
				}
				if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
					return err
				}
				if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", fill)); err != nil {
					return err
				}
				_, _, _ = conn.Read(ctx)
				return nil
			})
			p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixReconnectDelay(time.Millisecond))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			trades, errs := p.Trades(ctx)
			var reported []error
			var got quanttick.TradeEvent
			for got.UID == "" {
				select {
				case trade, ok := <-trades:
					if !ok {
						t.Fatal("trade stream closed before reconnect")
					}
					got = trade
				case err, ok := <-errs:
					if !ok {
						t.Fatal("error stream closed before reconnect")
					}
					reported = append(reported, err)
				case <-ctx.Done():
					t.Fatal("did not resume collection after reconnect")
				}
			}
			cancel()
			for extra := range trades {
				t.Errorf("unexpected extra trade: %+v", extra)
			}
			for err := range errs {
				reported = append(reported, err)
			}
			want, _ := parsePhoenixFill(fill, time.Now())
			want.UID = (&phoenixFillCounter{}).identify(want, fill)
			if got.UID != want.UID || connections.Load() != 2 {
				t.Fatalf("trade=%s connections=%d, want %s after two connections", got.UID, connections.Load(), want.UID)
			}
			if len(reported) != test.wantErrors {
				t.Fatalf("reported=%v, want %d errors", reported, test.wantErrors)
			}
			for _, err := range reported {
				if websocket.CloseStatus(err) != test.code {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestPhoenixRecoveryRetriesLaggingHistoryWithBufferedFills(t *testing.T) {
	first := phoenixTestFill("anchor", "2026-09-24T00:00:01Z")
	second := phoenixTestFill("recovered", "2026-09-24T00:00:02Z")
	third := phoenixTestFill("live", "2026-09-24T00:00:03Z")
	recoveryStarted := make(chan struct{})
	liveBuffered := make(chan struct{})
	queries := make(chan url.Values, 8)
	var requests atomic.Int32
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries <- r.URL.Query()
		switch requests.Add(1) {
		case 1:
			close(recoveryStarted)
			select {
			case <-liveBuffered:
			case <-r.Context().Done():
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{}, "hasMore": false})
		case 2:
			json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{second}, "hasMore": true, "nextCursor": "older"})
		case 3:
			json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{first}, "hasMore": false})
		default:
			http.Error(w, "unexpected recovery request", http.StatusInternalServerError)
		}
	}))
	defer rest.Close()
	wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		// Overlap arriving before the acknowledgement must survive the retry too.
		if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", second)); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
			return err
		}
		select {
		case <-recoveryStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", third, third)); err != nil {
			return err
		}
		readDone := make(chan struct{})
		go func() { _, _, _ = conn.Read(ctx); close(readDone) }()
		if err := conn.Ping(ctx); err != nil {
			return err
		}
		close(liveBuffered)
		<-readDone
		return nil
	})
	p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixRESTURL(rest.URL), WithPhoenixHTTPClient(rest.Client()))
	anchor, _ := parsePhoenixFill(first, time.Now())
	anchor.UID = (&phoenixFillCounter{}).identify(anchor, first)
	p.lastTrades["BTC"] = anchor
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 8)
	errs := make(chan error, 2)
	done := make(chan error, 1)
	go func() { done <- p.run(ctx, trades, errs) }()
	var got []quanttick.TradeEvent
	for len(got) < 3 {
		select {
		case trade := <-trades:
			got = append(got, trade)
		case err := <-errs:
			t.Fatalf("unexpected recovery error: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for recovered and buffered fills")
		}
	}
	cancel()
	<-done
	close(trades)
	close(errs)
	for extra := range trades {
		t.Errorf("duplicate trade after retry: %+v", extra)
	}
	for err := range errs {
		t.Errorf("unexpected recovery error: %v", err)
	}
	counter := phoenixFillCounter{}
	var wantIDs []string
	for _, fill := range []phoenixFill{second, third, third} {
		trade, _ := parsePhoenixFill(fill, time.Now())
		wantIDs = append(wantIDs, counter.identify(trade, fill))
	}
	if !reflect.DeepEqual(tradeUIDs(got), wantIDs) || p.lastTrades["BTC"].UID != wantIDs[2] {
		t.Fatalf("recovered IDs=%v anchor=%s, want %v", tradeUIDs(got), p.lastTrades["BTC"].UID, wantIDs)
	}
	if requests.Load() != 3 {
		t.Fatalf("REST requests=%d, want one lagging pass and two complete pages", requests.Load())
	}
	var end string
	for _, cursor := range []string{"", "", "older"} {
		query := <-queries
		if end == "" {
			end = query.Get("endTime")
		}
		if query.Get("startTime") != "1790208001000" || query.Get("endTime") != end || query.Get("cursor") != cursor {
			t.Fatalf("recovery changed its original window or cursor: %v", query)
		}
	}
}

func TestPhoenixRecoveryDeadlinePreservesOtherMarkets(t *testing.T) {
	btc := phoenixTestFill("btc-anchor", "2026-09-24T00:00:01Z")
	eth := phoenixTestFill("eth-anchor", "2026-09-24T00:00:01Z")
	eth.Symbol = "ETH"
	newETH := phoenixTestFill("eth-recovered", "2026-09-24T00:00:02Z")
	newETH.Symbol = "ETH"
	var btcRequests, ethRequests atomic.Int32
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/BTC/") {
			btcRequests.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{}, "hasMore": false})
		} else {
			ethRequests.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{newETH, eth}, "hasMore": false})
		}
	}))
	defer rest.Close()
	p := NewPhoenix([]string{"BTC", "ETH"}, WithPhoenixRESTURL(rest.URL), WithPhoenixHTTPClient(rest.Client()))
	p.recoveryThrottle = newRESTThrottle(0)
	for _, fill := range []phoenixFill{btc, eth} {
		anchor, _ := parsePhoenixFill(fill, time.Now())
		anchor.UID = (&phoenixFillCounter{}).identify(anchor, fill)
		p.lastTrades[fill.Symbol] = anchor
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	rows, err := p.recoverTrades(ctx, p.lastTrades["BTC"].Timestamp.Add(5*time.Second))
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "recovery gap for BTC") || !strings.Contains(err.Error(), "previous WebSocket fill") {
		t.Fatalf("expected a bounded missing-anchor gap, got %v", err)
	}
	if btcRequests.Load() < 2 || ethRequests.Load() != 1 {
		t.Fatalf("BTC requests=%d ETH requests=%d, want retries only for the pending market", btcRequests.Load(), ethRequests.Load())
	}
	want, _ := parsePhoenixFill(newETH, time.Now())
	want.UID = (&phoenixFillCounter{}).identify(want, newETH)
	if len(rows) != 1 || rows[0].UID != want.UID {
		t.Fatalf("lost successful ETH recovery: %+v", rows)
	}
}

func TestPhoenixRecoveryRejectsUnmatchedAnchorOccurrence(t *testing.T) {
	fill := phoenixTestFill("repeated-anchor", "2026-09-24T00:00:01Z")
	var requests atomic.Int32
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{fill}, "hasMore": false})
	}))
	defer rest.Close()
	p := NewPhoenix([]string{"BTC"}, WithPhoenixRESTURL(rest.URL), WithPhoenixHTTPClient(rest.Client()))
	anchor, _ := parsePhoenixFill(fill, time.Now())
	counter := phoenixFillCounter{}
	counter.identify(anchor, fill)
	anchor.UID = counter.identify(anchor, fill)
	p.lastTrades["BTC"] = anchor
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := p.recoverTrades(ctx, anchor.Timestamp.Add(5*time.Second))
	if err == nil || !strings.Contains(err.Error(), "anchor occurrence differs") || requests.Load() != 1 || len(rows) != 0 {
		t.Fatalf("rows=%v requests=%d error=%v, want an immediate occurrence mismatch", rows, requests.Load(), err)
	}
}
