package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestPhoenixPublicFillFixture(t *testing.T) {
	// Captured from the public BTC fills stream; both rows belong to one transaction.
	data, err := os.ReadFile("testdata/phoenix_fills.json")
	if err != nil {
		t.Fatal(err)
	}
	p := NewPhoenix([]string{"BTC"})
	received := time.Now().UTC()
	_, trades, err := p.parseMessage(data, received, &phoenixFillCounter{})
	if err != nil {
		t.Fatal(err)
	}
	assertStrings(t, tradeExchanges(trades), []string{"phoenix", "phoenix"})
	assertStrings(t, tradeSymbols(trades), []string{"BTC", "BTC"})
	assertDecimals(t, tradePrices(trades), []string{"84405.79642058167", "84411"})
	assertDecimals(t, tradeNotionals(trades), []string{"0.0894", "0.36"})
	assertDecimals(t, tradeVolumes(trades), []string{"7545.8782", "30387.96"})
	assertInts(t, tradeTickRules(trades), []int{-1, -1})
	assertBools(t, tradeSequential(trades), []bool{false, false})
	for _, trade := range trades {
		if trade.Timestamp.Format(time.RFC3339) != "2026-09-24T02:48:02Z" || !trade.ReceivedAt.Equal(received) || trade.Ticks != 1 {
			t.Fatalf("unexpected trade metadata: %+v", trade)
		}
	}
	if trades[0].UID == trades[1].UID {
		t.Fatal("same-transaction fills collided")
	}
}

func TestPhoenixPreservesIdenticalFillsAcrossFrames(t *testing.T) {
	p := NewPhoenix([]string{"BTC"})
	counter := &phoenixFillCounter{}
	fill := phoenixTestFill("transaction", "2026-09-24T00:00:01.123456789Z")
	_, first, err := p.parseMessage(phoenixTestMessage(t, "BTC", fill), time.Now(), counter)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := p.parseMessage(phoenixTestMessage(t, "BTC", fill), time.Now(), counter)
	if err != nil {
		t.Fatal(err)
	}
	if first[0].UID == second[0].UID {
		t.Fatal("identical fills were collapsed")
	}
	if first[0].Nanoseconds != 789 || first[0].Timestamp.Nanosecond() != 123456000 {
		t.Fatalf("lost timestamp precision: %+v", first[0])
	}
	fill.BaseQty = "0.0100"
	fill.QuoteQty = "-844.000"
	fill.Price = "84400.00"
	_, replayed, err := p.parseMessage(phoenixTestMessage(t, "BTC", fill, fill), time.Now(), &phoenixFillCounter{})
	if err != nil {
		t.Fatal(err)
	}
	assertStrings(t, tradeUIDs(replayed), []string{first[0].UID, second[0].UID})
	assertInts(t, tradeTickRules(first), []int{1})
}

func TestPhoenixRejectsInvalidFills(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*phoenixFill)
	}{
		{"zero price", func(f *phoenixFill) { f.Price = "0" }},
		{"invalid price", func(f *phoenixFill) { f.Price = "NaN" }},
		{"zero base", func(f *phoenixFill) { f.BaseQty = "0" }},
		{"invalid base", func(f *phoenixFill) { f.BaseQty = "NaN" }},
		{"same signed quantities", func(f *phoenixFill) { f.QuoteQty = "844" }},
		{"invalid quote", func(f *phoenixFill) { f.QuoteQty = "NaN" }},
		{"missing signature", func(f *phoenixFill) { f.Signature = "" }},
		{"wrong symbol", func(f *phoenixFill) { f.Symbol = "ETH" }},
		{"invalid timestamp", func(f *phoenixFill) { f.Timestamp = "yesterday" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fill := phoenixTestFill("tx", "2026-09-24T00:00:01Z")
			test.change(&fill)
			_, _, err := NewPhoenix([]string{"BTC"}).parseMessage(phoenixTestMessage(t, "BTC", fill), time.Now(), &phoenixFillCounter{})
			if err == nil {
				t.Fatal("accepted invalid fill")
			}
		})
	}
}

func TestPhoenixSubscriptionsAndControlMessages(t *testing.T) {
	p := NewPhoenix([]string{"BTC", "SOL"})
	want := []map[string]any{
		{"type": "subscribe", "subscription": map[string]string{"channel": "fills", "marketSymbol": "BTC"}},
		{"type": "subscribe", "subscription": map[string]string{"channel": "fills", "marketSymbol": "SOL"}},
	}
	if p.Name() != "phoenix" || !reflect.DeepEqual(p.SubscriptionMessages(), want) {
		t.Fatalf("unexpected subscriptions: %#v", p.SubscriptionMessages())
	}
	for _, test := range []struct {
		message, ack string
		fail         bool
	}{
		{`{"channel":"subscriptionStatus","status":"subscribed","subscription":{"channel":"fills","marketSymbol":"BTC"}}`, "BTC", false},
		{`{"type":"subscriptionConfirmed","subscription":{"channel":"fills","symbol":"SOL"}}`, "SOL", false},
		{`{"channel":"subscriptionStatus","status":"unsubscribed","subscription":{"channel":"fills","marketSymbol":"BTC"}}`, "", true},
		{`{"channel":"subscriptionStatus","status":"subscribed","subscription":{"channel":"fills","marketSymbol":"ETH"}}`, "", true},
		{`{"type":"subscriptionConfirmed","subscription":{"channel":"trades","symbol":"BTC"}}`, "", true},
		{`{"type":"subscriptionError","message":"invalid market"}`, "", true},
		{`{"channel":"error","code":400,"error":"unknown channel"}`, "", true},
		{`{"channel":"fills","symbol":"BTC"}`, "", true},
		{`{"channel":"fills","symbol":"ETH","fills":[]}`, "", true},
		{`{"channel":"fills","symbol":"BTC","fills":[]}`, "", false},
		{`{"channel":"pong"}`, "", false},
		{`{`, "", true},
	} {
		t.Run(test.message, func(t *testing.T) {
			ack, _, err := p.parseMessage([]byte(test.message), time.Now(), &phoenixFillCounter{})
			if ack != test.ack || (err != nil) != test.fail {
				t.Fatalf("ack=%q, error=%v", ack, err)
			}
		})
	}
}

func TestPhoenixReconnectRecoversPagesAndRemovesLiveOverlap(t *testing.T) {
	first := phoenixTestFill("anchor-tx", "2026-09-24T00:00:01Z")
	first.BaseQty, first.QuoteQty = "-0.01", "844"
	repeated := phoenixTestFill("repeated-tx", "2026-09-24T00:00:01Z")
	second := phoenixTestFill("next-tx", "2026-09-24T00:00:02Z")
	third := phoenixTestFill("later-tx", "2026-09-24T00:00:03Z")
	recoveryStarted := make(chan struct{})
	liveBuffered := make(chan struct{})
	var requests atomic.Int32
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/v1/trades/BTC/fills" || r.URL.Query().Get("startTime") != "1790208001000" || r.URL.Query().Get("limit") != "1000" {
			t.Errorf("unexpected request: %s", r.URL)
		}
		if r.URL.Query().Get("cursor") == "" {
			close(recoveryStarted)
			select {
			case <-liveBuffered:
			case <-r.Context().Done():
				return
			}
			// Identical fills straddle both the page boundary and the anchor.
			json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{second, repeated, repeated}, "hasMore": true, "nextCursor": "older"})
		} else {
			if r.URL.Query().Get("cursor") != "older" {
				t.Errorf("unexpected cursor: %s", r.URL)
			}
			json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{first, repeated}, "hasMore": false})
		}
	}))
	defer rest.Close()
	var connections atomic.Int32
	wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
			return err
		}
		if connections.Add(1) == 1 {
			if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", first)); err != nil {
				return err
			}
			return conn.Close(websocket.StatusNormalClosure, "reconnect")
		}
		select {
		case <-recoveryStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", repeated, second, third, third)); err != nil {
			return err
		}
		// The reader must stay active while REST waits, including control frames.
		readDone := make(chan struct{})
		go func() {
			_, _, _ = conn.Read(ctx)
			close(readDone)
		}()
		if err := conn.Ping(ctx); err != nil {
			return err
		}
		close(liveBuffered)
		<-readDone
		return nil
	})
	p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixRESTURL(rest.URL), WithPhoenixHTTPClient(rest.Client()), WithPhoenixReconnectDelay(time.Millisecond))
	p.recoveryThrottle = newRESTThrottle(0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	trades, errs := p.Trades(ctx)
	var got []quanttick.TradeEvent
	for len(got) < 6 {
		select {
		case trade, ok := <-trades:
			if !ok {
				t.Fatal("trade stream closed early")
			}
			got = append(got, trade)
		case err := <-errs:
			t.Fatalf("unexpected collector error: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out receiving recovered fills")
		}
	}
	cancel()
	for trade := range trades {
		t.Errorf("extra fill after recovery: %+v", trade)
	}
	if requests.Load() != 2 {
		t.Fatalf("REST requests = %d", requests.Load())
	}
	if !strings.HasSuffix(got[1].UID, ":2") || !strings.HasSuffix(got[2].UID, ":3") {
		t.Fatalf("lost repeated fill: %v", tradeUIDs(got))
	}
	if got[4].UID == got[5].UID {
		t.Fatal("collapsed identical live fills after recovery")
	}
	for i, second := range []int{1, 1, 1, 2, 3, 3} {
		if got[i].Timestamp.Second() != second {
			t.Fatalf("fill order: %v", got)
		}
	}
}

func TestPhoenixReconnectFromWithinASecondPreservesClosingPrice(t *testing.T) {
	data, err := os.ReadFile("testdata/phoenix_recovery.json")
	if err != nil {
		t.Fatal(err)
	}
	var history struct {
		Data []phoenixFill `json:"data"`
	}
	if err := json.Unmarshal(data, &history); err != nil {
		t.Fatal(err)
	}
	slices.Reverse(history.Data)
	for _, test := range []struct {
		name     string
		start    int
		notional string
		volume   string
	}{
		{"first fill", 0, "0.4546", "38372.7962"},
		{"middle fill", 1, "0.3652", "30826.918"},
		{"last fill", 2, "0.0052", "438.958"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(data)
			}))
			defer rest.Close()
			var connections atomic.Int32
			later := phoenixTestFill("later-tx", "2026-09-24T02:48:03Z")
			wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
					return err
				}
				fill := later
				if connections.Add(1) == 1 {
					fill = history.Data[test.start]
				}
				if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", fill)); err != nil {
					return err
				}
				return conn.Close(websocket.StatusNormalClosure, "reconnect")
			})
			p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixRESTURL(rest.URL))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			trades := make(chan quanttick.TradeEvent, len(history.Data)+2)
			errs := make(chan error, 2)
			for range 2 {
				if err := p.run(ctx, trades, errs); err != nil {
					t.Fatal(err)
				}
			}
			close(trades)
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			aggregator := quanttick.NewTradeAggregator()
			var completed, emitted []quanttick.TradeEvent
			for trade := range trades {
				emitted = append(emitted, trade)
				rows, err := aggregator.Add(trade)
				if err != nil {
					t.Fatal(err)
				}
				completed = append(completed, rows...)
			}
			if len(completed) != 1 {
				t.Fatalf("completed aggregates = %d, want 1", len(completed))
			}
			wantClose := quanttick.MustDecimal("84415.00000000001")
			if !completed[0].Price.Equal(wantClose) {
				t.Errorf("aggregate close = %s, want %s; emitted prices = %v", completed[0].Price, wantClose, tradePrices(emitted))
			}
			assertDecimals(t, tradeNotionals(completed), []string{test.notional})
			assertDecimals(t, tradeVolumes(completed), []string{test.volume})
			var wantPrices []string
			for _, fill := range history.Data[test.start:] {
				wantPrices = append(wantPrices, fill.Price)
			}
			wantPrices = append(wantPrices, later.Price)
			assertDecimals(t, tradePrices(emitted), wantPrices)
		})
	}
}

func TestPhoenixReconnectReportsAmbiguousRepeatedFill(t *testing.T) {
	buy := phoenixTestFill("repeated-tx", "2026-09-24T00:00:01Z")
	sell := buy
	sell.BaseQty, sell.QuoteQty = "-0.01", "844"
	later := phoenixTestFill("later-tx", "2026-09-24T00:00:02Z")
	for _, test := range []struct {
		name string
		live []phoenixFill
	}{
		{"first occurrence", []phoenixFill{buy}},
		{"last occurrence after sell", []phoenixFill{sell, buy}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{buy, sell, buy}, "hasMore": false})
			}))
			defer rest.Close()
			var connections atomic.Int32
			wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
					return err
				}
				fills := []phoenixFill{buy, buy, later}
				if connections.Add(1) == 1 {
					fills = test.live
				}
				if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", fills...)); err != nil {
					return err
				}
				return conn.Close(websocket.StatusNormalClosure, "reconnect")
			})
			p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixRESTURL(rest.URL))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			trades := make(chan quanttick.TradeEvent, 8)
			errs := make(chan error, 2)
			for range 2 {
				if err := p.run(ctx, trades, errs); err != nil {
					t.Fatal(err)
				}
			}
			close(trades)
			close(errs)
			var gaps []error
			for err := range errs {
				gaps = append(gaps, err)
			}
			if len(gaps) != 1 || !strings.Contains(gaps[0].Error(), "recovery gap for BTC") || !strings.Contains(gaps[0].Error(), "ambiguous") {
				t.Errorf("recovery errors = %v, want one ambiguous gap", gaps)
			}
			aggregator := quanttick.NewTradeAggregator()
			significant := quanttick.NewSignificantTradeAggregator(quanttick.MustDecimal("1000"), time.Minute)
			var emitted []quanttick.TradeEvent
			for trade := range trades {
				emitted = append(emitted, trade)
				rows, err := aggregator.Add(trade)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					if !row.Volume.Equal(quanttick.MustDecimal("844")) {
						t.Errorf("aggregate volume = %s, want 844", row.Volume)
					}
					signals, err := significant.Add(row)
					if err != nil {
						t.Fatal(err)
					}
					for _, signal := range signals {
						if signal.Volume != nil {
							t.Errorf("false significant trade: %+v", signal)
						}
					}
				}
			}
			if len(emitted) != len(test.live)+1 || emitted[len(emitted)-1].Timestamp.Second() != 2 {
				t.Fatalf("unexpected fills after reconnect: %+v", emitted)
			}
		})
	}
}

func TestPhoenixReconnectWaitsForAnchorSecondToClose(t *testing.T) {
	// The next second keeps the wait observable even when the test starts near
	// a boundary. Live fills arriving during the wait must still be read.
	timestamp := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	first := phoenixTestFill("anchor", timestamp.Format(time.RFC3339))
	second := phoenixTestFill("second", first.Timestamp)
	third := phoenixTestFill("later", timestamp.Add(time.Second).Format(time.RFC3339))
	second.Price, second.QuoteQty = "84500", "-845"
	third.Price, third.QuoteQty = "84600", "-846"
	var requests atomic.Int32
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		wantEnd := timestamp.Add(time.Second)
		if time.Now().Before(wantEnd) || r.URL.Query().Get("endTime") != fmt.Sprint(wantEnd.UnixMilli()) {
			t.Errorf("recovered an unfinished second: now=%s, request=%s", time.Now(), r.URL)
		}
		json.NewEncoder(w).Encode(map[string]any{"data": []phoenixFill{second, first}, "hasMore": false})
	}))
	defer rest.Close()
	wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", second, third)); err != nil {
			return err
		}
		return conn.Close(websocket.StatusNormalClosure, "done")
	})
	p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixRESTURL(rest.URL))
	anchor, err := parsePhoenixFill(first, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	anchor.UID = (&phoenixFillCounter{}).identify(anchor, first)
	p.lastTrades["BTC"] = anchor
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 4)
	errs := make(chan error, 1)
	if err := p.run(ctx, trades, errs); err != nil {
		t.Fatal(err)
	}
	close(trades)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var got []quanttick.TradeEvent
	for trade := range trades {
		got = append(got, trade)
	}
	assertDecimals(t, tradePrices(got), []string{"84500", "84600"})
	if requests.Load() != 1 {
		t.Fatalf("REST requests = %d, want 1", requests.Load())
	}
}

func TestPhoenixWaitsForEverySubscription(t *testing.T) {
	preAck := make(chan struct{})
	release := make(chan struct{})
	wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		for range 2 {
			if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
				return err
			}
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", phoenixTestFill("tx", "2026-09-24T00:00:01Z"))); err != nil {
			return err
		}
		close(preAck)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("SOL")); err != nil {
			return err
		}
		_, _, _ = conn.Read(ctx)
		return nil
	})
	p := NewPhoenix([]string{"BTC", "SOL"}, WithPhoenixURL(wsURL))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 2)
	done := make(chan error, 1)
	go func() { done <- p.run(ctx, trades, make(chan error, 1)) }()
	select {
	case <-preAck:
	case <-ctx.Done():
		t.Fatal("no pre-ack trade")
	}
	select {
	case trade := <-trades:
		t.Fatalf("emitted before all acknowledgements: %+v", trade)
	default:
	}
	close(release)
	select {
	case <-trades:
	case <-ctx.Done():
		t.Fatal("lost buffered trade")
	}
	cancel()
	<-done
}

func TestPhoenixSubscriptionTimeoutAndBufferLimit(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(fmt.Sprint(overflow), func(t *testing.T) {
			wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				if overflow {
					fills := make([]phoenixFill, phoenixTradeLimit+1)
					for i := range fills {
						fills[i] = phoenixTestFill("tx", "2026-09-24T00:00:01Z")
					}
					_ = conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", fills...))
				}
				_, _, _ = conn.Read(ctx)
				return nil
			})
			timeout := 25 * time.Millisecond
			want := "timed out"
			if overflow {
				timeout = time.Second
				want = "buffer exceeded"
			}
			p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixSubscriptionTimeout(timeout))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := p.run(ctx, make(chan quanttick.TradeEvent), make(chan error, 1))
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %v, want %s", err, want)
			}
		})
	}
}

func TestPhoenixRecoveryRejectsIncompleteHistory(t *testing.T) {
	fill := phoenixTestFill("anchor", "2026-09-24T00:00:01Z")
	anchor, _ := parsePhoenixFill(fill, time.Now())
	anchor.UID = (&phoenixFillCounter{}).identify(anchor, fill)
	for _, test := range []struct {
		name     string
		response any
		want     string
	}{
		{"missing anchor", map[string]any{"data": []phoenixFill{}, "hasMore": false}, "previous WebSocket fill"},
		{"missing hasMore", map[string]any{"data": []phoenixFill{fill}}, "malformed"},
		{"stuck cursor", map[string]any{"data": []phoenixFill{fill}, "hasMore": true, "nextCursor": "repeat"}, "pagination did not advance"},
		{"empty cursor", map[string]any{"data": []phoenixFill{fill}, "hasMore": true}, "pagination did not advance"},
		{"outside window", map[string]any{"data": []phoenixFill{phoenixTestFill("future", "2026-09-24T00:00:10Z")}, "hasMore": false}, "window"},
		{"oldest first", map[string]any{"data": []phoenixFill{fill, phoenixTestFill("newer", "2026-09-24T00:00:02Z")}, "hasMore": false}, "newest-first"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(test.response) }))
			defer rest.Close()
			p := NewPhoenix([]string{"BTC"}, WithPhoenixRESTURL(rest.URL))
			p.recoveryThrottle = newRESTThrottle(0)
			p.lastTrades["BTC"] = anchor
			rows, err := p.recoverTrades(context.Background(), anchor.Timestamp.Add(5*time.Second))
			if err == nil || !strings.Contains(err.Error(), test.want) || len(rows) != 0 {
				t.Fatalf("rows=%v error=%v", rows, err)
			}
		})
	}
}

func TestPhoenixRecoveryHonorsRetryAfterAndCancellation(t *testing.T) {
	requested := make(chan struct{})
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		close(requested)
	}))
	defer rest.Close()
	p := NewPhoenix([]string{"BTC"}, WithPhoenixRESTURL(rest.URL))
	p.lastTrades["BTC"] = quanttick.TradeEvent{Timestamp: time.Now().Add(-time.Minute)}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := p.recoverTrades(ctx, time.Now()); done <- err }()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("no recovery request")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted rate-limited recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("rate-limit wait ignored cancellation")
	}
	if time.Until(p.recoveryThrottle.nextRequest) < 59*time.Second {
		t.Fatal("ignored Retry-After")
	}
}

func TestPhoenixRecoveryFailureReportsGapAndKeepsLiveFills(t *testing.T) {
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer rest.Close()
	wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, phoenixTestAck("BTC")); err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", phoenixTestFill("live", "2026-09-24T00:00:02Z"))); err != nil {
			return err
		}
		_, _, _ = conn.Read(ctx)
		return nil
	})
	p := NewPhoenix([]string{"BTC"}, WithPhoenixURL(wsURL), WithPhoenixRESTURL(rest.URL))
	anchor, _ := parsePhoenixFill(phoenixTestFill("previous", "2026-09-24T00:00:01Z"), time.Now())
	p.lastTrades["BTC"] = anchor
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	trades := make(chan quanttick.TradeEvent, 1)
	errs := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- p.run(ctx, trades, errs) }()
	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "recovery gap for BTC") || !strings.Contains(err.Error(), "503") {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("recovery failure was not reported")
	}
	select {
	case trade := <-trades:
		if trade.Timestamp.Second() != 2 {
			t.Fatalf("wrong live fill: %+v", trade)
		}
	case <-ctx.Done():
		t.Fatal("recovery failure blocked live fills")
	}
	cancel()
	<-done
}

func TestPhoenixReaderBoundsCombinedBacklog(t *testing.T) {
	wsURL := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		fill := phoenixTestFill("tx", "2026-09-24T00:00:01Z")
		if err := conn.Write(ctx, websocket.MessageText, phoenixTestMessage(t, "BTC", fill, fill)); err != nil {
			return err
		}
		_, _, _ = conn.Read(ctx)
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := dialWebSocket(ctx, PhoenixName, wsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	backlog, err := newTradeBacklog(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, errs := NewPhoenix([]string{"BTC"}).startTradeReader(ctx, conn, backlog, &phoenixFillCounter{})
	select {
	case err := <-errs:
		if err == nil || !strings.Contains(err.Error(), "buffer exceeded") {
			t.Fatalf("overflow error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("reader did not detect a full backlog")
	}
}

func phoenixTestFill(signature, timestamp string) phoenixFill {
	return phoenixFill{Symbol: "BTC", BaseQty: "0.01", QuoteQty: "-844", Price: "84400", Timestamp: timestamp, Signature: signature, Instruction: "PlaceMarketOrder"}
}

func phoenixTestMessage(t *testing.T, symbol string, fills ...phoenixFill) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"channel": "fills", "symbol": symbol, "fills": fills})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func phoenixTestAck(symbol string) string {
	return fmt.Sprintf(`{"channel":"subscriptionStatus","status":"subscribed","subscription":{"channel":"fills","marketSymbol":%q}}`, symbol)
}
