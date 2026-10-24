package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestOKXNormalizesIndividualSpotAndSwapFills(t *testing.T) {
	var instrumentRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/public/instruments" || r.URL.Query().Get("instId") != "BTC-USDT-SWAP" || r.URL.Query().Get("instType") != "SWAP" {
			t.Errorf("request = %s", r.URL)
		}
		instrumentRequests++
		fmt.Fprint(w, `{"code":"0","data":[{"instId":"BTC-USDT-SWAP","ctType":"linear","ctValCcy":"BTC","ctVal":"0.01"}]}`)
	}))
	defer server.Close()
	exchange := NewOKX([]string{"BTC-USDT", "BTC-USDT-SWAP"})
	exchange.RESTURL = server.URL
	if err := exchange.loadInstruments(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := exchange.loadInstruments(context.Background()); err != nil {
		t.Fatal(err)
	}
	if instrumentRequests != 1 {
		t.Fatalf("metadata requests = %d", instrumentRequests)
	}
	now := time.Now()
	for _, test := range []struct{ message, id, quantity, volume string }{
		{`{"arg":{"channel":"trades-all","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1069679516","px":"83682.9","sz":"0.000166","side":"sell","ts":"1791870960866","source":"0"}]}`, "1069679516", "0.000166", "13.8913614"},
		{`{"arg":{"channel":"trades-all","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"2990801406","px":"83649.5","sz":"0.01","side":"sell","ts":"1791870960875","source":"0"}]}`, "2990801406", "0.0001", "8.36495"},
	} {
		message, err := exchange.parseMessage([]byte(test.message), now)
		if err != nil || len(message.trades) != 1 {
			t.Fatalf("parse = %+v %v", message, err)
		}
		trade := message.trades[0]
		if trade.UID != test.id || trade.Notional.String() != test.quantity || trade.Volume.String() != test.volume || trade.TickRule != -1 || trade.Ticks != 1 || !trade.ReceivedAt.Equal(now) {
			t.Fatalf("trade = %+v", trade)
		}
	}
}

func TestOKXRejectsUnsupportedContractUnits(t *testing.T) {
	for _, instrument := range []string{
		`{"instId":"BTC-USDT-SWAP","ctType":"inverse","ctValCcy":"USD","ctVal":"100"}`,
		`{"instId":"BTC-USDT-SWAP","ctType":"linear","ctValCcy":"USDT","ctVal":"1"}`,
		`{"instId":"ETH-USDT-SWAP","ctType":"linear","ctValCcy":"BTC","ctVal":"1"}`,
		`{"instId":"BTC-USDT-SWAP","ctType":"linear","ctValCcy":"BTC","ctVal":"0"}`,
	} {
		t.Run(instrument, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, `{"code":"0","data":[%s]}`, instrument) }))
			defer server.Close()
			exchange := NewOKX([]string{"BTC-USDT-SWAP"})
			exchange.RESTURL = server.URL
			if err := exchange.loadInstruments(context.Background()); err == nil {
				t.Fatal("accepted invalid contract")
			}
		})
	}
}

func TestOKXWaitsForEverySubscription(t *testing.T) {
	endpoint := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
			return err
		}
		if err := writeExchangeWebSocketMessage(ctx, conn, `{"event":"subscribe","arg":{"channel":"trades-all","instId":"BTC-USDT"}}`); err != nil {
			return err
		}
		protocol := marketTestProtocols[0]
		if err := protocol.trade(conn, ctx, 102, time.Now().UnixMilli(), "100"); err != nil {
			return err
		}
		_, _, _ = conn.Read(ctx)
		return nil
	})
	exchange := NewOKX([]string{"BTC-USDT", "ETH-USDT"})
	exchange.URL, exchange.SubscriptionTimeout = endpoint, 30*time.Millisecond
	out := make(chan quanttick.TradeEvent, 1)
	if err := exchange.run(context.Background(), out, make(chan error, 1)); err == nil {
		t.Fatal("missing acknowledgement accepted")
	}
	if len(out) != 0 || len(exchange.last) != 0 {
		t.Fatal("incomplete subscription published trades")
	}
}

func TestUpbitUsesTradeTimestampAndExactKRWAmounts(t *testing.T) {
	exchange := NewUpbit([]string{"KRW-BTC"})
	data := []byte(`{"type":"trade","code":"KRW-BTC","timestamp":1791870963147,"trade_timestamp":1791870963099,"trade_price":113150000.00000000,"trade_volume":0.00017675,"ask_bid":"BID","sequential_id":17918709630990000,"stream_type":"REALTIME"}`)
	parsed, err := exchange.parseMessage(data, time.Now())
	if err != nil || len(parsed.trades) != 1 {
		t.Fatalf("parse = %+v %v", parsed, err)
	}
	trade := parsed.trades[0]
	if trade.UID != "17918709630990000" || trade.Timestamp.UnixMilli() != 1791870963099 || trade.Price.String() != "113150000" || trade.Notional.String() != "0.00017675" || trade.Volume.String() != "19999.2625" || trade.TickRule != 1 || trade.Ticks != 1 {
		t.Fatalf("trade = %+v", trade)
	}
	snapshot := strings.Replace(string(data), "REALTIME", "SNAPSHOT", 1)
	parsed, err = exchange.parseMessage([]byte(snapshot), time.Now())
	if err != nil || len(parsed.trades) != 0 {
		t.Fatalf("snapshot parsed = %+v %v", parsed, err)
	}
}

func TestMarketParsersRejectInvalidTrades(t *testing.T) {
	for _, protocol := range marketTestProtocols {
		for _, field := range []string{"side", "size", "price", "id", "symbol", "time"} {
			t.Run(protocol.name+"/"+field, func(t *testing.T) {
				stream := protocol.newStream()
				if stream.prepare != nil {
					if err := stream.prepare(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				row := protocol.row(102, time.Now().UnixMilli(), "100", true).(map[string]any)
				fields := map[string]string{"side": "side", "size": "sz", "price": "px", "id": "tradeId", "symbol": "instId", "time": "ts"}
				if protocol.name == UpbitName {
					fields = map[string]string{"side": "ask_bid", "size": "trade_volume", "price": "trade_price", "id": "sequential_id", "symbol": "code", "time": "trade_timestamp"}
				}
				key := fields[field]
				if field == "side" || field == "symbol" {
					row[key] = "unknown"
				} else {
					row[key] = nil
				}
				var body any = row
				if protocol.name == OKXName {
					body = map[string]any{"arg": map[string]string{"channel": "trades-all", "instId": protocol.symbol}, "data": []any{row}}
				}
				data, _ := json.Marshal(body)
				if _, err := stream.parse(data, time.Now()); err == nil {
					t.Fatalf("accepted malformed %s", data)
				}
			})
		}
	}
}

func TestUpbitRecoveryPagesAcrossUTCMidnight(t *testing.T) {
	midnight := time.Now().UTC().Truncate(24 * time.Hour)
	protocol := marketTestProtocols[1]
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("days_ago")+":"+r.URL.Query().Get("cursor"))
		switch queries[len(queries)-1] {
		case "0:":
			protocol.writePage(w, []int{103}, midnight.Add(time.Second).UnixMilli())
		case "0:103":
			protocol.writePage(w, nil, 0)
		case "1:":
			protocol.writePage(w, []int{102, 101}, midnight.Add(-time.Second).UnixMilli())
		default:
			t.Errorf("unexpected request %s", r.URL)
			protocol.writePage(w, nil, 0)
		}
	}))
	defer server.Close()
	exchange := NewUpbit([]string{protocol.symbol})
	exchange.RESTURL = server.URL
	exchange.throttle = newRESTThrottle(0)
	anchor, err := parseMarketTrade(UpbitName, protocol.symbol, "102", fmt.Sprint(midnight.Add(-time.Second).UnixMilli()), "100", "1", "buy", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	exchange.last[protocol.symbol] = anchor
	recovered, err := exchange.recoverTrades(context.Background(), protocol.symbol, "102")
	if err != nil || len(recovered) != 1 || recovered[0].UID != "103" {
		t.Fatalf("recovery = %+v %v", recovered, err)
	}
	if !reflect.DeepEqual(queries, []string{"0:", "0:103", "1:"}) {
		t.Fatalf("queries = %v", queries)
	}
}

func TestOKXRecoveryPagesAndConvertsSwapSize(t *testing.T) {
	millis := time.Now().UnixMilli()
	protocol := marketTestProtocol{OKXName, "BTC-USDT-SWAP"}
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/market/history-trades" || r.URL.Query().Get("instId") != protocol.symbol || r.URL.Query().Get("limit") != "100" {
			t.Errorf("request = %s", r.URL)
		}
		cursor := r.URL.Query().Get("after")
		queries = append(queries, cursor)
		if cursor == "" {
			protocol.writePage(w, []int{104, 103}, millis)
			return
		}
		if cursor != "103" || r.URL.Query().Get("type") != "1" {
			t.Errorf("bad ID cursor %s", r.URL)
		}
		protocol.writePage(w, []int{102, 101}, millis)
	}))
	defer server.Close()
	exchange := NewOKX([]string{protocol.symbol})
	exchange.RESTURL = server.URL
	exchange.throttle = newRESTThrottle(0)
	exchange.basePerContract[protocol.symbol] = quanttick.MustDecimal("0.01")
	anchor, err := exchange.parseTrade(okxTrade{Symbol: protocol.symbol, ID: "102", Price: "100", Size: "1", Side: "buy", Time: fmt.Sprint(millis)}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	exchange.last[protocol.symbol] = anchor
	recovered, err := exchange.recoverTrades(context.Background(), protocol.symbol, "102")
	if err != nil || len(recovered) != 2 || recovered[0].UID != "103" || recovered[1].UID != "104" || recovered[0].Notional.String() != "0.01" || recovered[0].Volume.String() != "1.01" {
		t.Fatalf("recovery = %+v %v", recovered, err)
	}
	if !reflect.DeepEqual(queries, []string{"", "103"}) {
		t.Fatalf("queries = %v", queries)
	}
}

func TestMarketHTTPRetryHonorsCooldownAndCancellation(t *testing.T) {
	for _, code := range []string{"429", "50011"} {
		t.Run(code, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if code == "429" {
					w.Header().Set("Retry-After", "3")
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				fmt.Fprint(w, `{"code":"50011","msg":"rate limited","data":[]}`)
			}))
			defer server.Close()
			exchange := NewOKX([]string{"BTC-USDT"})
			exchange.RESTURL = server.URL
			exchange.throttle = newRESTThrottle(0)
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			var rows []any
			if err := exchange.getData(ctx, "/test", url.Values{}, &rows); err == nil {
				t.Fatal("retry ignored deadline")
			}
			if calls != 1 {
				t.Fatalf("requests during cooldown = %d", calls)
			}
			minimum := time.Second
			if code == "429" {
				minimum = 2 * time.Second
			}
			if time.Until(exchange.throttle.nextRequest) < minimum {
				t.Fatal("cooldown shorter than API response/fallback")
			}
		})
	}
}

func TestMarketRecoveryRejectsChangedAnchor(t *testing.T) {
	anchor, _ := parseMarketTrade(UpbitName, "KRW-BTC", "102", "1791870963099", "100", "1", "buy", time.Now())
	changed := anchor
	changed.Notional = quanttick.MustDecimal("2")
	_, err := recoverMarketTrades(context.Background(), anchor, func(context.Context, string) ([]quanttick.TradeEvent, string, error) {
		return []quanttick.TradeEvent{changed}, "", nil
	})
	if err == nil || !strings.Contains(err.Error(), "anchor 102 changed") {
		t.Fatalf("error = %v", err)
	}
}
