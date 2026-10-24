package exchanges

import (
	"context"
	"encoding/json"
	"errors"
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

type marketTestProtocol struct{ name, symbol string }

var marketTestProtocols = []marketTestProtocol{{OKXName, "BTC-USDT"}, {UpbitName, "KRW-BTC"}}

func (p marketTestProtocol) newStream() *marketStream {
	if p.name == OKXName {
		return NewOKX([]string{p.symbol}).marketStream
	}
	return NewUpbit([]string{p.symbol}).marketStream
}

func (p marketTestProtocol) row(id int, millis int64, price string, live bool) any {
	if p.name == OKXName {
		return map[string]any{"instId": p.symbol, "tradeId": strconv.Itoa(id), "ts": strconv.FormatInt(millis, 10), "px": price, "sz": "1", "side": "buy"}
	}
	row := map[string]any{"market": p.symbol, "sequential_id": id, "timestamp": millis, "trade_price": json.Number(price), "trade_volume": 1, "ask_bid": "BID"}
	if live {
		row["type"], row["code"], row["trade_timestamp"], row["timestamp"], row["stream_type"] = "trade", p.symbol, millis, millis+37, "REALTIME"
		delete(row, "market")
	}
	return row
}

func (p marketTestProtocol) trade(conn *websocket.Conn, ctx context.Context, id int, millis int64, price string) error {
	row := p.row(id, millis, price, true)
	messageType := websocket.MessageBinary
	if p.name == OKXName {
		row = map[string]any{"arg": map[string]string{"channel": "trades-all", "instId": p.symbol}, "data": []any{row}}
		messageType = websocket.MessageText
	}
	data, _ := json.Marshal(row)
	return conn.Write(ctx, messageType, data)
}

func (p marketTestProtocol) acknowledge(ctx context.Context, conn *websocket.Conn) error {
	if p.name != OKXName {
		return nil
	}
	return writeExchangeWebSocketMessage(ctx, conn, fmt.Sprintf(`{"event":"subscribe","arg":{"channel":"trades-all","instId":%q}}`, p.symbol))
}

func (p marketTestProtocol) writePage(w http.ResponseWriter, ids []int, millis int64) {
	rows := make([]any, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, p.row(id, millis, marketTestPrice(id), false))
	}
	var body any = rows
	if p.name == OKXName {
		body = map[string]any{"code": "0", "data": rows}
	}
	_ = json.NewEncoder(w).Encode(body)
}

func marketTestPrice(id int) string {
	if id == 101 || id == 102 || id == 104 {
		return "100"
	}
	return strconv.Itoa(id - 2)
}

func TestMarketStreamsRecoverOrderedSuffixBeforeWebSocket(t *testing.T) {
	for _, protocol := range marketTestProtocols {
		for _, tc := range []struct {
			name       string
			lagging    bool
			firstIDs   []int
			timeOffset int64
		}{
			{name: "current"},
			{name: "behind-stream", lagging: true, firstIDs: []int{104, 103, 102}},
			{name: "behind-anchor", lagging: true, firstIDs: []int{101}, timeOffset: -1},
			{name: "same-time-missing-anchor", lagging: true, firstIDs: []int{101}},
			{name: "empty-history", lagging: true},
		} {
			t.Run(protocol.name+"/"+tc.name, func(t *testing.T) {
				millis := time.Now().UTC().UnixMilli()
				var requests, connections atomic.Int32
				rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("after") != "" || r.URL.Query().Get("cursor") != "" {
						protocol.writePage(w, nil, millis)
						return
					}
					n := requests.Add(1)
					if tc.lagging && n == 1 {
						protocol.writePage(w, tc.firstIDs, millis+tc.timeOffset)
						return
					}
					protocol.writePage(w, []int{106, 105, 104, 103, 102, 101}, millis)
				}))
				defer rest.Close()
				closeFirst := make(chan struct{})
				endpoint := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
					data, err := readExchangeWebSocketMessage(ctx, conn)
					if err != nil {
						return err
					}
					if protocol.name == OKXName && !strings.Contains(string(data), `"trades-all"`) {
						return fmt.Errorf("wrong subscription %s", data)
					}
					if protocol.name == UpbitName && !strings.Contains(string(data), `"is_only_realtime":true`) {
						return fmt.Errorf("wrong subscription %s", data)
					}
					if connections.Add(1) == 1 {
						if err := protocol.acknowledge(ctx, conn); err != nil {
							return err
						}
						if err := protocol.trade(conn, ctx, 102, millis, "100"); err != nil {
							return err
						}
						select {
						case <-closeFirst:
						case <-ctx.Done():
							return ctx.Err()
						}
						_ = conn.Close(websocket.StatusNormalClosure, "reconnect")
						return nil
					}
					// OKX can send fills before the subscription acknowledgement.
					if err := protocol.trade(conn, ctx, 105, millis, "103"); err != nil {
						return err
					}
					if err := protocol.acknowledge(ctx, conn); err != nil {
						return err
					}
					for _, id := range []int{105, 106} {
						if err := protocol.trade(conn, ctx, id, millis, marketTestPrice(id)); err != nil {
							return err
						}
					}
					_, _, _ = conn.Read(ctx)
					return nil
				})
				exchange := protocol.newStream()
				exchange.URL, exchange.RESTURL = endpoint, rest.URL
				exchange.ReconnectDelay, exchange.HeartbeatInterval = 0, 0
				exchange.throttle = newRESTThrottle(0)
				ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
				defer cancel()
				trades, errs := exchange.Trades(ctx)
				var received []quanttick.TradeEvent
				for len(received) < 5 {
					select {
					case trade, ok := <-trades:
						if !ok {
							t.Fatal("stream closed")
						}
						received = append(received, trade)
						if len(received) == 1 {
							close(closeFirst)
						}
					case err := <-errs:
						t.Fatalf("exchange error: %v", err)
					case <-ctx.Done():
						t.Fatalf("received %d trades: %v", len(received), ctx.Err())
					}
				}
				cancel()
				for range trades {
				}
				var ids []string
				aggregator := quanttick.NewTradeAggregator()
				for _, trade := range received {
					ids = append(ids, trade.UID)
					if trade.Exchange != protocol.name || !trade.Timestamp.Equal(time.UnixMilli(millis)) {
						t.Fatalf("wrong identity/timestamp: %+v", trade)
					}
					flushed, err := aggregator.Add(trade)
					if err != nil || len(flushed) != 0 {
						t.Fatalf("unexpected aggregation flush: %v %v", flushed, err)
					}
				}
				if !reflect.DeepEqual(ids, []string{"102", "103", "104", "105", "106"}) {
					t.Fatalf("fill order = %v", ids)
				}
				grouped, err := aggregator.Flush(quanttick.ExchangeSymbolKey(protocol.name, protocol.symbol))
				if err != nil || len(grouped) != 1 || grouped[0].Ticks != 5 || grouped[0].Price.String() != "104" || grouped[0].Volume.String() != "508" {
					t.Fatalf("aggregated fills = %+v, err=%v", grouped, err)
				}
				if tc.lagging && requests.Load() < 2 {
					t.Fatal("REST handoff did not wait for overlap")
				}
			})
		}
	}
}

func TestMarketRecoveryMissingAnchorReportsGapAndResumes(t *testing.T) {
	for _, protocol := range marketTestProtocols {
		t.Run(protocol.name, func(t *testing.T) {
			millis := time.Now().UnixMilli()
			rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cursor := r.URL.Query().Get("after")
				if protocol.name == UpbitName {
					cursor = r.URL.Query().Get("cursor")
				}
				if cursor == "" {
					protocol.writePage(w, []int{103}, millis+1)
				} else {
					protocol.writePage(w, nil, millis)
				}
			}))
			defer rest.Close()
			endpoint := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				if err := protocol.acknowledge(ctx, conn); err != nil {
					return err
				}
				if err := protocol.trade(conn, ctx, 104, millis+2, "100"); err != nil {
					return err
				}
				_, _, _ = conn.Read(ctx)
				return nil
			})
			exchange := protocol.newStream()
			exchange.URL, exchange.RESTURL = endpoint, rest.URL
			exchange.HeartbeatInterval = 0
			exchange.throttle = newRESTThrottle(0)
			exchange.last[protocol.symbol] = quanttick.NewTradeEvent(quanttick.TradeEventInput{Exchange: protocol.name, Symbol: protocol.symbol, UID: "102", Timestamp: time.UnixMilli(millis), Price: quanttick.MustDecimal("100"), Notional: quanttick.MustDecimal("1"), TickRule: 1})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			trades, errs := exchange.Trades(ctx)
			select {
			case err := <-errs:
				if err == nil || !strings.Contains(err.Error(), "recovery gap") {
					t.Fatalf("error = %v", err)
				}
			case <-ctx.Done():
				t.Fatal("no gap report")
			}
			select {
			case trade := <-trades:
				if trade.UID != "104" {
					t.Fatalf("replayed unanchored trade: %+v", trade)
				}
			case <-ctx.Done():
				t.Fatal("live collection did not resume")
			}
			cancel()
			for range trades {
			}
		})
	}
}

func TestMarketRecoveryLagRetainsBufferUntilDeadline(t *testing.T) {
	for _, protocol := range marketTestProtocols {
		for _, active := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/active=%v", protocol.name, active), func(t *testing.T) {
				millis := time.Now().UnixMilli()
				var requests atomic.Int32
				rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					protocol.writePage(w, []int{101}, millis-1)
				}))
				defer rest.Close()
				exchange := protocol.newStream()
				exchange.RESTURL = rest.URL
				exchange.throttle = newRESTThrottle(0)
				anchor, err := parseMarketTrade(protocol.name, protocol.symbol, "102", fmt.Sprint(millis), "100", "1", "buy", time.Now())
				if err != nil {
					t.Fatal(err)
				}
				exchange.last[protocol.symbol] = anchor
				stream := make(chan quanttick.TradeEvent, 1)
				var buffered []quanttick.TradeEvent
				if active {
					trade := anchor
					trade.UID = "105"
					buffered = append(buffered, trade)
					trade.UID = "106"
					stream <- trade
				}
				ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
				defer cancel()
				if exchange.prepare != nil {
					if err := exchange.prepare(ctx); err != nil {
						t.Fatal(err)
					}
				}
				recovered, held, err := recoverWithWebSocketOverlap(
					ctx, protocol.name, exchange.Symbols, map[string]string{protocol.symbol: anchor.UID},
					exchange.recoverSymbol, time.Millisecond, buffered, stream, make(chan error),
				)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("recovery ended before its deadline: %v", err)
				}
				if requests.Load() < 2 || len(recovered) != 0 || exchange.last[protocol.symbol].UID != anchor.UID {
					t.Fatalf("requests=%d recovered=%v anchor=%s", requests.Load(), tradeUIDs(recovered), exchange.last[protocol.symbol].UID)
				}
				if active {
					if got := tradeUIDs(held); !reflect.DeepEqual(got, []string{"105", "106"}) {
						t.Fatalf("buffered fills = %v", got)
					}
				} else if len(held) != 0 {
					t.Fatalf("unexpected buffered fills = %v", tradeUIDs(held))
				}
			})
		}
	}
}

func TestMarketSubscriptionRejectsErrorsWithoutPublishing(t *testing.T) {
	for _, protocol := range marketTestProtocols {
		t.Run(protocol.name, func(t *testing.T) {
			endpoint := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				failure := `{"error":{"name":"INVALID_PARAM","message":"bad market"}}`
				if protocol.name == OKXName {
					if err := protocol.trade(conn, ctx, 102, time.Now().UnixMilli(), "100"); err != nil {
						return err
					}
					failure = `{"event":"error","code":"60018","msg":"bad market"}`
				}
				if err := writeExchangeWebSocketMessage(ctx, conn, failure); err != nil {
					return err
				}
				_, _, _ = conn.Read(ctx)
				return nil
			})
			exchange := protocol.newStream()
			exchange.URL, exchange.HeartbeatInterval = endpoint, 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			out := make(chan quanttick.TradeEvent, 10)
			err := exchange.run(ctx, out, make(chan error, 1))
			if err == nil || !strings.Contains(err.Error(), "bad market") {
				t.Fatalf("error = %v", err)
			}
			if len(out) != 0 || len(exchange.last) != 0 {
				t.Fatal("failed subscription advanced collection")
			}
		})
	}
}

func TestMarketHeartbeatKeepsQuietStreamAlive(t *testing.T) {
	for _, protocol := range marketTestProtocols {
		t.Run(protocol.name, func(t *testing.T) {
			pongSent := make(chan struct{})
			endpoint := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if _, err := readExchangeWebSocketMessage(ctx, conn); err != nil {
					return err
				}
				if err := protocol.acknowledge(ctx, conn); err != nil {
					return err
				}
				if protocol.name == OKXName {
					first := true
					for {
						data, err := readExchangeWebSocketMessage(ctx, conn)
						if err != nil {
							return nil
						}
						if string(data) != "ping" {
							return fmt.Errorf("heartbeat = %s", data)
						}
						if err := writeExchangeWebSocketMessage(ctx, conn, "pong"); err != nil {
							return err
						}
						if first {
							close(pongSent)
							first = false
						}
					}
				}
				// Read services protocol-level ping/pong frames on an idle market.
				close(pongSent)
				_, _, _ = conn.Read(ctx)
				return nil
			})
			exchange := protocol.newStream()
			exchange.URL, exchange.HeartbeatInterval = endpoint, 10*time.Millisecond
			exchange.SubscriptionTimeout = 20 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			trades, errs := exchange.Trades(ctx)
			select {
			case <-pongSent:
			case err := <-errs:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal("heartbeat timed out")
			}
			select {
			case err := <-errs:
				t.Fatal(err)
			case <-time.After(40 * time.Millisecond):
			}
			cancel()
			for range trades {
			}
		})
	}
}
