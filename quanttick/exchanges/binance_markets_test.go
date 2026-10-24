package exchanges

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestBinanceCompatibleMarketsRecoverIndividualTrades(t *testing.T) {
	for _, test := range []struct {
		name        string
		constructor func([]string, ...BinanceOption) *Binance
	}{
		{BinanceName, NewBinance}, {BinanceUSName, NewBinanceUS}, {AsterName, NewAster},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests, connections atomic.Int32
			rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if test.name == AsterName && r.URL.Query().Get("fromId") == "" {
					fmt.Fprint(w, `[{"id":104,"price":"100","qty":"2","time":1791870963099,"isBuyerMaker":true}]`)
					return
				}
				if r.URL.Path != "/historicalTrades" || r.URL.Query().Get("fromId") != "103" || r.URL.Query().Get("symbol") != "BTCUSDT" || r.Header.Get("X-MBX-APIKEY") != "" {
					t.Errorf("invalid public recovery request: %s", r.URL)
				}
				fmt.Fprint(w, `[{"id":103,"price":"101","qty":"1","time":1791870963099,"isBuyerMaker":false},{"id":104,"price":"100","qty":"2","time":1791870963099,"isBuyerMaker":true}]`)
			}))
			defer rest.Close()
			closeFirst := make(chan struct{})
			endpoint := newExchangeWebSocketServer(t, func(ctx context.Context, conn *websocket.Conn) error {
				data, err := readExchangeWebSocketMessage(ctx, conn)
				if err != nil {
					return err
				}
				if !strings.Contains(string(data), `btcusdt@trade`) {
					return fmt.Errorf("wrong subscription %s", data)
				}
				if err := writeExchangeWebSocketMessage(ctx, conn, `{"result":null,"id":1}`); err != nil {
					return err
				}
				id, price, qty, maker := 102, "100", "1", "false"
				if connections.Add(1) > 1 {
					id, qty, maker = 104, "2", "true"
				}
				message := fmt.Sprintf(`{"e":"trade","s":"BTCUSDT","t":%d,"T":1791870963099,"E":1791870963137,"p":%q,"q":%q,"m":%s}`, id, price, qty, maker)
				if err := writeExchangeWebSocketMessage(ctx, conn, message); err != nil {
					return err
				}
				if id == 102 {
					select {
					case <-closeFirst:
					case <-ctx.Done():
						return ctx.Err()
					}
					_ = conn.Close(websocket.StatusNormalClosure, "reconnect")
				} else {
					_, _, _ = conn.Read(ctx)
				}
				return nil
			})
			exchange := test.constructor([]string{"BTCUSDT"}, WithBinanceURL(endpoint), WithBinanceRESTURL(rest.URL), WithBinanceReconnectDelay(0))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			trades, errs := exchange.Trades(ctx)
			var ids []string
			for len(ids) < 3 {
				select {
				case trade := <-trades:
					ids = append(ids, trade.UID)
					if trade.Exchange != test.name || trade.Ticks != 1 {
						t.Fatalf("trade = %+v", trade)
					}
					if trade.UID == "104" && (trade.TickRule != -1 || trade.Volume.String() != "200") {
						t.Fatalf("raw maker/quantity lost: %+v", trade)
					}
					if len(ids) == 1 {
						close(closeFirst)
					}
				case err := <-errs:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatalf("only received %v", ids)
				}
			}
			cancel()
			for range trades {
			}
			wantRequests := int32(1)
			if test.name == AsterName {
				wantRequests = 2
			}
			if !reflect.DeepEqual(ids, []string{"102", "103", "104"}) || requests.Load() != wantRequests {
				t.Fatalf("recovery ids=%v requests=%d", ids, requests.Load())
			}
		})
	}
}
