package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	OKXName    = "okx"
	OKXURL     = "wss://ws.okx.com:8443/ws/v5/business"
	OKXRESTURL = "https://www.okx.com/api/v5"
)

var _ quanttick.Exchange = (*OKX)(nil)

type OKX struct {
	*marketStream
	basePerContract map[string]quanttick.Decimal
}

func NewOKX(symbols []string) *OKX {
	o := &OKX{marketStream: newMarketStream(OKXName, OKXURL, OKXRESTURL, symbols),
		basePerContract: make(map[string]quanttick.Decimal)}
	o.requireAck, o.textPing = true, true
	o.prepare = o.loadInstruments
	o.subscription = o.subscriptionMessage
	o.parse = o.parseMessage
	o.recoverSymbol = o.recoverTrades
	return o
}

func (o *OKX) subscriptionMessage() any {
	args := make([]map[string]string, 0, len(o.Symbols))
	for _, symbol := range o.Symbols {
		args = append(args, map[string]string{"channel": "trades-all", "instId": symbol})
	}
	return map[string]any{"op": "subscribe", "args": args}
}

func (o *OKX) getData(ctx context.Context, path string, params url.Values, target any) error {
	for {
		var payload struct {
			Code    string          `json:"code"`
			Message string          `json:"msg"`
			Data    json.RawMessage `json:"data"`
		}
		if err := o.get(ctx, path, params, &payload); err != nil {
			return err
		}
		if payload.Code == "50011" || payload.Code == "50040" {
			o.throttle.deferFor(2 * time.Second)
			continue
		}
		if payload.Code != "0" {
			return fmt.Errorf("OKX %s: %s %s", path, payload.Code, payload.Message)
		}
		return json.Unmarshal(payload.Data, target)
	}
}

// Swap sizes are contracts. Both live and recovered fills use the instrument's
// base quantity per contract so quote volume retains the same units as spot.
func (o *OKX) loadInstruments(ctx context.Context) error {
	for _, symbol := range o.Symbols {
		if _, ok := o.basePerContract[symbol]; ok {
			continue
		}
		parts := strings.Split(symbol, "-")
		if len(parts) == 2 {
			o.basePerContract[symbol] = quanttick.MustDecimal("1")
			continue
		}
		if len(parts) != 3 || parts[2] != "SWAP" {
			return fmt.Errorf("unsupported OKX instrument %s", symbol)
		}
		var rows []struct {
			ID       string `json:"instId"`
			Type     string `json:"ctType"`
			Currency string `json:"ctValCcy"`
			Value    string `json:"ctVal"`
		}
		if err := o.getData(ctx, "/public/instruments", url.Values{"instType": {"SWAP"}, "instId": {symbol}}, &rows); err != nil {
			return err
		}
		if len(rows) != 1 || rows[0].ID != symbol || rows[0].Type != "linear" || rows[0].Currency != parts[0] {
			return fmt.Errorf("OKX %s requires a linear base-denominated contract", symbol)
		}
		value, err := quanttick.ParseDecimal(rows[0].Value)
		if err != nil || !value.IsPositive() {
			return fmt.Errorf("invalid OKX contract value %q", rows[0].Value)
		}
		o.basePerContract[symbol] = value
	}
	return nil
}

type okxTrade struct {
	Symbol string `json:"instId"`
	ID     string `json:"tradeId"`
	Price  string `json:"px"`
	Size   string `json:"sz"`
	Side   string `json:"side"`
	Time   string `json:"ts"`
}

func (o *OKX) parseTrade(raw okxTrade, receivedAt time.Time) (quanttick.TradeEvent, error) {
	factor, ok := o.basePerContract[raw.Symbol]
	if !ok {
		return quanttick.TradeEvent{}, fmt.Errorf("unconfigured OKX instrument %s", raw.Symbol)
	}
	size, err := quanttick.ParseDecimal(raw.Size)
	if err != nil {
		return quanttick.TradeEvent{}, err
	}
	return parseMarketTrade(OKXName, raw.Symbol, raw.ID, raw.Time, raw.Price, size.Mul(factor).String(), raw.Side, receivedAt)
}

func (o *OKX) parseMessage(data []byte, receivedAt time.Time) (marketMessage, error) {
	if string(data) == "pong" {
		return marketMessage{pong: true}, nil
	}
	var payload struct {
		Event   string `json:"event"`
		Code    string `json:"code"`
		Message string `json:"msg"`
		Arg     struct {
			Channel string `json:"channel"`
			Symbol  string `json:"instId"`
		} `json:"arg"`
		Data []okxTrade `json:"data"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return marketMessage{}, err
	}
	if payload.Event == "error" || payload.Event == "notice" {
		return marketMessage{}, fmt.Errorf("OKX websocket %s: %s", payload.Code, payload.Message)
	}
	if payload.Arg.Channel != "trades-all" {
		return marketMessage{}, nil
	}
	if _, ok := o.basePerContract[payload.Arg.Symbol]; !ok {
		return marketMessage{}, fmt.Errorf("unexpected OKX subscription %s", payload.Arg.Symbol)
	}
	if payload.Event == "subscribe" {
		return marketMessage{acknowledged: payload.Arg.Symbol}, nil
	}
	var message marketMessage
	for _, raw := range payload.Data {
		if raw.Symbol != payload.Arg.Symbol {
			return marketMessage{}, fmt.Errorf("OKX trade symbol does not match channel")
		}
		trade, err := o.parseTrade(raw, receivedAt)
		if err != nil {
			return marketMessage{}, err
		}
		message.trades = append(message.trades, trade)
	}
	return message, nil
}

func (o *OKX) recoverTrades(ctx context.Context, symbol, _ string) ([]quanttick.TradeEvent, error) {
	return recoverMarketTrades(ctx, o.last[symbol], func(ctx context.Context, cursor string) ([]quanttick.TradeEvent, string, error) {
		params := url.Values{"instId": {symbol}, "limit": {"100"}}
		if cursor != "" {
			params.Set("type", "1")
			params.Set("after", cursor)
		}
		var page []okxTrade
		if err := o.getData(ctx, "/market/history-trades", params, &page); err != nil {
			return nil, "", err
		}
		trades := make([]quanttick.TradeEvent, 0, len(page))
		for _, raw := range page {
			trade, err := o.parseTrade(raw, time.Now().UTC())
			if err != nil {
				return nil, "", err
			}
			trades = append(trades, trade)
		}
		next := ""
		if len(page) > 0 {
			next = page[len(page)-1].ID
		}
		return trades, next, nil
	})
}
