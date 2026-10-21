package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	UpbitName    = "upbit"
	UpbitURL     = "wss://api.upbit.com/websocket/v1"
	UpbitRESTURL = "https://api.upbit.com/v1"
)

var _ quanttick.Exchange = (*Upbit)(nil)

type Upbit struct{ *marketStream }

func NewUpbit(symbols []string) *Upbit {
	u := &Upbit{newMarketStream(UpbitName, UpbitURL, UpbitRESTURL, symbols)}
	u.subscription = u.subscriptionMessage
	u.parse = u.parseMessage
	u.recoverSymbol = u.recoverTrades
	return u
}

func (u *Upbit) subscriptionMessage() any {
	return []any{
		map[string]string{"ticket": "go-quant-tick"},
		map[string]any{"type": "trade", "codes": u.Symbols, "is_only_realtime": true},
		map[string]string{"format": "DEFAULT"},
	}
}

type upbitTrade struct {
	Type           string      `json:"type"`
	Code           string      `json:"code"`
	Market         string      `json:"market"`
	ID             json.Number `json:"sequential_id"`
	Price          json.Number `json:"trade_price"`
	Size           json.Number `json:"trade_volume"`
	Side           string      `json:"ask_bid"`
	Timestamp      int64       `json:"timestamp"`
	TradeTimestamp int64       `json:"trade_timestamp"`
	StreamType     string      `json:"stream_type"`
	Error          *struct {
		Name    string `json:"name"`
		Message string `json:"message"`
	} `json:"error"`
}

func (u *Upbit) parseTrade(raw upbitTrade, receivedAt time.Time) (quanttick.TradeEvent, error) {
	symbol, timestamp := raw.Market, raw.Timestamp
	if raw.Type == "trade" {
		symbol, timestamp = raw.Code, raw.TradeTimestamp
	}
	if !slices.Contains(u.Symbols, symbol) {
		return quanttick.TradeEvent{}, fmt.Errorf("unconfigured Upbit market %s", symbol)
	}
	side := ""
	switch raw.Side {
	case "BID":
		side = "buy"
	case "ASK":
		side = "sell"
	}
	// Price and quote volume retain the market's quote currency, e.g. KRW-BTC.
	return parseMarketTrade(UpbitName, symbol, raw.ID.String(), strconv.FormatInt(timestamp, 10), raw.Price.String(), raw.Size.String(), side, receivedAt)
}

func (u *Upbit) parseMessage(data []byte, receivedAt time.Time) (marketMessage, error) {
	var raw upbitTrade
	if err := json.Unmarshal(data, &raw); err != nil {
		return marketMessage{}, err
	}
	if raw.Error != nil {
		return marketMessage{}, fmt.Errorf("Upbit websocket %s: %s", raw.Error.Name, raw.Error.Message)
	}
	if raw.Type != "trade" || raw.StreamType == "SNAPSHOT" {
		return marketMessage{}, nil
	}
	trade, err := u.parseTrade(raw, receivedAt)
	if err != nil {
		return marketMessage{}, err
	}
	return marketMessage{trades: []quanttick.TradeEvent{trade}}, nil
}

func (u *Upbit) recoverTrades(ctx context.Context, symbol, _ string) ([]quanttick.TradeEvent, error) {
	anchor := u.last[symbol]
	day := time.Now().UTC().Truncate(24 * time.Hour)
	anchorDay := anchor.Timestamp.UTC().Truncate(24 * time.Hour)
	return recoverMarketTrades(ctx, anchor, func(ctx context.Context, cursor string) ([]quanttick.TradeEvent, string, error) {
		for !day.Before(anchorDay) {
			daysAgo := int(time.Now().UTC().Truncate(24*time.Hour).Sub(day) / (24 * time.Hour))
			if daysAgo < 0 || daysAgo > 7 {
				return nil, "", fmt.Errorf("Upbit recovery exceeds seven-day history")
			}
			params := url.Values{"market": {symbol}, "count": {"500"}, "days_ago": {strconv.Itoa(daysAgo)}}
			if cursor != "" {
				params.Set("cursor", cursor)
			}
			var page []upbitTrade
			if err := u.get(ctx, "/trades/ticks", params, &page); err != nil {
				return nil, "", err
			}
			if len(page) == 0 {
				day = day.AddDate(0, 0, -1)
				cursor = ""
				continue
			}
			trades := make([]quanttick.TradeEvent, 0, len(page))
			for _, raw := range page {
				trade, err := u.parseTrade(raw, time.Now().UTC())
				if err != nil {
					return nil, "", err
				}
				trades = append(trades, trade)
			}
			return trades, page[len(page)-1].ID.String(), nil
		}
		return nil, "", nil
	})
}
