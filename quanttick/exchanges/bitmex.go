package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BitmexName                    = "bitmex"
	BitmexURL                     = "wss://ws.bitmex.com/realtime"
	bitmexSeenTradeLimit          = 10000
	bitmexSubscriptionBufferLimit = 10000
)

var _ quanttick.Exchange = (*Bitmex)(nil)

type Bitmex struct {
	Symbols             []string
	URL                 string
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
}

type BitmexOption func(*Bitmex)

func NewBitmex(symbols []string, options ...BitmexOption) *Bitmex {
	exchange := &Bitmex{
		Symbols:             append([]string(nil), symbols...),
		URL:                 BitmexURL,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBitmexURL(url string) BitmexOption {
	return func(exchange *Bitmex) {
		exchange.URL = url
	}
}

func WithBitmexReconnectDelay(delay time.Duration) BitmexOption {
	return func(exchange *Bitmex) {
		exchange.ReconnectDelay = delay
	}
}

func WithBitmexSubscriptionTimeout(timeout time.Duration) BitmexOption {
	return func(exchange *Bitmex) {
		exchange.SubscriptionTimeout = timeout
	}
}

func (b *Bitmex) Name() string {
	return BitmexName
}

func (b *Bitmex) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)
	seen := newBitmexSeenTrades(bitmexSeenTradeLimit)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(b.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := b.run(ctx, trades, seen); err != nil {
				if ctx.Err() != nil {
					return
				}
				sendError(ctx, errs, err)
			}

			if err := sleepContext(ctx, backoff.Next(time.Since(startedAt))); err != nil {
				return
			}
		}
	}()

	return trades, errs
}

func (b *Bitmex) SubscriptionMessages() []map[string]any {
	args := make([]string, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		args = append(args, "trade:"+symbol)
	}

	return []map[string]any{
		{
			"op":   "subscribe",
			"args": args,
		},
	}
}

func (b *Bitmex) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var msg bitmexTradeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("parse bitmex message: %w", err)
	}
	return bitmexTradesFromMessage(msg, receivedAt)
}

func bitmexTradesFromMessage(msg bitmexTradeMessage, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	if msg.Table != "trade" || (msg.Action != "insert" && msg.Action != "partial") {
		return nil, nil
	}

	trades := make([]quanttick.TradeEvent, 0, len(msg.Data))
	for _, rawTrade := range msg.Data {
		price, err := parseRawDecimal(rawTrade.Price)
		if err != nil {
			return nil, fmt.Errorf("parse bitmex price: %w", err)
		}

		volume, notional, err := parseBitmexVolumeAndNotional(rawTrade, price)
		if err != nil {
			return nil, err
		}

		timestamp, err := time.Parse(time.RFC3339Nano, rawTrade.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse bitmex timestamp: %w", err)
		}
		timestamp, nanoseconds := splitEventTimestamp(timestamp)

		tickRule := -1
		if strings.ToLower(rawTrade.Side) == "buy" {
			tickRule = 1
		}

		trades = append(trades, quanttick.NewTradeEvent(quanttick.TradeEventInput{
			Exchange:    BitmexName,
			UID:         rawTrade.TradeMatchID,
			Symbol:      rawTrade.Symbol,
			Timestamp:   timestamp,
			Nanoseconds: nanoseconds,
			ReceivedAt:  receivedAt,
			Price:       price,
			Volume:      &volume,
			Notional:    notional,
			TickRule:    tickRule,
		}))
	}

	if msg.Action == "partial" {
		sort.SliceStable(trades, func(i, j int) bool {
			if trades[i].Timestamp.Equal(trades[j].Timestamp) {
				return trades[i].Nanoseconds < trades[j].Nanoseconds
			}
			return trades[i].Timestamp.Before(trades[j].Timestamp)
		})
	}
	return trades, nil
}

func (b *Bitmex) run(ctx context.Context, trades chan<- quanttick.TradeEvent, seen *bitmexSeenTrades) error {
	conn, err := dialWebSocket(ctx, BitmexName, b.URL)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal bitmex subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send bitmex subscription: %w", err)
		}
	}

	buffered, err := b.awaitSubscriptions(ctx, conn)
	if err != nil {
		return err
	}
	for _, trade := range buffered {
		if !seen.Add(trade) {
			continue
		}
		select {
		case trades <- trade:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			if isNormalWebSocketClose(err) {
				return nil
			}
			return fmt.Errorf("read bitmex websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		_, isAck, err := parseBitmexSubscriptionResponse(data)
		if err != nil {
			return err
		}
		if isAck {
			continue
		}

		parsedTrades, err := b.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return err
		}
		for _, trade := range parsedTrades {
			if !seen.Add(trade) {
				continue
			}
			select {
			case trades <- trade:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (b *Bitmex) awaitSubscriptions(ctx context.Context, conn *websocket.Conn) ([]quanttick.TradeEvent, error) {
	expectedTopics := make(map[string]string, len(b.Symbols))
	expectedSymbols := make(map[string]struct{}, len(b.Symbols))
	for _, symbol := range b.Symbols {
		expectedTopics["trade:"+symbol] = symbol
		expectedSymbols[symbol] = struct{}{}
	}
	if len(expectedSymbols) == 0 {
		return nil, nil
	}

	ackCtx, cancel := context.WithTimeout(ctx, b.SubscriptionTimeout)
	defer cancel()

	acked := make(map[string]struct{}, len(expectedSymbols))
	partials := make(map[string]struct{}, len(expectedSymbols))
	buffered := make([]quanttick.TradeEvent, 0)
	for len(acked) < len(expectedSymbols) || len(partials) < len(expectedSymbols) {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf(
					"bitmex subscription readiness timed out after %s (missing acknowledgements: %s; missing partial snapshots: %s)",
					b.SubscriptionTimeout,
					strings.Join(missingBitmexSymbols(expectedSymbols, acked), ","),
					strings.Join(missingBitmexSymbols(expectedSymbols, partials), ","),
				)
			}
			return nil, fmt.Errorf("read bitmex subscription readiness: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		topic, isAck, err := parseBitmexSubscriptionResponse(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			symbol, ok := expectedTopics[topic]
			if !ok {
				return nil, fmt.Errorf("bitmex acknowledged unexpected subscription %q", topic)
			}
			acked[symbol] = struct{}{}
			continue
		}

		var msg bitmexTradeMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, fmt.Errorf("parse bitmex message: %w", err)
		}
		if msg.Table != "trade" {
			continue
		}

		var parsedTrades []quanttick.TradeEvent
		switch msg.Action {
		case "partial":
			partialSymbols, err := bitmexPartialSymbols(msg, expectedSymbols, partials)
			if err != nil {
				return nil, err
			}
			parsedTrades, err = bitmexTradesFromMessage(msg, time.Now().UTC())
			if err != nil {
				return nil, err
			}
			for _, symbol := range partialSymbols {
				partials[symbol] = struct{}{}
			}
		case "insert":
			readyData := make([]bitmexTradeEntry, 0, len(msg.Data))
			for _, trade := range msg.Data {
				if _, ok := expectedSymbols[trade.Symbol]; !ok {
					return nil, fmt.Errorf("bitmex received trade for unexpected symbol %q", trade.Symbol)
				}
				if _, ready := partials[trade.Symbol]; ready {
					readyData = append(readyData, trade)
				}
			}
			if len(readyData) == 0 {
				continue
			}
			msg.Data = readyData
			parsedTrades, err = bitmexTradesFromMessage(msg, time.Now().UTC())
			if err != nil {
				return nil, err
			}
		default:
			continue
		}
		if len(buffered)+len(parsedTrades) > bitmexSubscriptionBufferLimit {
			return nil, fmt.Errorf("bitmex subscription trade buffer exceeded %d events", bitmexSubscriptionBufferLimit)
		}
		buffered = append(buffered, parsedTrades...)
	}
	return buffered, nil
}

func bitmexPartialSymbols(
	msg bitmexTradeMessage,
	expected map[string]struct{},
	ready map[string]struct{},
) ([]string, error) {
	symbols := make(map[string]struct{})
	if msg.Filter.Symbol != "" {
		symbols[msg.Filter.Symbol] = struct{}{}
	}
	for _, trade := range msg.Data {
		if trade.Symbol == "" {
			return nil, fmt.Errorf("bitmex partial snapshot contains an empty symbol")
		}
		if msg.Filter.Symbol != "" && trade.Symbol != msg.Filter.Symbol {
			return nil, fmt.Errorf(
				"bitmex partial snapshot filter %q contains symbol %q",
				msg.Filter.Symbol,
				trade.Symbol,
			)
		}
		symbols[trade.Symbol] = struct{}{}
	}

	if len(symbols) == 0 {
		for symbol := range expected {
			if _, ok := ready[symbol]; !ok {
				symbols[symbol] = struct{}{}
			}
		}
		if len(symbols) != 1 {
			return nil, fmt.Errorf("bitmex empty partial snapshot is missing a symbol filter")
		}
	}

	result := make([]string, 0, len(symbols))
	for symbol := range symbols {
		if _, ok := expected[symbol]; !ok {
			return nil, fmt.Errorf("bitmex received partial snapshot for unexpected symbol %q", symbol)
		}
		result = append(result, symbol)
	}
	sort.Strings(result)
	return result, nil
}

func missingBitmexSymbols(expected, actual map[string]struct{}) []string {
	missing := make([]string, 0, len(expected)-len(actual))
	for symbol := range expected {
		if _, ok := actual[symbol]; !ok {
			missing = append(missing, symbol)
		}
	}
	sort.Strings(missing)
	return missing
}

func parseBitmexSubscriptionResponse(data []byte) (string, bool, error) {
	var response bitmexSubscriptionResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return "", false, fmt.Errorf("parse bitmex message: %w", err)
	}
	if response.Error != "" {
		if response.Status != 0 {
			return "", false, fmt.Errorf("bitmex websocket error %d: %s", response.Status, response.Error)
		}
		return "", false, fmt.Errorf("bitmex websocket error: %s", response.Error)
	}
	if response.Success == nil {
		return "", false, nil
	}
	if !*response.Success || response.Subscribe == "" {
		return "", false, fmt.Errorf("invalid bitmex subscription response: %s", string(data))
	}
	return response.Subscribe, true, nil
}

type bitmexSubscriptionResponse struct {
	Success   *bool  `json:"success"`
	Subscribe string `json:"subscribe"`
	Error     string `json:"error"`
	Status    int    `json:"status"`
}

type bitmexTradeMessage struct {
	Table  string `json:"table"`
	Action string `json:"action"`
	Filter struct {
		Symbol string `json:"symbol"`
	} `json:"filter"`
	Data []bitmexTradeEntry `json:"data"`
}

type bitmexTradeEntry struct {
	TradeMatchID    string          `json:"trdMatchID"`
	Symbol          string          `json:"symbol"`
	Timestamp       string          `json:"timestamp"`
	Side            string          `json:"side"`
	Price           json.RawMessage `json:"price"`
	HomeNotional    json.RawMessage `json:"homeNotional"`
	ForeignNotional json.RawMessage `json:"foreignNotional"`
}

type bitmexSeenTrades struct {
	limit int
	seen  map[string]struct{}
	order []string
}

func newBitmexSeenTrades(limit int) *bitmexSeenTrades {
	return &bitmexSeenTrades{
		limit: limit,
		seen:  make(map[string]struct{}),
	}
}

func (s *bitmexSeenTrades) Add(trade quanttick.TradeEvent) bool {
	key := trade.Symbol + "|" + trade.UID
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = struct{}{}
	s.order = append(s.order, key)
	for s.limit > 0 && len(s.order) > s.limit {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.seen, oldest)
	}
	return true
}

func parseBitmexVolumeAndNotional(rawTrade bitmexTradeEntry, price quanttick.Decimal) (quanttick.Decimal, quanttick.Decimal, error) {
	if !price.IsPositive() {
		return quanttick.Decimal{}, quanttick.Decimal{}, fmt.Errorf("invalid bitmex price %s", price)
	}
	if len(rawTrade.ForeignNotional) != 0 && !bytes.Equal(rawTrade.ForeignNotional, []byte("null")) {
		volume, err := parseRawDecimal(rawTrade.ForeignNotional)
		if err != nil {
			return quanttick.Decimal{}, quanttick.Decimal{}, fmt.Errorf("parse bitmex foreign notional: %w", err)
		}
		return volume, volume.Div(price), nil
	}

	if len(rawTrade.HomeNotional) != 0 && !bytes.Equal(rawTrade.HomeNotional, []byte("null")) {
		notional, err := parseRawDecimal(rawTrade.HomeNotional)
		if err != nil {
			return quanttick.Decimal{}, quanttick.Decimal{}, fmt.Errorf("parse bitmex home notional: %w", err)
		}
		return price.Mul(notional), notional, nil
	}

	return quanttick.Decimal{}, quanttick.Decimal{}, fmt.Errorf("missing bitmex notional")
}

func parseRawDecimal(raw json.RawMessage) (quanttick.Decimal, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return quanttick.Decimal{}, fmt.Errorf("missing decimal value")
	}
	if bytes.Equal(raw, []byte("null")) {
		return quanttick.Decimal{}, fmt.Errorf("null decimal value")
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return quanttick.Decimal{}, err
		}
		return quanttick.ParseDecimal(value)
	}
	return quanttick.ParseDecimal(string(raw))
}
