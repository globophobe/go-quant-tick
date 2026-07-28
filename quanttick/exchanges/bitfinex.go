package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BitfinexName                    = "bitfinex"
	BitfinexURL                     = "wss://api-pub.bitfinex.com/ws/2"
	BitfinexRESTURL                 = "https://api-pub.bitfinex.com/v2"
	bitfinexSubscriptionBufferLimit = 10000
	bitfinexRecoveryPageLimit       = 10000
	bitfinexRecoveryMaxPages        = 100
	bitfinexRecoveryRequestLimit    = 15
	bitfinexRecoveryRequestInterval = time.Minute / bitfinexRecoveryRequestLimit
	bitfinexRecoveryTimeout         = (bitfinexRecoveryRequestLimit-1)*bitfinexRecoveryRequestInterval + 10*time.Second
)

var _ quanttick.Exchange = (*Bitfinex)(nil)

type bitfinexLifecycleEvent struct {
	code    int64
	message string
}

func (e *bitfinexLifecycleEvent) Error() string {
	return fmt.Sprintf("bitfinex lifecycle event %d: %s", e.code, e.message)
}

type Bitfinex struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	RecoveryTimeout     time.Duration
	recoveryThrottle    *restThrottle

	channelSymbols    map[int64]string
	subscribedSymbols map[string]struct{}
	lastIDs           map[string]int64
	pendingOrder      map[string][]int64
	pendingOrderID    map[string]map[int64]struct{}
	pendingUpdates    map[string]map[int64]bitfinexTradeUpdate
	recoveryGaps      map[string]bool
}

type BitfinexOption func(*Bitfinex)

func NewBitfinex(symbols []string, options ...BitfinexOption) *Bitfinex {
	exchange := &Bitfinex{
		Symbols:             append([]string(nil), symbols...),
		URL:                 BitfinexURL,
		RESTURL:             BitfinexRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		RecoveryTimeout:     bitfinexRecoveryTimeout,
		recoveryThrottle:    newRESTThrottle(bitfinexRecoveryRequestInterval),
		channelSymbols:      make(map[int64]string),
		subscribedSymbols:   make(map[string]struct{}),
		lastIDs:             make(map[string]int64),
		pendingOrder:        make(map[string][]int64),
		pendingOrderID:      make(map[string]map[int64]struct{}),
		pendingUpdates:      make(map[string]map[int64]bitfinexTradeUpdate),
		recoveryGaps:        make(map[string]bool),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBitfinexURL(url string) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.URL = url
	}
}

func WithBitfinexRESTURL(url string) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.RESTURL = url
	}
}

func WithBitfinexHTTPClient(client *http.Client) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.HTTPClient = client
	}
}

func WithBitfinexReconnectDelay(delay time.Duration) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.ReconnectDelay = delay
	}
}

func WithBitfinexSubscriptionTimeout(timeout time.Duration) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.SubscriptionTimeout = timeout
	}
}

func WithBitfinexRecoveryTimeout(timeout time.Duration) BitfinexOption {
	return func(exchange *Bitfinex) {
		exchange.RecoveryTimeout = timeout
	}
}

func (b *Bitfinex) Name() string {
	return BitfinexName
}

func (b *Bitfinex) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(b.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := b.run(ctx, trades, errs); err != nil {
				if ctx.Err() != nil {
					return
				}
				var lifecycle *bitfinexLifecycleEvent
				if errors.As(err, &lifecycle) {
					if lifecycle.code == 20051 || lifecycle.code == 20061 {
						continue
					}
				} else {
					sendError(ctx, errs, err)
				}
			}

			if err := sleepContext(ctx, backoff.Next(time.Since(startedAt))); err != nil {
				return
			}
		}
	}()

	return trades, errs
}

func (b *Bitfinex) SubscriptionMessages() []map[string]any {
	messages := make([]map[string]any, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		messages = append(messages, map[string]any{
			"event":   "subscribe",
			"channel": "trades",
			"symbol":  b.APISymbol(symbol),
		})
	}
	return messages
}

func (b *Bitfinex) APISymbol(symbol string) string {
	if strings.HasPrefix(symbol, "t") {
		return symbol
	}
	return "t" + symbol
}

func (b *Bitfinex) resetSessionState() {
	b.channelSymbols = make(map[int64]string)
	b.subscribedSymbols = make(map[string]struct{})
	b.pendingOrder = make(map[string][]int64)
	b.pendingOrderID = make(map[string]map[int64]struct{})
	b.pendingUpdates = make(map[string]map[int64]bitfinexTradeUpdate)
	b.recoveryGaps = make(map[string]bool)
}

func (b *Bitfinex) requestedSymbol(symbol string) bool {
	for _, requested := range b.Symbols {
		if b.APISymbol(requested) == symbol {
			return true
		}
	}
	return false
}

func (b *Bitfinex) subscriptionsReady() bool {
	for _, requested := range b.Symbols {
		if _, ok := b.subscribedSymbols[b.APISymbol(requested)]; !ok {
			return false
		}
	}
	return true
}

func (b *Bitfinex) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}

	switch data[0] {
	case '{':
		return b.parseEventMessage(data)
	case '[':
		return b.parseArrayMessage(data, receivedAt)
	default:
		return nil, nil
	}
}

func (b *Bitfinex) parseEventMessage(data []byte) ([]quanttick.TradeEvent, error) {
	var msg bitfinexEventMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("parse bitfinex event: %w", err)
	}

	switch msg.Event {
	case "subscribed":
		if msg.Channel != "trades" {
			return nil, fmt.Errorf("bitfinex subscribed unexpected channel %q", msg.Channel)
		}
		if !b.requestedSymbol(msg.Symbol) {
			return nil, fmt.Errorf("bitfinex subscribed unexpected symbol %q", msg.Symbol)
		}
		channelID, err := parseRawInt64(msg.ChannelID)
		if err != nil {
			return nil, fmt.Errorf("parse bitfinex subscribed channel id: %w", err)
		}
		if existing := b.channelSymbols[channelID]; existing != "" && existing != msg.Symbol {
			return nil, fmt.Errorf(
				"bitfinex channel %d changed symbol from %q to %q",
				channelID,
				existing,
				msg.Symbol,
			)
		}
		b.channelSymbols[channelID] = msg.Symbol
		b.subscribedSymbols[msg.Symbol] = struct{}{}
	case "error":
		return nil, fmt.Errorf("bitfinex protocol error %d: %s", msg.Code, msg.Message)
	case "info":
		if msg.Platform != nil {
			switch msg.Platform.Status {
			case 1:
			case 0:
				return nil, &bitfinexLifecycleEvent{code: 20060, message: "platform maintenance"}
			default:
				return nil, fmt.Errorf("bitfinex platform unavailable with status %d", msg.Platform.Status)
			}
		}
		switch msg.Code {
		case 0:
		case 20051, 20060, 20061:
			return nil, &bitfinexLifecycleEvent{code: msg.Code, message: msg.Message}
		default:
			return nil, fmt.Errorf("bitfinex info %d: %s", msg.Code, msg.Message)
		}
	case "unsubscribed":
		return nil, fmt.Errorf("bitfinex channel was unsubscribed")
	}
	return nil, nil
}

func (b *Bitfinex) parseArrayMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var msg []json.RawMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("parse bitfinex array: %w", err)
	}
	if len(msg) < 2 {
		return nil, nil
	}

	channelID, err := parseRawInt64(msg[0])
	if err != nil {
		return nil, fmt.Errorf("parse bitfinex channel id: %w", err)
	}

	symbol := b.channelSymbols[channelID]
	if symbol == "" {
		return nil, nil
	}

	tag, ok, err := parseBitfinexTag(msg[1])
	if err != nil {
		return nil, err
	}
	if !ok || tag == "hb" {
		return nil, nil
	}
	if (tag != "te" && tag != "tu") || len(msg) < 3 {
		return nil, nil
	}

	update, err := parseBitfinexTradeUpdate(msg[2], receivedAt)
	if err != nil {
		return nil, err
	}
	if update.TradeID == 0 {
		return nil, nil
	}

	// Bitfinex sends "te" first and "tu" as the final update. Use "te" only to
	// preserve exchange stream order; emit the final trade from the matching "tu".
	if tag == "te" {
		b.enqueueBitfinexTrade(symbol, update.TradeID)
	} else {
		b.storeBitfinexTradeUpdate(symbol, update)
	}
	return b.emitReadyBitfinexTrades(symbol), nil
}

type bitfinexTradeUpdate struct {
	TradeID         int64
	TimestampMillis int64
	Notional        quanttick.Decimal
	Price           quanttick.Decimal
	TickRule        int
	ReceivedAt      time.Time
}

func parseBitfinexTradeUpdate(raw json.RawMessage, receivedAt time.Time) (bitfinexTradeUpdate, error) {
	var rawTrade []json.RawMessage
	if err := json.Unmarshal(raw, &rawTrade); err != nil {
		return bitfinexTradeUpdate{}, fmt.Errorf("parse bitfinex trade: %w", err)
	}
	if len(rawTrade) < 4 {
		return bitfinexTradeUpdate{}, nil
	}

	tradeID, err := parseRawInt64(rawTrade[0])
	if err != nil {
		return bitfinexTradeUpdate{}, fmt.Errorf("parse bitfinex trade id: %w", err)
	}
	timestampMillis, err := parseRawInt64(rawTrade[1])
	if err != nil {
		return bitfinexTradeUpdate{}, fmt.Errorf("parse bitfinex timestamp: %w", err)
	}
	notional, err := parseRawDecimal(rawTrade[2])
	if err != nil {
		return bitfinexTradeUpdate{}, fmt.Errorf("parse bitfinex notional: %w", err)
	}
	price, err := parseRawDecimal(rawTrade[3])
	if err != nil {
		return bitfinexTradeUpdate{}, fmt.Errorf("parse bitfinex price: %w", err)
	}

	tickRule := 1
	if notional.IsNegative() {
		tickRule = -1
		notional = notional.Abs()
	}
	return bitfinexTradeUpdate{
		TradeID:         tradeID,
		TimestampMillis: timestampMillis,
		Notional:        notional,
		Price:           price,
		TickRule:        tickRule,
		ReceivedAt:      receivedAt,
	}, nil
}

func (b *Bitfinex) ensureBitfinexTradeState(symbol string) {
	if b.pendingOrderID[symbol] == nil {
		b.pendingOrderID[symbol] = make(map[int64]struct{})
	}
	if b.pendingUpdates[symbol] == nil {
		b.pendingUpdates[symbol] = make(map[int64]bitfinexTradeUpdate)
	}
}

func (b *Bitfinex) enqueueBitfinexTrade(symbol string, tradeID int64) {
	b.ensureBitfinexTradeState(symbol)
	if lastID, ok := b.lastIDs[symbol]; ok && tradeID <= lastID {
		return
	}
	if _, ok := b.pendingOrderID[symbol][tradeID]; ok {
		return
	}
	b.pendingOrder[symbol] = append(b.pendingOrder[symbol], tradeID)
	b.pendingOrderID[symbol][tradeID] = struct{}{}
}

func (b *Bitfinex) storeBitfinexTradeUpdate(symbol string, update bitfinexTradeUpdate) {
	b.ensureBitfinexTradeState(symbol)
	if lastID, ok := b.lastIDs[symbol]; ok && update.TradeID <= lastID {
		return
	}
	b.pendingUpdates[symbol][update.TradeID] = update
}

func (b *Bitfinex) emitReadyBitfinexTrades(symbol string) []quanttick.TradeEvent {
	b.ensureBitfinexTradeState(symbol)
	order := b.pendingOrder[symbol]
	trades := make([]quanttick.TradeEvent, 0)
	for len(order) > 0 {
		tradeID := order[0]
		update, ok := b.pendingUpdates[symbol][tradeID]
		if !ok {
			break
		}
		order = order[1:]
		delete(b.pendingOrderID[symbol], tradeID)
		delete(b.pendingUpdates[symbol], tradeID)

		prevID, hadPrevID := b.lastIDs[symbol]
		b.lastIDs[symbol] = tradeID
		trades = append(trades, newBitfinexTradeEvent(
			symbol,
			update,
			b.bitfinexTradeIsSequential(symbol, hadPrevID, prevID, tradeID),
		))
	}
	b.pendingOrder[symbol] = order
	return trades
}

func (b *Bitfinex) bitfinexTradeIsSequential(
	symbol string,
	hadPrevious bool,
	previousID int64,
	tradeID int64,
) bool {
	if b.recoveryGaps[symbol] {
		delete(b.recoveryGaps, symbol)
		return false
	}
	return hadPrevious && tradeID > previousID
}

func newBitfinexTradeEvent(symbol string, update bitfinexTradeUpdate, isSequential bool) quanttick.TradeEvent {
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     BitfinexName,
		UID:          strconv.FormatInt(update.TradeID, 10),
		Symbol:       symbol,
		Timestamp:    time.UnixMilli(update.TimestampMillis).UTC(),
		ReceivedAt:   update.ReceivedAt,
		Price:        update.Price,
		Notional:     update.Notional,
		TickRule:     update.TickRule,
		IsSequential: isSequential,
	})
}

func (b *Bitfinex) run(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	errs chan<- error,
) error {
	b.resetSessionState()
	conn, err := dialWebSocket(ctx, BitfinexName, b.URL)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal bitfinex subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send bitfinex subscription: %w", err)
		}
	}

	buffered, err := b.awaitSubscriptions(ctx, conn)
	if err != nil {
		return err
	}
	backlog, err := newTradeBacklog(bitfinexSubscriptionBufferLimit, len(buffered))
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, streamErr := b.startMessageReader(streamCtx, conn, backlog)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, b.RecoveryTimeout)
	recovered, recoveryErr := b.recoverTrades(recoveryCtx)
	cancelRecovery()
	for _, trade := range recovered {
		if err := b.emitRecoveredTrade(ctx, trades, trade); err != nil {
			return err
		}
	}
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, message := range buffered {
		err := b.emitMessageTrades(ctx, trades, message)
		backlog.release()
		if err != nil {
			return err
		}
	}
	for message := range stream {
		err := b.emitMessageTrades(ctx, trades, message)
		backlog.release()
		if err != nil {
			return err
		}
	}
	cancelStream()
	err = <-streamErr
	if isNormalWebSocketClose(err) || err == nil {
		return nil
	}
	return err
}

type bitfinexEventMessage struct {
	Event     string            `json:"event"`
	Channel   string            `json:"channel"`
	ChannelID json.RawMessage   `json:"chanId"`
	Symbol    string            `json:"symbol"`
	Code      int64             `json:"code"`
	Message   string            `json:"msg"`
	Platform  *bitfinexPlatform `json:"platform"`
}

type bitfinexPlatform struct {
	Status int `json:"status"`
}

func parseBitfinexTag(raw json.RawMessage) (string, bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return "", false, nil
	}
	if raw[0] == '[' {
		return "", false, nil
	}

	var tag string
	if err := json.Unmarshal(raw, &tag); err != nil {
		return "", false, fmt.Errorf("parse bitfinex tag: %w", err)
	}
	return tag, true, nil
}

func parseRawInt64(raw json.RawMessage) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing integer value")
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return 0, err
		}
		return strconv.ParseInt(value, 10, 64)
	}
	return strconv.ParseInt(string(raw), 10, 64)
}
