package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BinanceName                    = "binance"
	BinanceFuturesName             = "binance-futures"
	BinanceURL                     = "wss://stream.binance.com:9443/ws"
	BinanceFuturesURL              = "wss://fstream.binance.com/market/stream"
	binanceSubscriptionBufferLimit = 10000
	binanceSubscriptionRequestID   = 1
)

var (
	_                        quanttick.Exchange = (*Binance)(nil)
	errBinanceServerShutdown                    = errors.New("binance server shutdown")
)

type Binance struct {
	Symbols             []string
	name                string
	URL                 string
	stream              string
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration

	lastIDs map[string]int64
}

type binanceParsedTrade struct {
	event   quanttick.TradeEvent
	tradeID int64
}

type BinanceOption func(*Binance)

func NewBinance(symbols []string, options ...BinanceOption) *Binance {
	exchange := &Binance{
		Symbols:             append([]string(nil), symbols...),
		name:                BinanceName,
		URL:                 BinanceURL,
		stream:              "trade",
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		lastIDs:             make(map[string]int64),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func NewBinanceFutures(symbols []string, options ...BinanceOption) *Binance {
	exchange := &Binance{
		Symbols:             append([]string(nil), symbols...),
		name:                BinanceFuturesName,
		URL:                 BinanceFuturesURL,
		stream:              "aggTrade",
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		lastIDs:             make(map[string]int64),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBinanceURL(url string) BinanceOption {
	return func(exchange *Binance) {
		exchange.URL = url
	}
}

func WithBinanceReconnectDelay(delay time.Duration) BinanceOption {
	return func(exchange *Binance) {
		exchange.ReconnectDelay = delay
	}
}

func WithBinanceSubscriptionTimeout(timeout time.Duration) BinanceOption {
	return func(exchange *Binance) {
		exchange.SubscriptionTimeout = timeout
	}
}

func (b *Binance) Name() string {
	return b.name
}

func (b *Binance) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(b.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := b.run(ctx, trades); err != nil {
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, errBinanceServerShutdown) {
					continue
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

func (b *Binance) SubscriptionMessages() []map[string]any {
	params := make([]string, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		params = append(params, strings.ToLower(symbol)+"@"+b.stream)
	}

	return []map[string]any{
		{
			"method": "SUBSCRIBE",
			"params": params,
			"id":     binanceSubscriptionRequestID,
		},
	}
}

func (b *Binance) ParseTradeMessage(data []byte, receivedAt time.Time) (quanttick.TradeEvent, bool, error) {
	parsed, ok, err := b.parseTradeMessage(data, receivedAt, b.lastIDs)
	return parsed.event, ok, err
}

func (b *Binance) parseTradeMessage(
	data []byte,
	receivedAt time.Time,
	lastIDs map[string]int64,
) (binanceParsedTrade, bool, error) {
	payload, combinedStream, err := b.unwrapTradeMessage(data)
	if err != nil {
		return binanceParsedTrade{}, false, err
	}
	eventType, ok, err := binanceEventType(payload)
	if err != nil {
		return binanceParsedTrade{}, false, err
	}
	if !ok || eventType != b.stream {
		return binanceParsedTrade{}, false, nil
	}

	var msg binanceTradeMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance message: %w", err)
	}
	if combinedStream != "" {
		expectedStream := strings.ToLower(msg.Symbol) + "@" + b.stream
		if combinedStream != expectedStream {
			return binanceParsedTrade{}, false, fmt.Errorf(
				"binance combined stream %q does not match trade %q",
				combinedStream,
				expectedStream,
			)
		}
	}

	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Quantity)
	if err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance quantity: %w", err)
	}
	buyerIsMaker, err := binanceBuyerIsMaker(payload)
	if err != nil {
		return binanceParsedTrade{}, false, err
	}

	tradeID := msg.TradeID
	ticks := 1
	if b.stream == "aggTrade" {
		tradeID = msg.AggregateTradeID
		ticks = int(msg.LastTradeID - msg.FirstTradeID + 1)
	}
	prevID, hadPrevID := lastIDs[msg.Symbol]
	lastIDs[msg.Symbol] = tradeID
	tickRule := 1
	if buyerIsMaker {
		tickRule = -1
	}

	event := quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     b.name,
		UID:          strconv.FormatInt(tradeID, 10),
		Symbol:       msg.Symbol,
		Timestamp:    time.UnixMilli(msg.TradeTime).UTC(),
		ReceivedAt:   receivedAt,
		Price:        price,
		Notional:     notional,
		TickRule:     tickRule,
		Ticks:        ticks,
		IsSequential: hadPrevID && tradeID == prevID+1,
	})
	return binanceParsedTrade{event: event, tradeID: tradeID}, true, nil
}

func (b *Binance) unwrapTradeMessage(data []byte) ([]byte, string, error) {
	if b.name != BinanceFuturesName {
		return data, "", nil
	}

	var envelope binanceCombinedStreamMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, "", fmt.Errorf("parse binance message: %w", err)
	}
	if envelope.Stream == "" {
		return data, "", nil
	}
	if len(envelope.Data) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Data), []byte("null")) {
		return nil, "", fmt.Errorf("binance combined stream %q has no data", envelope.Stream)
	}
	return envelope.Data, envelope.Stream, nil
}

func (b *Binance) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, b.URL, nil)
	if err != nil {
		return fmt.Errorf("dial binance websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal binance subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send binance subscription: %w", err)
		}
	}

	sequenceIDs := cloneBinanceLastIDs(b.lastIDs)
	buffered, err := b.awaitSubscription(ctx, conn, sequenceIDs)
	if err != nil {
		return err
	}
	for _, parsed := range buffered {
		select {
		case trades <- parsed.event:
			b.lastIDs[parsed.event.Symbol] = parsed.tradeID
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
			return fmt.Errorf("read binance websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		isAck, err := parseBinanceSubscriptionResponse(data)
		if err != nil {
			return err
		}
		if isAck {
			continue
		}

		parsed, ok, err := b.parseTradeMessage(data, time.Now().UTC(), sequenceIDs)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		select {
		case trades <- parsed.event:
			b.lastIDs[parsed.event.Symbol] = parsed.tradeID
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *Binance) awaitSubscription(
	ctx context.Context,
	conn *websocket.Conn,
	sequenceIDs map[string]int64,
) ([]binanceParsedTrade, error) {
	ackCtx, cancel := context.WithTimeout(ctx, b.SubscriptionTimeout)
	defer cancel()

	buffered := make([]binanceParsedTrade, 0)
	for {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("binance subscription acknowledgement timed out after %s", b.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read binance subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		isAck, err := parseBinanceSubscriptionResponse(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			return buffered, nil
		}

		parsed, ok, err := b.parseTradeMessage(data, time.Now().UTC(), sequenceIDs)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if len(buffered) >= binanceSubscriptionBufferLimit {
			return nil, fmt.Errorf("binance subscription trade buffer exceeded %d events", binanceSubscriptionBufferLimit)
		}
		buffered = append(buffered, parsed)
	}
}

func cloneBinanceLastIDs(lastIDs map[string]int64) map[string]int64 {
	cloned := make(map[string]int64, len(lastIDs))
	for symbol, tradeID := range lastIDs {
		cloned[symbol] = tradeID
	}
	return cloned
}

func parseBinanceSubscriptionResponse(data []byte) (bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("parse binance message: %w", err)
	}

	if rawEvent, ok := envelope["e"]; ok && bytes.Equal(bytes.TrimSpace(rawEvent), []byte(`"serverShutdown"`)) {
		return false, errBinanceServerShutdown
	}

	if rawCode, ok := envelope["code"]; ok {
		var code int
		if err := json.Unmarshal(rawCode, &code); err != nil {
			return false, fmt.Errorf("parse binance websocket error code: %w", err)
		}
		var message string
		if rawMessage, ok := envelope["msg"]; ok {
			_ = json.Unmarshal(rawMessage, &message)
		}
		return false, fmt.Errorf("binance websocket error %d: %s", code, message)
	}

	rawResult, hasResult := envelope["result"]
	rawID, hasID := envelope["id"]
	if !hasResult && !hasID {
		return false, nil
	}
	if !hasResult || !hasID {
		return false, fmt.Errorf("invalid binance subscription response: %s", string(data))
	}

	var id int64
	if err := json.Unmarshal(rawID, &id); err != nil {
		return false, fmt.Errorf("parse binance subscription response id: %w", err)
	}
	if id != binanceSubscriptionRequestID {
		return false, fmt.Errorf("binance subscription response has unexpected id %d", id)
	}
	if string(bytes.TrimSpace(rawResult)) != "null" {
		return false, fmt.Errorf("binance subscription request failed: %s", string(data))
	}
	return true, nil
}

type binanceCombinedStreamMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type binanceTradeMessage struct {
	Symbol           string `json:"s"`
	TradeID          int64  `json:"t"`
	AggregateTradeID int64  `json:"a"`
	FirstTradeID     int64  `json:"f"`
	LastTradeID      int64  `json:"l"`
	TradeTime        int64  `json:"T"`
	Price            string `json:"p"`
	Quantity         string `json:"q"`
}

func binanceBuyerIsMaker(data []byte) (bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("parse binance buyer maker envelope: %w", err)
	}
	raw, ok := envelope["m"]
	if !ok {
		return false, fmt.Errorf("parse binance buyer maker: missing m")
	}

	// encoding/json matches field tags case-insensitively, so read exact "m"
	// to avoid Binance's "M" flag overwriting buyer-is-maker.
	var buyerIsMaker bool
	if err := json.Unmarshal(raw, &buyerIsMaker); err != nil {
		return false, fmt.Errorf("parse binance buyer maker: %w", err)
	}
	return buyerIsMaker, nil
}

func binanceEventType(data []byte) (string, bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", false, fmt.Errorf("parse binance envelope: %w", err)
	}
	eventRaw, ok := envelope["e"]
	if !ok || len(eventRaw) == 0 {
		return "", false, nil
	}

	var eventType string
	if err := json.Unmarshal(eventRaw, &eventType); err != nil {
		return "", false, nil
	}
	return eventType, true, nil
}

func sendError(ctx context.Context, errs chan<- error, err error) {
	select {
	case errs <- err:
	case <-ctx.Done():
	default:
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
