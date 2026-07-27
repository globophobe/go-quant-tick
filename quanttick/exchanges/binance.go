package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
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
	BinanceFuturesRESTURL          = "https://fapi.binance.com/fapi/v1"
	binanceSubscriptionBufferLimit = 10000
	binanceSubscriptionRequestID   = 1
	binanceRecoveryPageLimit       = 1000
	binanceRecoveryMaxPages        = 100
	binanceRecoveryMaxWeight       = 1200
	binanceRecoveryRetryDelay      = time.Second
)

var (
	_                        quanttick.Exchange = (*Binance)(nil)
	errBinanceServerShutdown                    = errors.New("binance server shutdown")
)

type Binance struct {
	Symbols             []string
	name                string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	stream              string
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	RecoveryMaxWeight   int

	lastIDs          map[string]int64
	recoveryThrottle *restThrottle
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
		HTTPClient:          defaultRecoveryHTTPClient,
		stream:              "trade",
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		RecoveryMaxWeight:   binanceRecoveryMaxWeight,
		lastIDs:             make(map[string]int64),
		recoveryThrottle:    newRESTThrottle(0),
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
		RESTURL:             BinanceFuturesRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		stream:              "aggTrade",
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		RecoveryMaxWeight:   binanceRecoveryMaxWeight,
		lastIDs:             make(map[string]int64),
		recoveryThrottle:    newRESTThrottle(0),
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

func WithBinanceFuturesRESTURL(url string) BinanceOption {
	return func(exchange *Binance) {
		exchange.RESTURL = url
	}
}

func WithBinanceHTTPClient(client *http.Client) BinanceOption {
	return func(exchange *Binance) {
		exchange.HTTPClient = client
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

func WithBinanceRecoveryMaxWeight(maxWeight int) BinanceOption {
	return func(exchange *Binance) {
		exchange.RecoveryMaxWeight = maxWeight
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
			if err := b.run(ctx, trades, errs); err != nil {
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

	buyerIsMaker, err := binanceBuyerIsMaker(payload)
	if err != nil {
		return binanceParsedTrade{}, false, err
	}
	return b.buildParsedTrade(msg, buyerIsMaker, receivedAt, lastIDs)
}

func (b *Binance) buildParsedTrade(
	msg binanceTradeMessage,
	buyerIsMaker bool,
	receivedAt time.Time,
	lastIDs map[string]int64,
) (binanceParsedTrade, bool, error) {
	price, err := quanttick.ParseDecimal(msg.Price)
	if err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(msg.Quantity)
	if err != nil {
		return binanceParsedTrade{}, false, fmt.Errorf("parse binance quantity: %w", err)
	}

	tradeID := msg.TradeID
	ticks := 1
	if b.stream == "aggTrade" {
		tradeID = msg.AggregateTradeID
		ticks = int(msg.LastTradeID - msg.FirstTradeID + 1)
	}
	prevID, hadPrevID := lastIDs[msg.Symbol]
	if !hadPrevID || tradeID > prevID {
		lastIDs[msg.Symbol] = tradeID
	}
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

func (b *Binance) run(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	errs chan<- error,
) error {
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
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	streamSequenceIDs := cloneBinanceLastIDs(sequenceIDs)
	stream, streamErr := b.startTradeReader(streamCtx, conn, streamSequenceIDs)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, recoveryErr := b.recoverFuturesTrades(recoveryCtx)
	cancelRecovery()
	for _, parsed := range recovered {
		if err := b.emitParsedTrade(ctx, trades, sequenceIDs, parsed); err != nil {
			return err
		}
	}
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, parsed := range buffered {
		if err := b.emitParsedTrade(ctx, trades, sequenceIDs, parsed); err != nil {
			cancelStream()
			return err
		}
	}
	for parsed := range stream {
		if err := b.emitParsedTrade(ctx, trades, sequenceIDs, parsed); err != nil {
			cancelStream()
			return err
		}
	}
	cancelStream()
	err = <-streamErr
	if isNormalWebSocketClose(err) {
		return nil
	}
	if err == nil {
		return nil
	}
	return err
}

func (b *Binance) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
	sequenceIDs map[string]int64,
) (<-chan binanceParsedTrade, <-chan error) {
	trades := make(chan binanceParsedTrade, binanceSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read binance websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}

			isAck, err := parseBinanceSubscriptionResponse(data)
			if err != nil {
				errs <- err
				return
			}
			if isAck {
				continue
			}

			parsed, ok, err := b.parseTradeMessage(data, time.Now().UTC(), sequenceIDs)
			if err != nil {
				errs <- err
				return
			}
			if !ok {
				continue
			}
			select {
			case trades <- parsed:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			default:
				errs <- fmt.Errorf(
					"binance websocket trade buffer exceeded %d events",
					binanceSubscriptionBufferLimit,
				)
				_ = conn.CloseNow()
				return
			}
		}
	}()
	return trades, errs
}

func (b *Binance) emitParsedTrade(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	sequenceIDs map[string]int64,
	parsed binanceParsedTrade,
) error {
	previousID, hadPreviousID := b.lastIDs[parsed.event.Symbol]
	if hadPreviousID && parsed.tradeID <= previousID {
		return nil
	}
	parsed.event.IsSequential = hadPreviousID && parsed.tradeID == previousID+1
	if err := sendTrade(ctx, trades, parsed.event); err != nil {
		return err
	}
	b.lastIDs[parsed.event.Symbol] = parsed.tradeID
	sequenceIDs[parsed.event.Symbol] = parsed.tradeID
	return nil
}

func (b *Binance) recoverFuturesTrades(ctx context.Context) ([]binanceParsedTrade, error) {
	if b.name != BinanceFuturesName || len(b.lastIDs) == 0 {
		return nil, nil
	}

	sequenceIDs := cloneBinanceLastIDs(b.lastIDs)
	recovered := make([]binanceParsedTrade, 0)
	var recoveryErrors []error
	for _, symbol := range b.Symbols {
		lastID, ok := b.lastIDs[symbol]
		if !ok {
			continue
		}
		rows, err := b.recoverFuturesSymbol(ctx, symbol, lastID+1, sequenceIDs)
		recovered = append(recovered, rows...)
		if err != nil {
			recoveryErrors = append(recoveryErrors, err)
		}
	}
	sort.SliceStable(recovered, func(left, right int) bool {
		leftTrade := recovered[left]
		rightTrade := recovered[right]
		if !leftTrade.event.Timestamp.Equal(rightTrade.event.Timestamp) {
			return leftTrade.event.Timestamp.Before(rightTrade.event.Timestamp)
		}
		if leftTrade.event.Symbol != rightTrade.event.Symbol {
			return leftTrade.event.Symbol < rightTrade.event.Symbol
		}
		return leftTrade.tradeID < rightTrade.tradeID
	})
	return recovered, errors.Join(recoveryErrors...)
}

func (b *Binance) recoverFuturesSymbol(
	ctx context.Context,
	symbol string,
	fromID int64,
	sequenceIDs map[string]int64,
) ([]binanceParsedTrade, error) {
	recovered := make([]binanceParsedTrade, 0)
	expectedID := fromID
	for page := 0; page < binanceRecoveryMaxPages; page++ {
		endpoint, err := url.Parse(strings.TrimRight(b.RESTURL, "/") + "/aggTrades")
		if err != nil {
			return recovered, fmt.Errorf("build binance futures recovery URL: %w", err)
		}
		query := endpoint.Query()
		query.Set("symbol", symbol)
		query.Set("fromId", strconv.FormatInt(expectedID, 10))
		query.Set("limit", strconv.Itoa(binanceRecoveryPageLimit))
		endpoint.RawQuery = query.Encode()

		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return recovered, fmt.Errorf("build binance futures recovery request: %w", err)
		}
		var response *http.Response
		for {
			if err := b.recoveryThrottle.wait(ctx); err != nil {
				return recovered, fmt.Errorf("wait for binance futures recovery rate limit: %w", err)
			}
			response, err = b.HTTPClient.Do(request)
			if err != nil {
				return recovered, fmt.Errorf("fetch binance futures recovery for %s: %w", symbol, err)
			}
			now := time.Now()
			delay, rateErr := binanceResponseDelay(response.Header, now, b.RecoveryMaxWeight)
			if rateErr != nil {
				response.Body.Close()
				return recovered, fmt.Errorf("fetch binance futures recovery for %s: %w", symbol, rateErr)
			}
			b.recoveryThrottle.deferFor(delay)
			if response.StatusCode != http.StatusTooManyRequests && response.StatusCode != http.StatusTeapot {
				break
			}
			response.Body.Close()
			if delay <= 0 {
				delay = binanceRecoveryRetryDelay
				b.recoveryThrottle.deferFor(delay)
			}
		}
		var payloads []json.RawMessage
		if err := decodeRecoveryResponse(response, &payloads); err != nil {
			return recovered, fmt.Errorf("fetch binance futures recovery for %s: %w", symbol, err)
		}
		if len(payloads) == 0 {
			return recovered, nil
		}

		for _, payload := range payloads {
			parsed, err := b.parseFuturesRecoveryTrade(
				payload,
				symbol,
				time.Now().UTC(),
				sequenceIDs,
			)
			if err != nil {
				return recovered, err
			}
			if parsed.tradeID != expectedID {
				return recovered, fmt.Errorf(
					"binance futures recovery for %s expected aggregate trade %d, got %d",
					symbol,
					expectedID,
					parsed.tradeID,
				)
			}
			recovered = append(recovered, parsed)
			expectedID++
		}
		if len(payloads) < binanceRecoveryPageLimit {
			return recovered, nil
		}
	}
	return recovered, fmt.Errorf(
		"binance futures recovery for %s exceeded %d pages",
		symbol,
		binanceRecoveryMaxPages,
	)
}

func binanceResponseDelay(headers http.Header, now time.Time, maxWeight int) (time.Duration, error) {
	delay, err := retryAfterDelay(headers, now)
	if err != nil {
		return 0, err
	}
	value := strings.TrimSpace(headers.Get("X-MBX-USED-WEIGHT-1M"))
	if value == "" || maxWeight <= 0 {
		return delay, nil
	}
	weight, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse X-MBX-USED-WEIGHT-1M %q: %w", value, err)
	}
	if weight < maxWeight {
		return delay, nil
	}
	minuteDelay := now.Truncate(time.Minute).Add(time.Minute).Sub(now)
	if minuteDelay > delay {
		delay = minuteDelay
	}
	return delay, nil
}

func (b *Binance) parseFuturesRecoveryTrade(
	payload []byte,
	symbol string,
	receivedAt time.Time,
	sequenceIDs map[string]int64,
) (binanceParsedTrade, error) {
	var msg binanceTradeMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return binanceParsedTrade{}, fmt.Errorf("parse binance futures recovery trade: %w", err)
	}
	msg.Symbol = symbol
	buyerIsMaker, err := binanceBuyerIsMaker(payload)
	if err != nil {
		return binanceParsedTrade{}, err
	}
	parsed, _, err := b.buildParsedTrade(msg, buyerIsMaker, receivedAt, sequenceIDs)
	return parsed, err
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
