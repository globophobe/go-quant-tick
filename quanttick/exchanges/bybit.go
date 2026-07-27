package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BybitName                    = "bybit"
	BybitURL                     = "wss://stream.bybit.com/v5/public/linear"
	BybitRESTURL                 = "https://api.bybit.com"
	bybitSubscriptionRequestID   = "trades"
	bybitSeenTradeLimit          = 10000
	bybitSubscriptionBufferLimit = 10000
	bybitHeartbeatInterval       = 20 * time.Second
	bybitRecoveryRetryDelay      = 250 * time.Millisecond
	bybitRecoveryRateLimitDelay  = time.Second
	bybitIPBanDelay              = 10 * time.Minute
	bybitIPRequestLimit          = 600
	bybitIPRequestWindow         = 5 * time.Second
)

var _ quanttick.Exchange = (*Bybit)(nil)

type Bybit struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	HeartbeatInterval   time.Duration
	lastUIDs            map[string]string
	recoveryThrottle    *restThrottle
	recoveryIPLimiter   *requestWindowLimiter
}

type BybitOption func(*Bybit)

func NewBybit(symbols []string, options ...BybitOption) *Bybit {
	exchange := &Bybit{
		Symbols:             append([]string(nil), symbols...),
		URL:                 BybitURL,
		RESTURL:             BybitRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		HeartbeatInterval:   bybitHeartbeatInterval,
		lastUIDs:            make(map[string]string),
		recoveryThrottle:    newRESTThrottle(0),
		recoveryIPLimiter:   newRequestWindowLimiter(bybitIPRequestLimit, bybitIPRequestWindow),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBybitURL(url string) BybitOption {
	return func(exchange *Bybit) {
		exchange.URL = url
	}
}

func WithBybitRESTURL(url string) BybitOption {
	return func(exchange *Bybit) {
		exchange.RESTURL = url
	}
}

func WithBybitHTTPClient(client *http.Client) BybitOption {
	return func(exchange *Bybit) {
		exchange.HTTPClient = client
	}
}

func WithBybitReconnectDelay(delay time.Duration) BybitOption {
	return func(exchange *Bybit) {
		exchange.ReconnectDelay = delay
	}
}

func WithBybitSubscriptionTimeout(timeout time.Duration) BybitOption {
	return func(exchange *Bybit) {
		exchange.SubscriptionTimeout = timeout
	}
}

func WithBybitHeartbeatInterval(interval time.Duration) BybitOption {
	return func(exchange *Bybit) {
		exchange.HeartbeatInterval = interval
	}
}

func (b *Bybit) Name() string {
	return BybitName
}

func (b *Bybit) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)
	seen := newSeenTradeIDs(bybitSeenTradeLimit)

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

func (b *Bybit) SubscriptionMessages() []map[string]any {
	topics := make([]string, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		topics = append(topics, "publicTrade."+symbol)
	}
	return []map[string]any{
		{
			"req_id": bybitSubscriptionRequestID,
			"op":     "subscribe",
			"args":   topics,
		},
	}
}

func (b *Bybit) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var envelope bybitEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse bybit message: %w", err)
	}
	const topicPrefix = "publicTrade."
	if !strings.HasPrefix(envelope.Topic, topicPrefix) {
		return nil, nil
	}
	if envelope.Type != "snapshot" {
		return nil, fmt.Errorf("invalid bybit trade message type %q", envelope.Type)
	}

	topicSymbol := strings.TrimPrefix(envelope.Topic, topicPrefix)
	if !b.hasSymbol(topicSymbol) {
		return nil, fmt.Errorf("bybit trade message has unexpected topic %q", envelope.Topic)
	}

	var rawTrades []bybitTrade
	if err := json.Unmarshal(envelope.Data, &rawTrades); err != nil {
		return nil, fmt.Errorf("parse bybit trades: %w", err)
	}

	trades := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		if rawTrade.Symbol != topicSymbol {
			return nil, fmt.Errorf(
				"bybit trade symbol %q does not match topic %q",
				rawTrade.Symbol,
				envelope.Topic,
			)
		}
		trade, err := parseBybitTrade(rawTrade, receivedAt)
		if err != nil {
			return nil, err
		}
		trades = append(trades, trade)
	}
	return trades, nil
}

func (b *Bybit) run(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	seen *seenTradeIDs,
) error {
	conn, _, err := websocket.Dial(ctx, b.URL, nil)
	if err != nil {
		return fmt.Errorf("dial bybit websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal bybit subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send bybit subscription: %w", err)
		}
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatErr, heartbeatDone := b.startHeartbeats(heartbeatCtx, conn)
	defer func() {
		cancelHeartbeat()
		<-heartbeatDone
	}()

	buffered, err := b.awaitSubscription(ctx, conn)
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	backlog, err := newTradeBacklog(bybitSubscriptionBufferLimit, len(buffered))
	if err != nil {
		return err
	}
	stream, streamErr := b.startTradeReader(streamCtx, conn, backlog)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, buffered, recoveryErr := b.recoverWithWebSocketOverlap(
		recoveryCtx,
		buffered,
		stream,
		streamErr,
	)
	cancelRecovery()
	if recoveryErr != nil {
		cancelStream()
		return recoveryErr
	}
	for _, trade := range recovered {
		if err := emitSeenTrade(ctx, trades, seen, b.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	for _, trade := range buffered {
		err := emitSeenTrade(ctx, trades, seen, b.lastUIDs, trade)
		backlog.release()
		if err != nil {
			cancelStream()
			return err
		}
	}
	for trade := range stream {
		err := emitSeenTrade(ctx, trades, seen, b.lastUIDs, trade)
		backlog.release()
		if err != nil {
			cancelStream()
			return err
		}
	}
	cancelStream()
	if heartbeatErr := takeBybitHeartbeatError(heartbeatErr); heartbeatErr != nil {
		return heartbeatErr
	}
	err = <-streamErr
	if isNormalWebSocketClose(err) {
		return nil
	}
	if err == nil {
		return nil
	}
	return err
}

func (b *Bybit) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
	backlog *tradeBacklog,
) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent, bybitSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read bybit websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}

			isAck, err := parseBybitSubscriptionResponse(data)
			if err != nil {
				errs <- err
				return
			}
			if isAck {
				continue
			}
			parsedTrades, err := b.ParseTradeMessage(data, time.Now().UTC())
			if err != nil {
				errs <- err
				return
			}
			for _, trade := range parsedTrades {
				if !backlog.reserve() {
					errs <- fmt.Errorf(
						"bybit websocket trade backlog exceeded %d events",
						bybitSubscriptionBufferLimit,
					)
					_ = conn.CloseNow()
					return
				}
				select {
				case trades <- trade:
				case <-ctx.Done():
					backlog.release()
					errs <- ctx.Err()
					return
				default:
					backlog.release()
					errs <- fmt.Errorf(
						"bybit websocket trade buffer exceeded %d events",
						bybitSubscriptionBufferLimit,
					)
					_ = conn.CloseNow()
					return
				}
			}
		}
	}()
	return trades, errs
}

func (b *Bybit) recoverWithWebSocketOverlap(
	ctx context.Context,
	buffered []quanttick.TradeEvent,
	stream <-chan quanttick.TradeEvent,
	streamErr <-chan error,
) ([]quanttick.TradeEvent, []quanttick.TradeEvent, error) {
	if len(b.lastUIDs) == 0 {
		return nil, buffered, nil
	}

	handoff := newBybitRecoveryHandoff(buffered)
	pending := make(map[string]struct{})
	for _, symbol := range b.Symbols {
		if _, ok := b.lastUIDs[symbol]; ok {
			pending[symbol] = struct{}{}
		}
	}
	verified := make(map[string][]quanttick.TradeEvent)
	quietCandidates := make(map[string][]quanttick.TradeEvent)
	for len(pending) > 0 {
		for symbol, rows := range quietCandidates {
			if _, ok := pending[symbol]; !ok {
				continue
			}
			if !handoff.hasTrades(symbol) || handoff.overlaps(symbol, rows) {
				verified[symbol] = rows
				delete(pending, symbol)
			}
			delete(quietCandidates, symbol)
		}
		if len(pending) == 0 {
			break
		}

		pass := make(map[string][]quanttick.TradeEvent, len(pending))
		for _, symbol := range b.Symbols {
			if _, ok := pending[symbol]; !ok {
				continue
			}
			rows, err := b.recoverSymbol(ctx, symbol, b.lastUIDs[symbol])
			if err != nil {
				return nil, nil, err
			}
			pass[symbol] = rows
		}

		for {
			select {
			case trade, ok := <-stream:
				if !ok {
					readErr := <-streamErr
					return nil, nil, fmt.Errorf(
						"bybit websocket closed before REST handoff overlap: %w",
						readErr,
					)
				}
				handoff.add(trade)
			default:
				goto streamDrained
			}
		}

	streamDrained:
		for symbol, rows := range pass {
			if handoff.overlaps(symbol, rows) {
				verified[symbol] = rows
				delete(pending, symbol)
				continue
			}
			if !handoff.hasTrades(symbol) {
				quietCandidates[symbol] = rows
			}
		}
		if len(pending) == 0 {
			break
		}
		retry := time.NewTimer(bybitRecoveryRetryDelay)
		for {
			select {
			case trade, ok := <-stream:
				if !ok {
					stopTimer(retry)
					readErr := <-streamErr
					return nil, nil, fmt.Errorf(
						"bybit websocket closed before REST handoff overlap: %w",
						readErr,
					)
				}
				handoff.add(trade)
			case <-retry.C:
				goto retryRecovery
			case <-ctx.Done():
				stopTimer(retry)
				return nil, nil, fmt.Errorf(
					"bybit REST-to-websocket handoff did not overlap: %w",
					ctx.Err(),
				)
			}
		}

	retryRecovery:
	}

	recovered := make([]quanttick.TradeEvent, 0)
	for _, symbol := range b.Symbols {
		recovered = append(recovered, verified[symbol]...)
	}
	sortTradeEventsChronologically(recovered)
	return recovered, handoff.trades, nil
}

type bybitRecoveryHandoff struct {
	trades []quanttick.TradeEvent
	uids   map[string]map[string]struct{}
}

func newBybitRecoveryHandoff(trades []quanttick.TradeEvent) *bybitRecoveryHandoff {
	handoff := &bybitRecoveryHandoff{
		trades: append([]quanttick.TradeEvent(nil), trades...),
		uids:   make(map[string]map[string]struct{}),
	}
	for _, trade := range trades {
		handoff.addUID(trade)
	}
	return handoff
}

func (h *bybitRecoveryHandoff) add(trade quanttick.TradeEvent) {
	h.trades = append(h.trades, trade)
	h.addUID(trade)
}

func (h *bybitRecoveryHandoff) addUID(trade quanttick.TradeEvent) {
	if h.uids[trade.Symbol] == nil {
		h.uids[trade.Symbol] = make(map[string]struct{})
	}
	h.uids[trade.Symbol][trade.UID] = struct{}{}
}

func (h *bybitRecoveryHandoff) hasTrades(symbol string) bool {
	return len(h.uids[symbol]) > 0
}

func (h *bybitRecoveryHandoff) overlaps(symbol string, recovered []quanttick.TradeEvent) bool {
	websocketUIDs := h.uids[symbol]
	if len(websocketUIDs) == 0 {
		return false
	}
	for _, trade := range recovered {
		if trade.Symbol != symbol {
			continue
		}
		if _, ok := websocketUIDs[trade.UID]; ok {
			return true
		}
	}
	return false
}

func (b *Bybit) recoverSymbol(
	ctx context.Context,
	symbol string,
	cursorUID string,
) ([]quanttick.TradeEvent, error) {
	endpoint, err := url.Parse(strings.TrimRight(b.RESTURL, "/") + "/v5/market/recent-trade")
	if err != nil {
		return nil, fmt.Errorf("build bybit recovery URL: %w", err)
	}
	query := endpoint.Query()
	query.Set("category", "linear")
	query.Set("symbol", symbol)
	query.Set("limit", "1000")
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build bybit recovery request: %w", err)
	}
	var payload bybitRecentTradesResponse
	for {
		if err := b.recoveryThrottle.wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for bybit recovery rate limit: %w", err)
		}
		if err := b.recoveryIPLimiter.wait(ctx, 1); err != nil {
			return nil, fmt.Errorf("wait for bybit recovery IP limit: %w", err)
		}
		response, err := b.HTTPClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("fetch bybit recovery for %s: %w", symbol, err)
		}
		now := time.Now()
		delay, rateErr := bybitResponseDelay(response.Header, now)
		if rateErr != nil {
			response.Body.Close()
			return nil, fmt.Errorf("fetch bybit recovery for %s: %w", symbol, rateErr)
		}
		b.recoveryThrottle.deferFor(delay)
		if response.StatusCode == http.StatusForbidden {
			response.Body.Close()
			b.HTTPClient.CloseIdleConnections()
			if delay < bybitIPBanDelay {
				delay = bybitIPBanDelay
			}
			b.recoveryThrottle.deferFor(delay)
			return nil, fmt.Errorf(
				"fetch bybit recovery for %s: HTTP 403; HTTP recovery paused for %s",
				symbol,
				delay,
			)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			response.Body.Close()
			if delay <= 0 {
				delay = bybitRecoveryRateLimitDelay
				b.recoveryThrottle.deferFor(delay)
			}
			continue
		}
		payload = bybitRecentTradesResponse{}
		if err := decodeRecoveryResponse(response, &payload); err != nil {
			return nil, fmt.Errorf("fetch bybit recovery for %s: %w", symbol, err)
		}
		if payload.ReturnCode == 10006 {
			if delay <= 0 {
				delay = bybitRecoveryRateLimitDelay
				b.recoveryThrottle.deferFor(delay)
			}
			continue
		}
		if payload.ReturnCode != 0 {
			return nil, fmt.Errorf(
				"fetch bybit recovery for %s: API error %d: %s",
				symbol,
				payload.ReturnCode,
				payload.ReturnMessage,
			)
		}
		break
	}

	receivedAt := time.Now().UTC()
	newestFirst := make([]quanttick.TradeEvent, 0, len(payload.Result.List))
	for _, rawTrade := range payload.Result.List {
		tradeTime, err := strconv.ParseInt(rawTrade.TradeTime, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse bybit recovery time: %w", err)
		}
		trade, err := parseBybitTrade(bybitTrade{
			TradeTime: tradeTime,
			Symbol:    rawTrade.Symbol,
			Side:      rawTrade.Side,
			Size:      rawTrade.Size,
			Price:     rawTrade.Price,
			TradeID:   rawTrade.TradeID,
		}, receivedAt)
		if err != nil {
			return nil, err
		}
		newestFirst = append(newestFirst, trade)
	}
	recovered, found := tradesAfterCursorNewestFirst(newestFirst, cursorUID)
	if !found {
		return nil, fmt.Errorf(
			"bybit recovery for %s does not contain cursor %s in %d recent trades",
			symbol,
			cursorUID,
			len(newestFirst),
		)
	}
	return recovered, nil
}

func bybitResponseDelay(headers http.Header, now time.Time) (time.Duration, error) {
	delay, err := retryAfterDelay(headers, now)
	if err != nil {
		return 0, err
	}
	remainingValue := strings.TrimSpace(headers.Get("X-Bapi-Limit-Status"))
	if remainingValue == "" {
		return delay, nil
	}
	remaining, err := strconv.Atoi(remainingValue)
	if err != nil {
		return 0, fmt.Errorf("parse X-Bapi-Limit-Status %q: %w", remainingValue, err)
	}
	if remaining > 0 {
		return delay, nil
	}
	resetValue := strings.TrimSpace(headers.Get("X-Bapi-Limit-Reset-Timestamp"))
	if resetValue == "" {
		return delay, nil
	}
	resetMillis, err := strconv.ParseInt(resetValue, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse X-Bapi-Limit-Reset-Timestamp %q: %w", resetValue, err)
	}
	resetDelay := time.UnixMilli(resetMillis).Sub(now)
	if resetDelay > delay {
		delay = resetDelay
	}
	return delay, nil
}

func parseBybitTrade(rawTrade bybitTrade, receivedAt time.Time) (quanttick.TradeEvent, error) {
	if rawTrade.TradeID == "" {
		return quanttick.TradeEvent{}, fmt.Errorf("bybit trade for %q has no trade id", rawTrade.Symbol)
	}
	price, err := quanttick.ParseDecimal(rawTrade.Price)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse bybit price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(rawTrade.Size)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse bybit size: %w", err)
	}
	tickRule, err := bybitTickRule(rawTrade.Side)
	if err != nil {
		return quanttick.TradeEvent{}, err
	}
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:   BybitName,
		UID:        rawTrade.TradeID,
		Symbol:     rawTrade.Symbol,
		Timestamp:  time.UnixMilli(rawTrade.TradeTime).UTC(),
		ReceivedAt: receivedAt,
		Price:      price,
		Notional:   notional,
		TickRule:   tickRule,
	}), nil
}

func (b *Bybit) awaitSubscription(
	ctx context.Context,
	conn *websocket.Conn,
) ([]quanttick.TradeEvent, error) {
	ackCtx, cancel := context.WithTimeout(ctx, b.SubscriptionTimeout)
	defer cancel()

	buffered := make([]quanttick.TradeEvent, 0)
	for {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("bybit subscription acknowledgement timed out after %s", b.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read bybit subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		isAck, err := parseBybitSubscriptionResponse(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			return buffered, nil
		}

		parsedTrades, err := b.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if len(buffered)+len(parsedTrades) > bybitSubscriptionBufferLimit {
			return nil, fmt.Errorf("bybit subscription trade buffer exceeded %d events", bybitSubscriptionBufferLimit)
		}
		buffered = append(buffered, parsedTrades...)
	}
}

func parseBybitSubscriptionResponse(data []byte) (bool, error) {
	var envelope bybitEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return false, fmt.Errorf("parse bybit message: %w", err)
	}
	if envelope.Success != nil && !*envelope.Success {
		return false, fmt.Errorf("bybit websocket %s failed: %s", envelope.Op, envelope.ReturnMessage)
	}
	if envelope.Op != "subscribe" {
		return false, nil
	}
	if envelope.Success == nil || !*envelope.Success {
		return false, fmt.Errorf("invalid bybit subscription response: %s", string(data))
	}
	if envelope.RequestID != bybitSubscriptionRequestID {
		return false, fmt.Errorf("bybit subscription response has unexpected request id %q", envelope.RequestID)
	}
	return true, nil
}

func (b *Bybit) startHeartbeats(ctx context.Context, conn *websocket.Conn) (<-chan error, <-chan struct{}) {
	errs := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if b.HeartbeatInterval <= 0 {
			return
		}

		ticker := time.NewTicker(b.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.Write(ctx, websocket.MessageText, []byte("{\"op\":\"ping\"}")); err != nil {
					select {
					case errs <- fmt.Errorf("send bybit heartbeat: %w", err):
					default:
					}
					_ = conn.CloseNow()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return errs, done
}

func takeBybitHeartbeatError(errs <-chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func (b *Bybit) hasSymbol(symbol string) bool {
	for _, configured := range b.Symbols {
		if configured == symbol {
			return true
		}
	}
	return false
}

func bybitTickRule(side string) (int, error) {
	switch strings.ToLower(side) {
	case "buy":
		return 1, nil
	case "sell":
		return -1, nil
	default:
		return 0, fmt.Errorf("invalid bybit side %q", side)
	}
}

type bybitEnvelope struct {
	Topic         string          `json:"topic"`
	Type          string          `json:"type"`
	Data          json.RawMessage `json:"data"`
	Success       *bool           `json:"success"`
	ReturnMessage string          `json:"ret_msg"`
	RequestID     string          `json:"req_id"`
	Op            string          `json:"op"`
}

type bybitRecentTradesResponse struct {
	ReturnCode    int    `json:"retCode"`
	ReturnMessage string `json:"retMsg"`
	Result        struct {
		List []struct {
			TradeID   string `json:"execId"`
			Symbol    string `json:"symbol"`
			Price     string `json:"price"`
			Size      string `json:"size"`
			Side      string `json:"side"`
			TradeTime string `json:"time"`
		} `json:"list"`
	} `json:"result"`
}

type bybitTrade struct {
	TradeTime int64  `json:"T"`
	Symbol    string `json:"s"`
	Side      string `json:"S"`
	Size      string `json:"v"`
	Price     string `json:"p"`
	TradeID   string `json:"i"`
	Sequence  int64  `json:"seq"`
}
