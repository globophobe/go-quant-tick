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
	HyperliquidName                      = "hyperliquid"
	HyperliquidURL                       = "wss://api.hyperliquid.xyz/ws"
	HyperliquidRESTURL                   = "https://api.hyperliquid.xyz"
	hyperliquidSeenTradeLimit            = 10000
	hyperliquidSubscriptionBufferLimit   = 10000
	hyperliquidHeartbeatInterval         = 30 * time.Second
	hyperliquidRateLimitWeightPerMinute  = 1200
	hyperliquidRecentTradesBaseWeight    = 20
	hyperliquidRecentTradesRowsPerWeight = 20
	hyperliquidRecentTradesMaxRows       = 10
	hyperliquidRecentTradesMaxWeight     = hyperliquidRecentTradesBaseWeight +
		(hyperliquidRecentTradesMaxRows+hyperliquidRecentTradesRowsPerWeight-1)/
			hyperliquidRecentTradesRowsPerWeight
)

var _ quanttick.Exchange = (*Hyperliquid)(nil)

type Hyperliquid struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	HeartbeatInterval   time.Duration
	lastUIDs            map[string]string
	recoveryLimiter     *requestWindowLimiter
}

type HyperliquidOption func(*Hyperliquid)

func NewHyperliquid(symbols []string, options ...HyperliquidOption) *Hyperliquid {
	exchange := &Hyperliquid{
		Symbols:             append([]string(nil), symbols...),
		URL:                 HyperliquidURL,
		RESTURL:             HyperliquidRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		HeartbeatInterval:   hyperliquidHeartbeatInterval,
		lastUIDs:            make(map[string]string),
		recoveryLimiter:     newRequestWindowLimiter(hyperliquidRateLimitWeightPerMinute, time.Minute),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithHyperliquidURL(url string) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.URL = url
	}
}

func WithHyperliquidRESTURL(url string) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.RESTURL = url
	}
}

func WithHyperliquidHTTPClient(client *http.Client) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.HTTPClient = client
	}
}

func WithHyperliquidReconnectDelay(delay time.Duration) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.ReconnectDelay = delay
	}
}

func WithHyperliquidSubscriptionTimeout(timeout time.Duration) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.SubscriptionTimeout = timeout
	}
}

func WithHyperliquidHeartbeatInterval(interval time.Duration) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.HeartbeatInterval = interval
	}
}

func (h *Hyperliquid) Name() string {
	return HyperliquidName
}

func (h *Hyperliquid) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)
	seen := newSeenTradeIDs(hyperliquidSeenTradeLimit)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(h.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := h.runWithErrors(ctx, trades, seen, errs); err != nil {
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

func (h *Hyperliquid) SubscriptionMessages() []map[string]any {
	messages := make([]map[string]any, 0, len(h.Symbols))
	for _, symbol := range h.Symbols {
		messages = append(messages, map[string]any{
			"method": "subscribe",
			"subscription": map[string]any{
				"type": "trades",
				"coin": symbol,
			},
		})
	}
	return messages
}

func (h *Hyperliquid) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel != "trades" {
		return nil, nil
	}

	var rawTrades []hyperliquidTrade
	if err := json.Unmarshal(envelope.Data, &rawTrades); err != nil {
		return nil, fmt.Errorf("parse hyperliquid trades: %w", err)
	}

	trades := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		trade, err := parseHyperliquidTrade(rawTrade, receivedAt)
		if err != nil {
			return nil, err
		}
		trades = append(trades, trade)
	}

	return trades, nil
}

func (h *Hyperliquid) run(ctx context.Context, trades chan<- quanttick.TradeEvent, seen *seenTradeIDs) error {
	return h.runWithErrors(ctx, trades, seen, nil)
}

func (h *Hyperliquid) runWithErrors(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	seen *seenTradeIDs,
	errs chan<- error,
) error {
	conn, _, err := websocket.Dial(ctx, h.URL, nil)
	if err != nil {
		return fmt.Errorf("dial hyperliquid websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range h.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal hyperliquid subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send hyperliquid subscription: %w", err)
		}
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatErr, heartbeatDone := h.startHeartbeats(heartbeatCtx, conn)
	defer func() {
		cancelHeartbeat()
		<-heartbeatDone
	}()

	buffered, err := h.awaitSubscriptions(ctx, conn)
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, streamErr := h.startTradeReader(streamCtx, conn)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, recoveryErr := h.recoverTrades(recoveryCtx)
	cancelRecovery()
	for _, trade := range recovered {
		if err := emitSeenTrade(ctx, trades, seen, h.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, trade := range buffered {
		if err := emitSeenTrade(ctx, trades, seen, h.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	for trade := range stream {
		if err := emitSeenTrade(ctx, trades, seen, h.lastUIDs, trade); err != nil {
			cancelStream()
			return err
		}
	}
	cancelStream()
	if heartbeatErr := takeHyperliquidHeartbeatError(heartbeatErr); heartbeatErr != nil {
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

func (h *Hyperliquid) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent, hyperliquidSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read hyperliquid websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}

			if err := parseHyperliquidProtocolError(data); err != nil {
				errs <- err
				return
			}
			parsedTrades, err := h.ParseTradeMessage(data, time.Now().UTC())
			if err != nil {
				errs <- err
				return
			}
			for _, trade := range parsedTrades {
				select {
				case trades <- trade:
				case <-ctx.Done():
					errs <- ctx.Err()
					return
				default:
					errs <- fmt.Errorf(
						"hyperliquid websocket trade buffer exceeded %d events",
						hyperliquidSubscriptionBufferLimit,
					)
					_ = conn.CloseNow()
					return
				}
			}
		}
	}()
	return trades, errs
}

func (h *Hyperliquid) recoverTrades(ctx context.Context) ([]quanttick.TradeEvent, error) {
	if len(h.lastUIDs) == 0 {
		return nil, nil
	}

	recovered := make([]quanttick.TradeEvent, 0)
	var recoveryErrors []error
	for _, symbol := range h.Symbols {
		cursorUID, ok := h.lastUIDs[symbol]
		if !ok {
			continue
		}
		rows, err := h.recoverSymbol(ctx, symbol, cursorUID)
		recovered = append(recovered, rows...)
		if err != nil {
			recoveryErrors = append(recoveryErrors, err)
		}
	}
	sortTradeEventsChronologically(recovered)
	return recovered, errors.Join(recoveryErrors...)
}

func (h *Hyperliquid) recoverSymbol(
	ctx context.Context,
	symbol string,
	cursorUID string,
) ([]quanttick.TradeEvent, error) {
	body, err := json.Marshal(map[string]string{
		"type": "recentTrades",
		"coin": symbol,
	})
	if err != nil {
		return nil, fmt.Errorf("build hyperliquid recovery payload: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(h.RESTURL, "/")+"/info",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build hyperliquid recovery request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	reservation, err := h.recoveryLimiter.reserve(ctx, hyperliquidRecentTradesMaxWeight)
	if err != nil {
		return nil, fmt.Errorf("wait for hyperliquid recovery rate limit: %w", err)
	}
	response, err := h.HTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch hyperliquid recovery for %s: %w", symbol, err)
	}
	var rawTrades []hyperliquidTrade
	if err := decodeRecoveryResponse(response, &rawTrades); err != nil {
		return nil, fmt.Errorf("fetch hyperliquid recovery for %s: %w", symbol, err)
	}
	if len(rawTrades) > hyperliquidRecentTradesMaxRows {
		return nil, fmt.Errorf(
			"fetch hyperliquid recovery for %s: recentTrades returned %d rows, expected at most %d",
			symbol,
			len(rawTrades),
			hyperliquidRecentTradesMaxRows,
		)
	}
	actualWeight := hyperliquidRecentTradesBaseWeight +
		hyperliquidRecentTradesExtraWeight(len(rawTrades))
	reservation.refund(hyperliquidRecentTradesMaxWeight - actualWeight)

	receivedAt := time.Now().UTC()
	newestFirst := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		trade, err := parseHyperliquidTrade(rawTrade, receivedAt)
		if err != nil {
			return nil, err
		}
		newestFirst = append(newestFirst, trade)
	}
	recovered, found := tradesAfterCursorNewestFirst(newestFirst, cursorUID)
	if !found {
		// Hyperliquid also replays missed trades in the WebSocket reconnect snapshot.
		return nil, nil
	}
	return recovered, nil
}

func hyperliquidRecentTradesExtraWeight(rows int) int {
	if rows <= 0 {
		return 0
	}
	return (rows + hyperliquidRecentTradesRowsPerWeight - 1) /
		hyperliquidRecentTradesRowsPerWeight
}

func parseHyperliquidTrade(
	rawTrade hyperliquidTrade,
	receivedAt time.Time,
) (quanttick.TradeEvent, error) {
	price, err := quanttick.ParseDecimal(rawTrade.Price)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse hyperliquid price: %w", err)
	}
	notional, err := quanttick.ParseDecimal(rawTrade.Size)
	if err != nil {
		return quanttick.TradeEvent{}, fmt.Errorf("parse hyperliquid size: %w", err)
	}
	tickRule, err := hyperliquidTickRule(rawTrade.Side)
	if err != nil {
		return quanttick.TradeEvent{}, err
	}
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:   HyperliquidName,
		UID:        strconv.FormatInt(rawTrade.Time, 10) + ":" + rawTrade.Coin + ":" + strconv.FormatInt(rawTrade.TradeID, 10),
		Symbol:     rawTrade.Coin,
		Timestamp:  time.UnixMilli(rawTrade.Time).UTC(),
		ReceivedAt: receivedAt,
		Price:      price,
		Notional:   notional,
		TickRule:   tickRule,
	}), nil
}

func (h *Hyperliquid) awaitSubscriptions(ctx context.Context, conn *websocket.Conn) ([]quanttick.TradeEvent, error) {
	expected := make(map[string]struct{}, len(h.Symbols))
	for _, symbol := range h.Symbols {
		expected[symbol] = struct{}{}
	}
	if len(expected) == 0 {
		return nil, nil
	}

	ackCtx, cancel := context.WithTimeout(ctx, h.SubscriptionTimeout)
	defer cancel()

	acked := make(map[string]struct{}, len(expected))
	buffered := make([]quanttick.TradeEvent, 0)
	for len(acked) < len(expected) {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("hyperliquid subscription acknowledgement timed out after %s", h.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read hyperliquid subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		coin, isAck, err := parseHyperliquidSubscriptionAck(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			if _, ok := expected[coin]; !ok {
				return nil, fmt.Errorf("hyperliquid acknowledged unexpected trades subscription %q", coin)
			}
			acked[coin] = struct{}{}
			continue
		}

		parsedTrades, err := h.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if len(buffered)+len(parsedTrades) > hyperliquidSubscriptionBufferLimit {
			return nil, fmt.Errorf("hyperliquid subscription trade buffer exceeded %d events", hyperliquidSubscriptionBufferLimit)
		}
		buffered = append(buffered, parsedTrades...)
	}
	return buffered, nil
}

func parseHyperliquidSubscriptionAck(data []byte) (string, bool, error) {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", false, fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel == "error" {
		return "", false, hyperliquidProtocolError(envelope.Data)
	}
	if envelope.Channel != "subscriptionResponse" {
		return "", false, nil
	}

	var response hyperliquidSubscriptionResponse
	if err := json.Unmarshal(envelope.Data, &response); err != nil {
		return "", false, fmt.Errorf("parse hyperliquid subscription acknowledgement: %w", err)
	}
	if response.Method != "subscribe" || response.Subscription.Type != "trades" || response.Subscription.Coin == "" {
		return "", false, fmt.Errorf("invalid hyperliquid subscription acknowledgement: %s", string(data))
	}
	return response.Subscription.Coin, true, nil
}

func parseHyperliquidProtocolError(data []byte) error {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel != "error" {
		return nil
	}
	return hyperliquidProtocolError(envelope.Data)
}

func hyperliquidProtocolError(data json.RawMessage) error {
	var message string
	if err := json.Unmarshal(data, &message); err == nil {
		return fmt.Errorf("hyperliquid websocket error: %s", message)
	}
	return fmt.Errorf("hyperliquid websocket error: %s", string(data))
}

func (h *Hyperliquid) startHeartbeats(ctx context.Context, conn *websocket.Conn) (<-chan error, <-chan struct{}) {
	errs := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if h.HeartbeatInterval <= 0 {
			return
		}

		ticker := time.NewTicker(h.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.Write(ctx, websocket.MessageText, []byte("{\"method\":\"ping\"}")); err != nil {
					select {
					case errs <- fmt.Errorf("send hyperliquid heartbeat: %w", err):
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

func takeHyperliquidHeartbeatError(errs <-chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func hyperliquidTickRule(side string) (int, error) {
	switch strings.ToLower(side) {
	case "b", "buy":
		return 1, nil
	case "a", "sell":
		return -1, nil
	default:
		return 0, fmt.Errorf("invalid hyperliquid side %q", side)
	}
}

func sendTrade(ctx context.Context, trades chan<- quanttick.TradeEvent, trade quanttick.TradeEvent) error {
	select {
	case trades <- trade:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type hyperliquidEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type hyperliquidTrade struct {
	Coin    string `json:"coin"`
	Side    string `json:"side"`
	Price   string `json:"px"`
	Size    string `json:"sz"`
	Time    int64  `json:"time"`
	TradeID int64  `json:"tid"`
}

type hyperliquidSubscriptionResponse struct {
	Method       string `json:"method"`
	Subscription struct {
		Type string `json:"type"`
		Coin string `json:"coin"`
	} `json:"subscription"`
}
