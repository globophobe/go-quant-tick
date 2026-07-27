package exchanges

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

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
