package exchanges

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
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
	return recoverWithWebSocketOverlap(ctx, b.Name(), b.Symbols, b.lastUIDs,
		b.recoverSymbol, bybitRecoveryRetryDelay, buffered, stream, streamErr)
}

func (b *Bybit) recoverSymbol(
	ctx context.Context,
	symbol string,
	cursorUID string,
) ([]quanttick.TradeEvent, error) {
	limit := bybitDerivativeRecoveryLimit
	if b.category == "spot" {
		limit = bybitSpotRecoveryLimit
	}
	endpoint, err := url.Parse(strings.TrimRight(b.RESTURL, "/") + "/v5/market/recent-trade")
	if err != nil {
		return nil, fmt.Errorf("build bybit recovery URL: %w", err)
	}
	query := endpoint.Query()
	query.Set("category", b.category)
	query.Set("symbol", symbol)
	query.Set("limit", strconv.Itoa(limit))
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
		reservation, err := b.recoveryIPLimiter.reserve(ctx, 1)
		if err != nil {
			return nil, fmt.Errorf("wait for bybit recovery IP limit: %w", err)
		}
		if err := b.recoveryIPThrottle.wait(ctx); err != nil {
			reservation.refund(1)
			return nil, fmt.Errorf("wait for bybit recovery IP embargo: %w", err)
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
			b.recoveryIPThrottle.deferFor(delay)
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
		trade, err := b.parseTrade(bybitTrade{
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
	if found {
		return recovered, nil
	}
	recovered = append([]quanttick.TradeEvent(nil), newestFirst...)
	slices.Reverse(recovered)
	return recovered, &recoveryCursorGapError{
		exchange:  b.Name(),
		symbol:    symbol,
		cursorUID: cursorUID,
		rows:      len(newestFirst),
	}
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
