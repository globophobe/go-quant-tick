package exchanges

import (
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
)

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
