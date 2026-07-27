package exchanges

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (c *Coinbase) recoverTrades(ctx context.Context) ([]coinbaseParsedTrade, error) {
	if len(c.lastIDs) == 0 || strings.TrimSpace(c.RESTURL) == "" {
		return nil, nil
	}
	recovered := make([]coinbaseParsedTrade, 0)
	var recoveryErrors []error
	for _, symbol := range c.Symbols {
		cursor, ok := c.lastIDs[symbol]
		if !ok {
			continue
		}
		rows, err := c.recoverSymbol(ctx, symbol, cursor)
		recovered = append(recovered, rows...)
		if err != nil {
			recoveryErrors = append(recoveryErrors, err)
		}
	}
	sort.SliceStable(recovered, func(left, right int) bool {
		if !recovered[left].event.Timestamp.Equal(recovered[right].event.Timestamp) {
			return recovered[left].event.Timestamp.Before(recovered[right].event.Timestamp)
		}
		if recovered[left].event.Symbol != recovered[right].event.Symbol {
			return recovered[left].event.Symbol < recovered[right].event.Symbol
		}
		return recovered[left].tradeID < recovered[right].tradeID
	})
	return recovered, errors.Join(recoveryErrors...)
}

func (c *Coinbase) recoverSymbol(
	ctx context.Context,
	symbol string,
	cursor int64,
) ([]coinbaseParsedTrade, error) {
	newestFirst := make([]coinbaseParsedTrade, 0)
	var after int64
	for page := 0; page < coinbaseRecoveryMaxPages; page++ {
		endpoint, err := url.Parse(
			strings.TrimRight(c.RESTURL, "/") + "/products/" + url.PathEscape(symbol) + "/trades",
		)
		if err != nil {
			return nil, fmt.Errorf("build coinbase recovery URL: %w", err)
		}
		query := endpoint.Query()
		query.Set("limit", strconv.Itoa(coinbaseRecoveryPageLimit))
		if after > 0 {
			query.Set("after", strconv.FormatInt(after, 10))
		}
		endpoint.RawQuery = query.Encode()

		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("build coinbase recovery request: %w", err)
		}
		if err := c.recoveryThrottle.wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for coinbase recovery rate limit: %w", err)
		}
		response, err := c.HTTPClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("fetch coinbase recovery for %s: %w", symbol, err)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			delay, retryErr := retryAfterDelay(response.Header, time.Now())
			response.Body.Close()
			if retryErr != nil {
				return nil, fmt.Errorf("fetch coinbase recovery for %s: %w", symbol, retryErr)
			}
			if delay <= 0 {
				delay = time.Second
			}
			c.recoveryThrottle.deferFor(delay)
			page--
			continue
		}
		nextAfter := strings.TrimSpace(response.Header.Get("CB-AFTER"))

		var payloads []coinbaseMatchMessage
		if err := decodeRecoveryResponse(response, &payloads); err != nil {
			return nil, fmt.Errorf("fetch coinbase recovery for %s: %w", symbol, err)
		}
		if len(payloads) == 0 {
			return nil, fmt.Errorf("coinbase recovery for %s did not find cursor %d", symbol, cursor)
		}

		oldestID := int64(0)
		for _, payload := range payloads {
			if payload.ProductID == "" {
				payload.ProductID = symbol
			}
			if payload.ProductID != symbol {
				return nil, fmt.Errorf(
					"coinbase recovery for %s returned product %q",
					symbol,
					payload.ProductID,
				)
			}
			if oldestID == 0 || payload.TradeID < oldestID {
				oldestID = payload.TradeID
			}
			if payload.TradeID == cursor {
				sort.SliceStable(newestFirst, func(left, right int) bool {
					return newestFirst[left].tradeID < newestFirst[right].tradeID
				})
				expected := cursor + 1
				for _, parsed := range newestFirst {
					if parsed.tradeID != expected {
						return nil, fmt.Errorf(
							"coinbase recovery for %s expected trade %d, got %d",
							symbol,
							expected,
							parsed.tradeID,
						)
					}
					expected++
				}
				return newestFirst, nil
			}
			if payload.TradeID < cursor {
				continue
			}
			parsed, err := parseCoinbaseTrade(payload, time.Now().UTC())
			if err != nil {
				return nil, err
			}
			newestFirst = append(newestFirst, parsed)
		}
		previousAfter := after
		if nextAfter != "" {
			parsedAfter, err := strconv.ParseInt(nextAfter, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse coinbase CB-AFTER %q: %w", nextAfter, err)
			}
			after = parsedAfter
		} else {
			after = oldestID
		}
		if after <= 0 || after == previousAfter {
			return nil, fmt.Errorf("coinbase recovery for %s pagination did not advance", symbol)
		}
	}
	return nil, fmt.Errorf(
		"coinbase recovery for %s exceeded %d pages without cursor %d",
		symbol,
		coinbaseRecoveryMaxPages,
		cursor,
	)
}
