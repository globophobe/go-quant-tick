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

func (b *Binance) recoverTrades(ctx context.Context) ([]binanceParsedTrade, error) {
	if b.name == BinanceFuturesName {
		return b.recoverFuturesTrades(ctx)
	}
	if b.name != BinanceName || len(b.lastIDs) == 0 {
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
		rows, err := b.recoverSpotSymbol(ctx, symbol, lastID+1, sequenceIDs)
		recovered = append(recovered, rows...)
		if err != nil {
			recoveryErrors = append(recoveryErrors, err)
		}
	}
	sort.SliceStable(recovered, func(left, right int) bool {
		leftTrade := recovered[left]
		rightTrade := recovered[right]
		if leftTrade.event.Timestamp.Equal(rightTrade.event.Timestamp) {
			if leftTrade.event.Symbol == rightTrade.event.Symbol {
				return leftTrade.tradeID < rightTrade.tradeID
			}
			return leftTrade.event.Symbol < rightTrade.event.Symbol
		}
		return leftTrade.event.Timestamp.Before(rightTrade.event.Timestamp)
	})
	return recovered, errors.Join(recoveryErrors...)
}

func (b *Binance) recoverSpotSymbol(
	ctx context.Context,
	symbol string,
	fromID int64,
	sequenceIDs map[string]int64,
) ([]binanceParsedTrade, error) {
	recovered := make([]binanceParsedTrade, 0)
	for page := 0; page < binanceRecoveryMaxPages; page++ {
		endpoint, err := url.Parse(strings.TrimRight(b.RESTURL, "/") + "/historicalTrades")
		if err != nil {
			return recovered, fmt.Errorf("build binance spot recovery URL: %w", err)
		}
		query := endpoint.Query()
		query.Set("symbol", symbol)
		query.Set("fromId", strconv.FormatInt(fromID, 10))
		query.Set("limit", strconv.Itoa(binanceRecoveryPageLimit))
		endpoint.RawQuery = query.Encode()

		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return recovered, fmt.Errorf("build binance spot recovery request: %w", err)
		}
		if b.APIKey != "" {
			request.Header.Set("X-MBX-APIKEY", b.APIKey)
		}
		var response *http.Response
		for {
			if err := b.recoveryThrottle.wait(ctx); err != nil {
				return recovered, fmt.Errorf("wait for binance spot recovery rate limit: %w", err)
			}
			response, err = b.HTTPClient.Do(request)
			if err != nil {
				return recovered, fmt.Errorf("fetch binance spot recovery for %s: %w", symbol, err)
			}
			delay, rateErr := binanceResponseDelay(response.Header, time.Now().UTC(), b.RecoveryMaxWeight)
			if rateErr != nil {
				response.Body.Close()
				return recovered, fmt.Errorf("fetch binance spot recovery for %s: %w", symbol, rateErr)
			}
			b.recoveryThrottle.deferFor(delay)
			if response.StatusCode != http.StatusTooManyRequests && response.StatusCode != http.StatusTeapot {
				break
			}
			response.Body.Close()
			if delay <= 0 {
				b.recoveryThrottle.deferFor(binanceRecoveryRetryDelay)
			}
		}

		var payloads []binanceSpotRecoveryTrade
		if err := decodeRecoveryResponse(response, &payloads); err != nil {
			return recovered, fmt.Errorf("fetch binance spot recovery for %s: %w", symbol, err)
		}
		if len(payloads) == 0 {
			return recovered, nil
		}

		receivedAt := time.Now().UTC()
		for _, payload := range payloads {
			if payload.TradeID != fromID {
				return recovered, fmt.Errorf(
					"binance spot recovery for %s expected trade %d, got %d",
					symbol,
					fromID,
					payload.TradeID,
				)
			}
			parsed, ok, err := b.buildParsedTrade(
				binanceTradeMessage{
					Symbol:    symbol,
					TradeID:   payload.TradeID,
					TradeTime: payload.TradeTime,
					Price:     payload.Price,
					Quantity:  payload.Quantity,
				},
				payload.BuyerIsMaker,
				receivedAt,
				sequenceIDs,
			)
			if err != nil {
				return recovered, fmt.Errorf("parse binance spot recovery trade: %w", err)
			}
			if !ok {
				return recovered, errors.New("parse binance spot recovery trade: empty trade")
			}
			recovered = append(recovered, parsed)
			fromID++
		}
		if len(payloads) < binanceRecoveryPageLimit {
			return recovered, nil
		}
	}
	return recovered, fmt.Errorf(
		"binance spot recovery for %s exceeded %d pages",
		symbol,
		binanceRecoveryMaxPages,
	)
}

type binanceSpotRecoveryTrade struct {
	TradeID      int64  `json:"id"`
	Price        string `json:"price"`
	Quantity     string `json:"qty"`
	TradeTime    int64  `json:"time"`
	BuyerIsMaker bool   `json:"isBuyerMaker"`
}
