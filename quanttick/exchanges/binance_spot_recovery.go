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
	if b.stream != "trade" || len(b.lastIDs) == 0 {
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
	idRangePages := b.name == AsterName
	var latestID int64
	if idRangePages {
		// Aster's limit spans raw IDs, including holes. Freeze a latest ID so
		// short and empty pages cannot prematurely end recovery.
		latest, err := b.fetchRawRecoveryPage(ctx, symbol, nil, 1)
		if err != nil {
			return recovered, err
		}
		if len(latest) != 1 || latest[0].TradeID <= 0 {
			return recovered, fmt.Errorf("aster recovery for %s did not return a latest raw trade", symbol)
		}
		latestID = latest[0].TradeID
		if fromID > latestID {
			return recovered, nil
		}
	}
	for page := 0; page < binanceRecoveryMaxPages; page++ {
		payloads, err := b.fetchRawRecoveryPage(ctx, symbol, &fromID, binanceRecoveryPageLimit)
		if err != nil {
			return recovered, err
		}
		if len(payloads) == 0 {
			if !idRangePages {
				return recovered, nil
			}
			if latestID-fromID < binanceRecoveryPageLimit {
				return recovered, fmt.Errorf("aster recovery for %s did not contain latest trade %d", symbol, latestID)
			}
			fromID += binanceRecoveryPageLimit
			continue
		}

		receivedAt := time.Now().UTC()
		for _, payload := range payloads {
			if payload.TradeID < fromID || (!idRangePages && payload.TradeID != fromID) {
				return recovered, fmt.Errorf(
					"%s recovery for %s expected trade %d, got %d",
					b.name,
					symbol,
					fromID,
					payload.TradeID,
				)
			}
			if idRangePages && payload.TradeID > latestID {
				return recovered, fmt.Errorf("aster recovery for %s did not contain latest trade %d", symbol, latestID)
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
			fromID = payload.TradeID + 1
			if idRangePages && payload.TradeID == latestID {
				return recovered, nil
			}
		}
		if !idRangePages && len(payloads) < binanceRecoveryPageLimit {
			return recovered, nil
		}
	}
	return recovered, fmt.Errorf(
		"%s recovery for %s exceeded %d pages",
		b.name,
		symbol,
		binanceRecoveryMaxPages,
	)
}

func (b *Binance) fetchRawRecoveryPage(
	ctx context.Context,
	symbol string,
	fromID *int64,
	limit int,
) ([]binanceSpotRecoveryTrade, error) {
	endpoint, err := url.Parse(strings.TrimRight(b.RESTURL, "/") + "/historicalTrades")
	if err != nil {
		return nil, fmt.Errorf("build %s recovery URL: %w", b.name, err)
	}
	query := endpoint.Query()
	query.Set("symbol", symbol)
	if fromID != nil {
		query.Set("fromId", strconv.FormatInt(*fromID, 10))
	}
	query.Set("limit", strconv.Itoa(limit))
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build %s recovery request: %w", b.name, err)
	}
	if b.APIKey != "" {
		request.Header.Set("X-MBX-APIKEY", b.APIKey)
	}
	var response *http.Response
	for {
		if err := b.recoveryThrottle.wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for %s recovery rate limit: %w", b.name, err)
		}
		response, err = b.HTTPClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("fetch %s recovery for %s: %w", b.name, symbol, err)
		}
		delay, rateErr := binanceResponseDelay(response.Header, time.Now().UTC(), b.RecoveryMaxWeight)
		if rateErr != nil {
			response.Body.Close()
			return nil, fmt.Errorf("fetch %s recovery for %s: %w", b.name, symbol, rateErr)
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
		return nil, fmt.Errorf("fetch %s recovery for %s: %w", b.name, symbol, err)
	}
	return payloads, nil
}

type binanceSpotRecoveryTrade struct {
	TradeID      int64  `json:"id"`
	Price        string `json:"price"`
	Quantity     string `json:"qty"`
	TradeTime    int64  `json:"time"`
	BuyerIsMaker bool   `json:"isBuyerMaker"`
}
