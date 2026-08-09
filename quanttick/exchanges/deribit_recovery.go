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

func (d *Deribit) recoverTrades(ctx context.Context) ([]deribitParsedTrade, error) {
	if len(d.lastSequences) == 0 || strings.TrimSpace(d.RESTURL) == "" {
		return nil, nil
	}
	recovered := make([]deribitParsedTrade, 0)
	var recoveryErrors []error
	for _, symbol := range d.Symbols {
		cursor, ok := d.lastSequences[symbol]
		if !ok {
			continue
		}
		rows, err := d.recoverSymbol(ctx, symbol, cursor+1)
		if err != nil {
			recoveryErrors = append(recoveryErrors, err)
		}
		recovered = append(recovered, rows...)
	}
	sort.SliceStable(recovered, func(left, right int) bool {
		if !recovered[left].event.Timestamp.Equal(recovered[right].event.Timestamp) {
			return recovered[left].event.Timestamp.Before(recovered[right].event.Timestamp)
		}
		if recovered[left].event.Symbol != recovered[right].event.Symbol {
			return recovered[left].event.Symbol < recovered[right].event.Symbol
		}
		return recovered[left].sequence < recovered[right].sequence
	})
	return recovered, errors.Join(recoveryErrors...)
}

func (d *Deribit) recoverSymbol(
	ctx context.Context,
	symbol string,
	startSequence int64,
) ([]deribitParsedTrade, error) {
	recovered := make([]deribitParsedTrade, 0)
	expected := startSequence
	for page := 0; page < deribitRecoveryMaxPages; page++ {
		endpoint, err := url.Parse(
			strings.TrimRight(d.RESTURL, "/") + "/get_last_trades_by_instrument",
		)
		if err != nil {
			return recovered, fmt.Errorf("build deribit recovery URL: %w", err)
		}
		query := endpoint.Query()
		query.Set("instrument_name", symbol)
		query.Set("start_seq", strconv.FormatInt(expected, 10))
		query.Set("count", strconv.Itoa(deribitRecoveryPageLimit))
		query.Set("sorting", "asc")
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return recovered, fmt.Errorf("build deribit recovery request: %w", err)
		}
		if err := d.recoveryThrottle.wait(ctx); err != nil {
			return recovered, fmt.Errorf("wait for deribit recovery rate limit: %w", err)
		}
		response, err := d.HTTPClient.Do(request)
		if err != nil {
			return recovered, fmt.Errorf("fetch deribit recovery for %s: %w", symbol, err)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			delay, retryErr := retryAfterDelay(response.Header, time.Now())
			response.Body.Close()
			if retryErr != nil {
				return recovered, fmt.Errorf("fetch deribit recovery for %s: %w", symbol, retryErr)
			}
			if delay <= 0 {
				delay = time.Second
			}
			d.recoveryThrottle.deferFor(delay)
			page--
			continue
		}
		var envelope deribitRecoveryEnvelope
		if err := decodeRecoveryResponse(response, &envelope); err != nil {
			return recovered, fmt.Errorf("fetch deribit recovery for %s: %w", symbol, err)
		}
		if envelope.Error != nil {
			return recovered, fmt.Errorf(
				"fetch deribit recovery for %s: error %d: %s",
				symbol,
				envelope.Error.Code,
				envelope.Error.Message,
			)
		}
		if len(envelope.Result.Trades) == 0 {
			if envelope.Result.HasMore {
				return recovered, fmt.Errorf("deribit recovery for %s returned has_more without trades", symbol)
			}
			return recovered, nil
		}
		sort.SliceStable(envelope.Result.Trades, func(left, right int) bool {
			return envelope.Result.Trades[left].TradeSequence < envelope.Result.Trades[right].TradeSequence
		})
		receivedAt := time.Now().UTC()
		for _, rawTrade := range envelope.Result.Trades {
			if rawTrade.InstrumentName != symbol {
				return recovered, fmt.Errorf(
					"deribit recovery for %s returned instrument %q",
					symbol,
					rawTrade.InstrumentName,
				)
			}
			if rawTrade.TradeSequence != expected {
				return recovered, fmt.Errorf(
					"deribit recovery for %s expected trade sequence %d, got %d",
					symbol,
					expected,
					rawTrade.TradeSequence,
				)
			}
			parsed, err := parseDeribitTrade(rawTrade, receivedAt)
			if err != nil {
				return recovered, err
			}
			recovered = append(recovered, parsed)
			expected++
		}
		if !envelope.Result.HasMore {
			return recovered, nil
		}
	}
	return recovered, fmt.Errorf(
		"deribit recovery for %s exceeded %d pages",
		symbol,
		deribitRecoveryMaxPages,
	)
}

type deribitRecoveryEnvelope struct {
	JSONRPC string        `json:"jsonrpc"`
	Error   *deribitError `json:"error"`
	Result  struct {
		Trades  []deribitTrade `json:"trades"`
		HasMore bool           `json:"has_more"`
	} `json:"result"`
}
