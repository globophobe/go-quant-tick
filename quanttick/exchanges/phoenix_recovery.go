package exchanges

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

// Recovery starts at the last emitted second, including its other fills. The
// cursor-paginated REST window is read in full before emitting it oldest first;
// reversing individual pages would reorder same-second fills at page boundaries.
// The fill feed supplies no sequence with which to prove continuity, so events
// retain IsSequential=false even after a successful REST overlap.
func (p *Phoenix) recoverTrades(ctx context.Context, until time.Time) ([]quanttick.TradeEvent, error) {
	var recovered []quanttick.TradeEvent
	var recoveryErrors []error
	for _, symbol := range p.Symbols {
		anchor, ok := p.lastTrades[symbol]
		if !ok {
			continue
		}
		rows, err := p.recoverSymbol(ctx, symbol, anchor, until)
		if err != nil {
			recoveryErrors = append(recoveryErrors, fmt.Errorf("phoenix recovery gap for %s: %w", symbol, err))
			continue
		}
		recovered = append(recovered, rows...)
	}
	sortTradeEventsChronologically(recovered)
	return recovered, errors.Join(recoveryErrors...)
}

func (p *Phoenix) recoverSymbol(ctx context.Context, symbol string, anchor quanttick.TradeEvent, until time.Time) ([]quanttick.TradeEvent, error) {
	endpoint, err := url.Parse(strings.TrimRight(p.RESTURL, "/") + "/v1/trades/" + url.PathEscape(symbol) + "/fills")
	if err != nil {
		return nil, fmt.Errorf("build phoenix recovery URL: %w", err)
	}
	query := endpoint.Query()
	since := anchor.Timestamp.Truncate(time.Second)
	query.Set("startTime", strconv.FormatInt(since.UnixMilli(), 10))
	query.Set("endTime", strconv.FormatInt(until.UnixMilli(), 10))
	query.Set("limit", strconv.Itoa(phoenixRecoveryPageLimit))
	var fills []phoenixFill
	cursors := make(map[string]bool)
	previous := until
	for {
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("build phoenix recovery request: %w", err)
		}
		if err := p.recoveryThrottle.wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for phoenix recovery rate limit: %w", err)
		}
		response, err := p.HTTPClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("fetch phoenix recovery: %w", err)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			delay, err := retryAfterDelay(response.Header, time.Now())
			response.Body.Close()
			if err != nil {
				return nil, err
			}
			p.recoveryThrottle.deferFor(max(delay, phoenixRecoveryRequestInterval))
			continue
		}
		var page struct {
			Data       []phoenixFill `json:"data"`
			HasMore    *bool         `json:"hasMore"`
			NextCursor string        `json:"nextCursor"`
		}
		if err := decodeRecoveryResponse(response, &page); err != nil {
			return nil, fmt.Errorf("fetch phoenix recovery: %w", err)
		}
		if page.Data == nil || page.HasMore == nil {
			return nil, fmt.Errorf("phoenix fill page is malformed")
		}
		for _, fill := range page.Data {
			timestamp, err := time.Parse(time.RFC3339Nano, fill.Timestamp)
			if err != nil {
				return nil, fmt.Errorf("parse phoenix recovery timestamp: %w", err)
			}
			if fill.Symbol != symbol || timestamp.Before(since) || !timestamp.Before(until) || timestamp.After(previous) {
				return nil, fmt.Errorf("phoenix recovery fills violate the requested market, window or newest-first order")
			}
			previous = timestamp
		}
		fills = append(fills, page.Data...)
		if len(fills) > phoenixTradeLimit {
			return nil, fmt.Errorf("phoenix recovery exceeded %d fills", phoenixTradeLimit)
		}
		if !*page.HasMore {
			break
		}
		if len(page.Data) == 0 || page.NextCursor == "" || cursors[page.NextCursor] {
			return nil, fmt.Errorf("phoenix recovery pagination did not advance")
		}
		cursors[page.NextCursor] = true
		query.Set("cursor", page.NextCursor)
	}
	slices.Reverse(fills)
	counter := phoenixFillCounter{}
	trades := make([]quanttick.TradeEvent, 0, len(fills))
	foundAnchor := false
	receivedAt := time.Now().UTC()
	for _, fill := range fills {
		trade, err := parsePhoenixFill(fill, receivedAt)
		if err != nil {
			return nil, err
		}
		trade.UID = counter.identify(trade, fill)
		foundAnchor = foundAnchor || trade.UID == anchor.UID
		trades = append(trades, trade)
	}
	if !foundAnchor {
		return nil, fmt.Errorf("phoenix REST fills did not include the previous WebSocket fill")
	}
	return trades, nil
}
