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

var errPhoenixRecoveryAnchorMissing = errors.New("phoenix REST fills did not include the previous WebSocket fill")

type phoenixRecoveryGapError struct {
	err error
}

func (e *phoenixRecoveryGapError) Error() string { return e.err.Error() }

func (e *phoenixRecoveryGapError) Unwrap() error { return e.err }

// An exhausted recovery leaves a data gap even when its cause is a timeout or
// transport failure. Keep it reportable instead of treating it as a disconnect.
func (e *phoenixRecoveryGapError) ClassifyTransient() (bool, bool) { return false, true }

// Recovery reads the last emitted second to locate the anchor, then emits only
// its suffix. Appending previously unseen predecessors from that second would
// corrupt closing prices. Read all REST pages before reversing the window so
// same-second fills retain their order across page boundaries. Repeated anchor
// fingerprints are ambiguous: source-local occurrence numbers cannot prove which
// occurrence the previous session observed. Report a gap instead of replaying it.
// The fill feed supplies no sequence with which to prove continuity, so events
// retain IsSequential=false even after a successful REST overlap.
// A missing anchor may reflect REST lag: retry complete windows under run's
// recovery deadline, with the original anchor and end fixed. Finish other markets
// before retrying lagging ones; malformed or ambiguous history fails immediately.
func (p *Phoenix) recoverTrades(ctx context.Context, until time.Time) ([]quanttick.TradeEvent, error) {
	var recovered []quanttick.TradeEvent
	var recoveryErrors []error
	pending := make(map[string]quanttick.TradeEvent)
	for _, symbol := range p.Symbols {
		if anchor, ok := p.lastTrades[symbol]; ok {
			pending[symbol] = anchor
		}
	}
	for len(pending) > 0 {
		for _, symbol := range p.Symbols {
			anchor, ok := pending[symbol]
			if !ok {
				continue
			}
			rows, err := p.recoverSymbol(ctx, symbol, anchor, until)
			if errors.Is(err, errPhoenixRecoveryAnchorMissing) {
				continue
			}
			delete(pending, symbol)
			if err != nil {
				recoveryErrors = append(recoveryErrors, fmt.Errorf("phoenix recovery gap for %s: %w", symbol, err))
				continue
			}
			recovered = append(recovered, rows...)
		}
		if len(pending) == 0 {
			break
		}
		if err := sleepContext(ctx, phoenixRecoveryRequestInterval); err != nil {
			for _, symbol := range p.Symbols {
				if _, ok := pending[symbol]; ok {
					recoveryErrors = append(recoveryErrors, fmt.Errorf("phoenix recovery gap for %s: %w: %w", symbol, errPhoenixRecoveryAnchorMissing, err))
				}
			}
			break
		}
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
	until = until.Truncate(time.Second)
	if !since.Before(until) {
		return nil, fmt.Errorf("phoenix recovery requires a completed anchor second")
	}
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
	anchorIndex := -1
	anchorKey, _, _ := strings.Cut(anchor.UID, ":")
	receivedAt := time.Now().UTC()
	for index, fill := range fills {
		trade, err := parsePhoenixFill(fill, receivedAt)
		if err != nil {
			return nil, err
		}
		trade.UID = counter.identify(trade, fill)
		key, _, _ := strings.Cut(trade.UID, ":")
		if key == anchorKey {
			if anchorIndex >= 0 {
				return nil, fmt.Errorf("phoenix REST anchor is ambiguous: identical fills have source-local occurrence numbers")
			}
			anchorIndex = index
		}
		trades = append(trades, trade)
	}
	if anchorIndex < 0 {
		return nil, errPhoenixRecoveryAnchorMissing
	}
	if trades[anchorIndex].UID != anchor.UID {
		return nil, fmt.Errorf("phoenix REST anchor occurrence differs from the previous WebSocket fill")
	}
	return trades[anchorIndex+1:], nil
}
