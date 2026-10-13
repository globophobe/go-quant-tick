package exchanges

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func (m *marketStream) get(ctx context.Context, path string, params url.Values, target any) error {
	endpoint := strings.TrimRight(m.RESTURL, "/") + path + "?" + params.Encode()
	for {
		if err := m.throttle.wait(ctx); err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		response, err := m.HTTPClient.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode == http.StatusTooManyRequests {
			delay, err := retryAfterDelay(response.Header, time.Now())
			response.Body.Close()
			if err != nil {
				return err
			}
			m.throttle.deferFor(max(delay, 2*time.Second))
			continue
		}
		return decodeRecoveryResponse(response, target)
	}
}

// Only the suffix after the emitted anchor is eligible for replay, including
// when collection originally started partway through a timestamp or REST page.
func recoverMarketTrades(ctx context.Context, anchor quanttick.TradeEvent,
	page func(context.Context, string) ([]quanttick.TradeEvent, string, error),
) ([]quanttick.TradeEvent, error) {
	var rows []quanttick.TradeEvent
	cursor := ""
	seen := make(map[string]bool)
	for range 100 {
		batch, next, err := page(ctx, cursor)
		if err != nil {
			return nil, err
		}
		for _, trade := range batch {
			if trade.Symbol != anchor.Symbol {
				return nil, fmt.Errorf("recovery returned unexpected symbol %s", trade.Symbol)
			}
			if seen[trade.UID] {
				return nil, fmt.Errorf("recovery repeated trade %s", trade.UID)
			}
			seen[trade.UID] = true
			if len(rows) > 0 && trade.Timestamp.After(rows[len(rows)-1].Timestamp) {
				return nil, fmt.Errorf("recovery trades are not ordered newest first")
			}
			if trade.UID == anchor.UID {
				if !trade.Timestamp.Equal(anchor.Timestamp) || !trade.Price.Equal(anchor.Price) || !trade.Notional.Equal(anchor.Notional) || trade.TickRule != anchor.TickRule {
					return nil, fmt.Errorf("recovery anchor %s changed", anchor.UID)
				}
				slices.Reverse(rows)
				return rows, nil
			}
			if trade.Timestamp.Before(anchor.Timestamp) {
				return nil, fmt.Errorf("recovery passed missing anchor %s", anchor.UID)
			}
			rows = append(rows, trade)
			if len(rows) >= marketTradeBufferLimit {
				return nil, fmt.Errorf("recovery exceeded %d trades", marketTradeBufferLimit)
			}
		}
		if next == "" {
			break
		}
		if next == cursor {
			return nil, fmt.Errorf("recovery cursor did not advance")
		}
		cursor = next
	}
	return nil, fmt.Errorf("%s %s recovery could not find emitted trade %s", anchor.Exchange, anchor.Symbol, anchor.UID)
}
