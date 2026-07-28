package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	recoveryResponseErrorLimit = 4096
	reconnectRecoveryTimeout   = 15 * time.Second
)

var defaultRecoveryHTTPClient = &http.Client{Timeout: 10 * time.Second}

type restThrottle struct {
	mu          sync.Mutex
	minInterval time.Duration
	lastRequest time.Time
	nextRequest time.Time
}

type windowUsage struct {
	id        uint64
	timestamp time.Time
	weight    int
}

type requestWindowLimiter struct {
	mu          sync.Mutex
	capacity    int
	window      time.Duration
	used        int
	nextUsageID uint64
	usage       []windowUsage
}

type requestWindowReservation struct {
	limiter *requestWindowLimiter
	usageID uint64
}

func newRequestWindowLimiter(capacity int, window time.Duration) *requestWindowLimiter {
	return &requestWindowLimiter{capacity: capacity, window: window}
}

func (l *requestWindowLimiter) wait(ctx context.Context, weight int) error {
	_, err := l.reserve(ctx, weight)
	return err
}

func (l *requestWindowLimiter) reserve(
	ctx context.Context,
	weight int,
) (*requestWindowReservation, error) {
	if weight <= 0 {
		return &requestWindowReservation{}, nil
	}
	if weight > l.capacity {
		return nil, fmt.Errorf(
			"request weight %d exceeds window capacity %d",
			weight,
			l.capacity,
		)
	}
	for {
		l.mu.Lock()
		now := time.Now()
		l.prune(now)
		if l.used+weight <= l.capacity {
			l.nextUsageID++
			usageID := l.nextUsageID
			l.usage = append(l.usage, windowUsage{
				id:        usageID,
				timestamp: now,
				weight:    weight,
			})
			l.used += weight
			l.mu.Unlock()
			return &requestWindowReservation{limiter: l, usageID: usageID}, nil
		}

		needed := l.used + weight - l.capacity
		released := 0
		allowedAt := now.Add(l.window)
		for _, usage := range l.usage {
			released += usage.weight
			if released >= needed {
				allowedAt = usage.timestamp.Add(l.window)
				break
			}
		}
		l.mu.Unlock()
		if err := sleepContext(ctx, time.Until(allowedAt)); err != nil {
			return nil, err
		}
	}
}

func (r *requestWindowReservation) refund(weight int) {
	if r == nil || r.limiter == nil || r.usageID == 0 || weight <= 0 {
		return
	}
	r.limiter.refund(r.usageID, weight)
}

func (l *requestWindowLimiter) refund(usageID uint64, weight int) {
	if weight <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(time.Now())
	for index := range l.usage {
		usage := &l.usage[index]
		if usage.id != usageID {
			continue
		}
		if weight > usage.weight {
			weight = usage.weight
		}
		usage.weight -= weight
		l.used -= weight
		if usage.weight == 0 {
			l.usage = slices.Delete(l.usage, index, index+1)
		}
		return
	}
}

func (l *requestWindowLimiter) prune(now time.Time) {
	cutoff := now.Add(-l.window)
	index := 0
	for index < len(l.usage) && !l.usage[index].timestamp.After(cutoff) {
		l.used -= l.usage[index].weight
		index++
	}
	if index > 0 {
		l.usage = append(l.usage[:0], l.usage[index:]...)
	}
}

func newRESTThrottle(minInterval time.Duration) *restThrottle {
	return &restThrottle{minInterval: minInterval}
}

func (t *restThrottle) wait(ctx context.Context) error {
	for {
		t.mu.Lock()
		now := time.Now()
		allowedAt := t.nextRequest
		minimumAt := t.lastRequest.Add(t.minInterval)
		if minimumAt.After(allowedAt) {
			allowedAt = minimumAt
		}
		if !allowedAt.After(now) {
			t.lastRequest = now
			t.mu.Unlock()
			return nil
		}
		t.mu.Unlock()

		if err := sleepContext(ctx, time.Until(allowedAt)); err != nil {
			return err
		}
	}
}

func (t *restThrottle) deferUntil(timestamp time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if timestamp.After(t.nextRequest) {
		t.nextRequest = timestamp
	}
}

func (t *restThrottle) deferFor(delay time.Duration) {
	if delay <= 0 {
		return
	}
	t.deferUntil(time.Now().Add(delay))
}

type tradeBacklog struct {
	slots chan struct{}
}

func newTradeBacklog(limit int, initial int) (*tradeBacklog, error) {
	if initial > limit {
		return nil, fmt.Errorf("websocket trade backlog has %d events, limit is %d", initial, limit)
	}
	backlog := &tradeBacklog{slots: make(chan struct{}, limit)}
	for range initial {
		backlog.slots <- struct{}{}
	}
	return backlog, nil
}

func (b *tradeBacklog) reserve() bool {
	select {
	case b.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (b *tradeBacklog) release() {
	<-b.slots
}

func stopTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func retryAfterDelay(headers http.Header, now time.Time) (time.Duration, error) {
	value := strings.TrimSpace(headers.Get("Retry-After"))
	if value == "" {
		return 0, nil
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		if seconds <= 0 {
			return 0, nil
		}
		return time.Duration(seconds * float64(time.Second)), nil
	}
	timestamp, err := http.ParseTime(value)
	if err != nil {
		return 0, fmt.Errorf("parse Retry-After %q: %w", value, err)
	}
	if !timestamp.After(now) {
		return 0, nil
	}
	return timestamp.Sub(now), nil
}

func decodeRecoveryResponse(response *http.Response, target any) error {
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, recoveryResponseErrorLimit))
		return fmt.Errorf("HTTP %s: %s", response.Status, body)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func tradesAfterCursorNewestFirst(
	trades []quanttick.TradeEvent,
	cursorUID string,
) ([]quanttick.TradeEvent, bool) {
	for index, trade := range trades {
		if trade.UID != cursorUID {
			continue
		}
		recovered := append([]quanttick.TradeEvent(nil), trades[:index]...)
		slices.Reverse(recovered)
		return recovered, true
	}
	return nil, false
}

func sortTradeEventsChronologically(trades []quanttick.TradeEvent) {
	sort.SliceStable(trades, func(left, right int) bool {
		if !trades[left].Timestamp.Equal(trades[right].Timestamp) {
			return trades[left].Timestamp.Before(trades[right].Timestamp)
		}
		if trades[left].Symbol != trades[right].Symbol {
			return trades[left].Symbol < trades[right].Symbol
		}
		return false
	})
}

func emitSeenTrade(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	seen *seenTradeIDs,
	lastUIDs map[string]string,
	trade quanttick.TradeEvent,
) error {
	if !seen.Add(trade.Symbol, trade.UID) {
		return nil
	}
	if err := sendTrade(ctx, trades, trade); err != nil {
		return err
	}
	lastUIDs[trade.Symbol] = trade.UID
	return nil
}
