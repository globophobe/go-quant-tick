package exchanges

import (
	"context"
	"net/http"
	"testing"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func TestTradesAfterCursorNewestFirstPreservesSourceOrder(t *testing.T) {
	timestamp := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	newestFirst := []quanttick.TradeEvent{
		{UID: "opaque-c", Symbol: "BTCUSDT", Timestamp: timestamp},
		{UID: "opaque-b", Symbol: "BTCUSDT", Timestamp: timestamp},
		{UID: "opaque-a", Symbol: "BTCUSDT", Timestamp: timestamp},
	}

	recovered, found := tradesAfterCursorNewestFirst(newestFirst, "opaque-a")
	if !found {
		t.Fatal("cursor not found")
	}
	sortTradeEventsChronologically(recovered)
	assertStrings(t, tradeUIDs(recovered), []string{"opaque-b", "opaque-c"})
}

func TestRequestWindowLimiterRunsImmediatelyUntilCapacity(t *testing.T) {
	limiter := newRequestWindowLimiter(2, time.Minute)
	if err := limiter.wait(context.Background(), 1); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := limiter.wait(context.Background(), 1); err != nil {
		t.Fatalf("second request: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := limiter.wait(ctx, 1); err == nil {
		t.Fatal("request beyond capacity should wait for the window")
	}
}

func TestRequestWindowReservationRefundsUnusedWeight(t *testing.T) {
	limiter := newRequestWindowLimiter(2, time.Minute)
	reservation, err := limiter.reserve(context.Background(), 2)
	if err != nil {
		t.Fatalf("reserve request weight: %v", err)
	}
	reservation.refund(1)
	if err := limiter.wait(context.Background(), 1); err != nil {
		t.Fatalf("use refunded request weight: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := limiter.wait(ctx, 1); err == nil {
		t.Fatal("refund should not release more weight than requested")
	}
}

func TestTradeBacklogBoundsBufferedAndQueuedTradesTogether(t *testing.T) {
	backlog, err := newTradeBacklog(2, 1)
	if err != nil {
		t.Fatalf("new backlog: %v", err)
	}
	if !backlog.reserve() {
		t.Fatal("second backlog slot should be available")
	}
	if backlog.reserve() {
		t.Fatal("backlog should reject a third outstanding trade")
	}
	backlog.release()
	if !backlog.reserve() {
		t.Fatal("released backlog slot should be reusable")
	}
}

func TestRetryAfterDelay(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	headers := make(http.Header)
	headers.Set("Retry-After", "2.5")
	delay, err := retryAfterDelay(headers, now)
	if err != nil {
		t.Fatalf("retry delay: %v", err)
	}
	if delay != 2500*time.Millisecond {
		t.Fatalf("retry delay = %s, want 2.5s", delay)
	}
}
