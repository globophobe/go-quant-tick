package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type testBucketFlushBeforeer struct {
	calls chan bucketFlushRequest
	flush func(context.Context, bucketFlushRequest) (int, error)
}

func (f *testBucketFlushBeforeer) FlushBefore(
	ctx context.Context,
	exchange string,
	symbol string,
	timestamp time.Time,
) (int, error) {
	request := bucketFlushRequest{exchange: exchange, symbol: symbol, timestamp: timestamp}
	f.calls <- request
	if f.flush != nil {
		return f.flush(ctx, request)
	}
	return 0, nil
}

func receiveBucketFlush(t *testing.T, calls <-chan bucketFlushRequest) bucketFlushRequest {
	t.Helper()
	select {
	case request := <-calls:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bucket flush")
		return bucketFlushRequest{}
	}
}

func noBucketFlushRetryDelay(int) time.Duration {
	return 0
}

func TestBucketFlushWorkerCoalescesLatestPendingWatermark(t *testing.T) {
	calls := make(chan bucketFlushRequest, 4)
	releaseFirst := make(chan struct{})
	callCount := 0
	flusher := &testBucketFlushBeforeer{
		calls: calls,
		flush: func(ctx context.Context, _ bucketFlushRequest) (int, error) {
			callCount++
			if callCount == 1 {
				select {
				case <-releaseFirst:
					return 0, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			return 0, nil
		},
	}
	worker := newBucketFlushWorkerWithRetry(context.Background(), flusher, time.Second, nil, noBucketFlushRetryDelay)
	defer worker.Close()

	start := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	latest := start.Add(3 * time.Minute)
	worker.Request("coinbase", "BTC-USD", start)
	first := receiveBucketFlush(t, calls)
	if !first.timestamp.Equal(start) {
		t.Fatalf("first timestamp = %s, want %s", first.timestamp, start)
	}

	worker.Request("coinbase", "BTC-USD", start.Add(time.Minute))
	worker.Request("coinbase", "BTC-USD", latest)
	worker.Request("coinbase", "BTC-USD", start.Add(2*time.Minute))
	close(releaseFirst)

	second := receiveBucketFlush(t, calls)
	if !second.timestamp.Equal(latest) {
		t.Fatalf("coalesced timestamp = %s, want %s", second.timestamp, latest)
	}
}

func TestBucketFlushWorkerTimesOutAndRetriesNewestWatermark(t *testing.T) {
	calls := make(chan bucketFlushRequest, 4)
	firstCall := true
	flusher := &testBucketFlushBeforeer{
		calls: calls,
		flush: func(ctx context.Context, _ bucketFlushRequest) (int, error) {
			if firstCall {
				firstCall = false
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 0, nil
		},
	}
	reported := make(chan error, 1)
	worker := newBucketFlushWorkerWithRetry(
		context.Background(),
		flusher,
		15*time.Millisecond,
		func(err error) { reported <- err },
		noBucketFlushRetryDelay,
	)
	defer worker.Close()

	start := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	newer := start.Add(time.Minute)
	worker.Request("bitmex", "XBTUSD", start)
	receiveBucketFlush(t, calls)
	worker.Request("bitmex", "XBTUSD", newer)

	select {
	case err := <-reported:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("reported error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for flush timeout")
	}
	second := receiveBucketFlush(t, calls)
	if !second.timestamp.Equal(newer) {
		t.Fatalf("retried timestamp = %s, want newer %s", second.timestamp, newer)
	}
}

func TestBucketFlushWorkerRequestDoesNotWaitForDatabase(t *testing.T) {
	calls := make(chan bucketFlushRequest, 4)
	release := make(chan struct{})
	flusher := &testBucketFlushBeforeer{
		calls: calls,
		flush: func(ctx context.Context, _ bucketFlushRequest) (int, error) {
			select {
			case <-release:
				return 0, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		},
	}
	worker := newBucketFlushWorkerWithRetry(context.Background(), flusher, time.Second, nil, noBucketFlushRetryDelay)
	defer worker.Close()

	worker.Request("binance", "BTCUSDT", time.Now().UTC())
	receiveBucketFlush(t, calls)

	requested := make(chan struct{})
	go func() {
		worker.Request("coinbase", "BTC-USD", time.Now().UTC())
		close(requested)
	}()
	select {
	case <-requested:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("request blocked on in-flight database flush")
	}
	close(release)
}

func TestBucketFlushWorkerCloseCancelsAndJoinsInFlightFlush(t *testing.T) {
	calls := make(chan bucketFlushRequest, 1)
	returned := make(chan struct{})
	flusher := &testBucketFlushBeforeer{
		calls: calls,
		flush: func(ctx context.Context, _ bucketFlushRequest) (int, error) {
			<-ctx.Done()
			close(returned)
			return 0, ctx.Err()
		},
	}
	worker := newBucketFlushWorker(context.Background(), flusher, time.Second, nil)
	worker.Request("hyperliquid", "BTC", time.Now().UTC())
	receiveBucketFlush(t, calls)

	closed := make(chan struct{})
	go func() { worker.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("worker close did not join")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("in-flight flush did not observe cancellation")
	}
}

func TestBucketFlushWorkerLongRetryDoesNotDelayHealthyKey(t *testing.T) {
	calls := make(chan bucketFlushRequest, 4)
	releaseFirst := make(chan struct{})
	firstCall := true
	flusher := &testBucketFlushBeforeer{
		calls: calls,
		flush: func(_ context.Context, _ bucketFlushRequest) (int, error) {
			if firstCall {
				firstCall = false
				<-releaseFirst
				return 0, errors.New("database unavailable")
			}
			return 0, nil
		},
	}
	reported := make(chan error, 1)
	worker := newBucketFlushWorkerWithRetry(context.Background(), flusher, time.Second, func(err error) { reported <- err }, func(int) time.Duration { return time.Hour })
	defer worker.Close()

	start := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	worker.Request("a", "BTC", start)
	first := receiveBucketFlush(t, calls)
	if first.exchange != "a" {
		t.Fatalf("first exchange = %s, want a", first.exchange)
	}
	worker.Request("b", "BTC", start)
	close(releaseFirst)

	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed flush report")
	}
	select {
	case second := <-calls:
		if second.exchange != "b" {
			t.Fatalf("second exchange = %s, want healthy b", second.exchange)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("healthy key waited for failed key retry delay")
	}
}

func TestBucketFlushRetryDelayIsExponentialAndBounded(t *testing.T) {
	first := bucketFlushRetryDelay(1)
	second := bucketFlushRetryDelay(2)
	if second <= first {
		t.Fatalf("second retry delay = %s, want greater than first %s", second, first)
	}
	capped := bucketFlushRetryDelay(100)
	if capped < 22500*time.Millisecond {
		t.Fatalf("capped retry delay = %s, want at least 22.5s", capped)
	}
	if capped > 30*time.Second {
		t.Fatalf("capped retry delay = %s, want at most 30s", capped)
	}
}

func receiveRetryFailure(t *testing.T, retries <-chan int) int {
	t.Helper()
	select {
	case failures := <-retries:
		return failures
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry")
		return 0
	}
}

func TestBucketFlushWorkerTracksRetryFailuresPerKey(t *testing.T) {
	calls := make(chan bucketFlushRequest, 4)
	releaseFirst := make(chan struct{})
	aCalls := 0
	flusher := &testBucketFlushBeforeer{
		calls: calls,
		flush: func(_ context.Context, request bucketFlushRequest) (int, error) {
			if request.exchange == "a" {
				aCalls++
				if aCalls == 1 {
					<-releaseFirst
				}
				if aCalls <= 2 {
					return 0, errors.New("bad market row")
				}
			}
			return 0, nil
		},
	}
	retries := make(chan int, 2)
	worker := newBucketFlushWorkerWithRetry(
		context.Background(),
		flusher,
		time.Second,
		nil,
		func(failures int) time.Duration { retries <- failures; return 0 },
	)
	defer worker.Close()

	start := time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC)
	worker.Request("a", "BTC", start)
	receiveBucketFlush(t, calls)
	worker.Request("b", "BTC", start)
	close(releaseFirst)

	if failures := receiveRetryFailure(t, retries); failures != 1 {
		t.Fatalf("first retry failures = %d, want 1", failures)
	}
	second := receiveBucketFlush(t, calls)
	if second.exchange != "b" {
		t.Fatalf("second exchange = %s, want b", second.exchange)
	}
	third := receiveBucketFlush(t, calls)
	if third.exchange != "a" {
		t.Fatalf("third exchange = %s, want retried a", third.exchange)
	}
	if failures := receiveRetryFailure(t, retries); failures != 2 {
		t.Fatalf("second retry failures = %d, want 2", failures)
	}
	fourth := receiveBucketFlush(t, calls)
	if fourth.exchange != "a" {
		t.Fatalf("fourth exchange = %s, want retried a", fourth.exchange)
	}
}
