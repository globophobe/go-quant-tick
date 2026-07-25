package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type bucketFlushBeforeer interface {
	FlushBefore(context.Context, string, string, time.Time) (int, error)
}

type bucketFlushRequest struct {
	exchange  string
	symbol    string
	timestamp time.Time
	failures  int
	notBefore time.Time
}

type bucketFlushKey struct {
	exchange string
	symbol   string
}

type bucketFlushWorker struct {
	flusher   bucketFlushBeforeer
	timeout   time.Duration
	onError   func(error)
	wake      chan struct{}
	done      chan struct{}
	cancel    context.CancelFunc
	retryWait func(int) time.Duration
	mu        sync.Mutex
	pending   map[bucketFlushKey]bucketFlushRequest
	order     []bucketFlushKey
	accepting bool
}

func newBucketFlushWorker(
	ctx context.Context,
	flusher bucketFlushBeforeer,
	timeout time.Duration,
	onError func(error),
) *bucketFlushWorker {
	return newBucketFlushWorkerWithRetry(ctx, flusher, timeout, onError, bucketFlushRetryDelay)
}

func newBucketFlushWorkerWithRetry(
	ctx context.Context,
	flusher bucketFlushBeforeer,
	timeout time.Duration,
	onError func(error),
	retryWait func(int) time.Duration,
) *bucketFlushWorker {
	workerCtx, cancel := context.WithCancel(ctx)
	worker := &bucketFlushWorker{
		flusher:   flusher,
		timeout:   timeout,
		onError:   onError,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		cancel:    cancel,
		retryWait: retryWait,
		pending:   make(map[bucketFlushKey]bucketFlushRequest),
		accepting: true,
	}
	go worker.run(workerCtx)
	return worker
}

func (w *bucketFlushWorker) Request(exchange string, symbol string, timestamp time.Time) {
	request := bucketFlushRequest{
		exchange:  exchange,
		symbol:    symbol,
		timestamp: timestamp.UTC(),
	}
	key := bucketFlushKey{exchange: exchange, symbol: symbol}

	shouldSignal := false
	w.mu.Lock()
	if !w.accepting {
		w.mu.Unlock()
		return
	}
	if pending, ok := w.pending[key]; !ok {
		w.pending[key] = request
		w.order = append(w.order, key)
		shouldSignal = true
	} else if request.timestamp.After(pending.timestamp) {
		request.failures = pending.failures
		request.notBefore = pending.notBefore
		w.pending[key] = request
	}
	w.mu.Unlock()
	if shouldSignal {
		w.signal()
	}
}

func (w *bucketFlushWorker) Close() {
	w.mu.Lock()
	w.accepting = false
	w.mu.Unlock()
	w.cancel()
	<-w.done
}

func (w *bucketFlushWorker) run(ctx context.Context) {
	defer close(w.done)
	for {
		request, delay, ok := w.take(time.Now())
		if !ok {
			if !w.waitForWork(ctx, delay) {
				return
			}
			continue
		}

		flushCtx, cancel := context.WithTimeout(ctx, w.timeout)
		_, err := w.flusher.FlushBefore(flushCtx, request.exchange, request.symbol, request.timestamp)
		cancel()
		if err == nil {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		request.failures++
		request.notBefore = time.Now().Add(w.retryWait(request.failures))
		w.requeue(request)
		if w.onError != nil {
			w.onError(fmt.Errorf(
				"flush websocket data before %s %s %s: %w",
				request.exchange,
				request.symbol,
				request.timestamp.Format(time.RFC3339Nano),
				err,
			))
		}
	}
}

func (w *bucketFlushWorker) take(now time.Time) (bucketFlushRequest, time.Duration, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return bucketFlushRequest{}, 0, false
	}

	readyIndex := -1
	var nextAttempt time.Time
	for index, key := range w.order {
		request := w.pending[key]
		if !request.notBefore.After(now) {
			readyIndex = index
			break
		}
		if nextAttempt.IsZero() || request.notBefore.Before(nextAttempt) {
			nextAttempt = request.notBefore
		}
	}
	if readyIndex == -1 {
		return bucketFlushRequest{}, nextAttempt.Sub(now), false
	}

	key := w.order[readyIndex]
	copy(w.order[readyIndex:], w.order[readyIndex+1:])
	w.order[len(w.order)-1] = bucketFlushKey{}
	w.order = w.order[:len(w.order)-1]
	request := w.pending[key]
	delete(w.pending, key)
	return request, 0, true
}

func (w *bucketFlushWorker) requeue(request bucketFlushRequest) {
	key := bucketFlushKey{exchange: request.exchange, symbol: request.symbol}
	w.mu.Lock()
	if w.accepting {
		if pending, ok := w.pending[key]; !ok {
			w.pending[key] = request
			w.order = append(w.order, key)
		} else {
			if request.timestamp.After(pending.timestamp) {
				pending.timestamp = request.timestamp
			}
			if request.failures > pending.failures {
				pending.failures = request.failures
			}
			if request.notBefore.After(pending.notBefore) {
				pending.notBefore = request.notBefore
			}
			w.pending[key] = pending
		}
	}
	w.mu.Unlock()
}

func (w *bucketFlushWorker) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *bucketFlushWorker) waitForWork(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		case <-w.wake:
			return true
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-w.wake:
		return true
	case <-timer.C:
		return true
	}
}

func bucketFlushRetryDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	delay := 250 * time.Millisecond
	for attempt := 1; attempt < failures && delay < 30*time.Second; attempt++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	// Keep retries from synchronizing across collectors while retaining a firm
	// upper bound on outage traffic.
	jitter := delay / 4
	return delay - jitter + time.Duration(rand.Int64N(int64(jitter)+1))
}
