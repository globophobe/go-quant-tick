package exchanges

import (
	"errors"
	"io"
	"math/rand"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

const maxWebSocketMessageBytes int64 = 16 << 20

const (
	websocketSubscriptionTimeout = 10 * time.Second
	websocketReconnectMaxDelay   = 30 * time.Second
	websocketStableSession       = time.Minute
)

type reconnectBackoff struct {
	base     time.Duration
	max      time.Duration
	failures uint
	jitter   func(time.Duration, time.Duration) time.Duration
}

func newReconnectBackoff(base time.Duration) *reconnectBackoff {
	maximum := websocketReconnectMaxDelay
	if base > maximum {
		maximum = base
	}
	return &reconnectBackoff{
		base:   base,
		max:    maximum,
		jitter: jitterReconnectDelay,
	}
}

func (b *reconnectBackoff) Next(sessionDuration time.Duration) time.Duration {
	if b.base <= 0 {
		return 0
	}
	if sessionDuration >= websocketStableSession {
		b.failures = 0
	}
	b.failures++

	delay := b.base
	for attempt := uint(1); attempt < b.failures && delay < b.max; attempt++ {
		if delay > b.max/2 {
			delay = b.max
			break
		}
		delay *= 2
	}
	if delay > b.max {
		delay = b.max
	}
	return b.jitter(delay, b.max)
}

func jitterReconnectDelay(delay, maximum time.Duration) time.Duration {
	// A small two-sided jitter keeps independent collectors from reconnecting in
	// lockstep while preserving the configured delay as the nominal first retry.
	factor := 0.8 + rand.Float64()*0.4
	jittered := time.Duration(float64(delay) * factor)
	if jittered > maximum {
		return maximum
	}
	return jittered
}

func isNormalWebSocketClose(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return true
	default:
		return false
	}
}
