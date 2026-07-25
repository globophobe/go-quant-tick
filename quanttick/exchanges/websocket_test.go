package exchanges

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestIsNormalWebSocketClose(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "normal closure",
			err:  websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: "Bye"},
			want: true,
		},
		{
			name: "wrapped normal closure",
			err: fmt.Errorf(
				"read websocket: %w",
				websocket.CloseError{Code: websocket.StatusNormalClosure},
			),
			want: true,
		},
		{
			name: "going away",
			err:  websocket.CloseError{Code: websocket.StatusGoingAway},
			want: true,
		},
		{
			name: "eof",
			err:  io.EOF,
			want: true,
		},
		{
			name: "wrapped eof",
			err:  fmt.Errorf("read websocket: failed to read frame header: %w", io.EOF),
			want: true,
		},
		{
			name: "wrapped connection reset",
			err: fmt.Errorf(
				"read websocket: failed to read frame header: %w",
				&net.OpError{
					Op:  "read",
					Net: "tcp",
					Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
				},
			),
			want: true,
		},
		{
			name: "abnormal closure",
			err:  websocket.CloseError{Code: websocket.StatusAbnormalClosure},
			want: false,
		},
		{
			name: "non close error",
			err:  fmt.Errorf("boom"),
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isNormalWebSocketClose(tc.err)
			if got != tc.want {
				t.Fatalf("isNormalWebSocketClose() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconnectBackoffGrowsCapsAndResetsAfterStableSession(t *testing.T) {
	backoff := newReconnectBackoff(time.Second)
	backoff.jitter = func(delay, _ time.Duration) time.Duration { return delay }

	for i, want := range []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	} {
		if got := backoff.Next(0); got != want {
			t.Fatalf("delay %d = %s, want %s", i, got, want)
		}
	}
	if got := backoff.Next(websocketStableSession); got != time.Second {
		t.Fatalf("delay after stable session = %s, want %s", got, time.Second)
	}
}

func TestReconnectBackoffPreservesZeroAndLargeConfiguredDelay(t *testing.T) {
	zero := newReconnectBackoff(0)
	if got := zero.Next(0); got != 0 {
		t.Fatalf("zero delay = %s, want 0", got)
	}

	large := newReconnectBackoff(time.Minute)
	large.jitter = func(delay, _ time.Duration) time.Duration { return delay }
	if got := large.Next(0); got != time.Minute {
		t.Fatalf("large configured delay = %s, want %s", got, time.Minute)
	}
	if got := large.Next(0); got != time.Minute {
		t.Fatalf("large capped delay = %s, want %s", got, time.Minute)
	}
}

func TestSeenTradeIDsDeduplicatesWithinBound(t *testing.T) {
	seen := newSeenTradeIDs(2)
	if !seen.Add("BTC-USD", "1") {
		t.Fatal("first trade should be new")
	}
	if seen.Add("BTC-USD", "1") {
		t.Fatal("duplicate trade should be rejected")
	}
	if !seen.Add("ETH-USD", "1") {
		t.Fatal("same ID on another symbol should be new")
	}
	if !seen.Add("BTC-USD", "2") {
		t.Fatal("third key should be new")
	}
	if !seen.Add("BTC-USD", "1") {
		t.Fatal("evicted key should become eligible again")
	}
}
