package exchanges

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestWebSocketDialErrorClassifiesHTTPStatus(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		want       bool
		wantKnown  bool
	}{
		{name: "rate limited", statusCode: http.StatusTooManyRequests, want: true, wantKnown: true},
		{name: "server error", statusCode: http.StatusServiceUnavailable, want: true, wantKnown: true},
		{name: "forbidden", statusCode: http.StatusForbidden, want: false, wantKnown: true},
		{name: "no response", statusCode: 0, want: false, wantKnown: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var response *http.Response
			if tc.statusCode != 0 {
				response = &http.Response{StatusCode: tc.statusCode}
			}
			err := newWebSocketDialError("test", response, errors.New("dial failed"))
			got, known := err.ClassifyTransient()
			if got != tc.want || known != tc.wantKnown {
				t.Fatalf("ClassifyTransient() = (%v, %v), want (%v, %v)", got, known, tc.want, tc.wantKnown)
			}
			if !errors.Is(err, err.err) {
				t.Fatal("dial error does not unwrap its cause")
			}
		})
	}
}

func TestDialWebSocketReturnsTypedHandshakeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	_, err := dialWebSocket(context.Background(), "test", url)
	if err == nil {
		t.Fatal("dial succeeded")
	}
	var dialErr *websocketDialError
	if !errors.As(err, &dialErr) {
		t.Fatalf("error type = %T, want *websocketDialError", err)
	}
	transient, known := dialErr.ClassifyTransient()
	if !known || !transient {
		t.Fatal("503 handshake error is not transient")
	}
}

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
