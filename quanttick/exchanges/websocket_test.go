package exchanges

import (
	"fmt"
	"testing"

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
