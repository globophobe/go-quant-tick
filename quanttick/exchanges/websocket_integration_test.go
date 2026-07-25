package exchanges

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func newExchangeWebSocketServer(
	t *testing.T,
	handler func(context.Context, *websocket.Conn) error,
) string {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept test websocket: %v", err)
			return
		}
		defer conn.CloseNow()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := handler(ctx, conn); err != nil && ctx.Err() == nil {
			t.Errorf("serve test websocket: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func readExchangeWebSocketMessage(ctx context.Context, conn *websocket.Conn) ([]byte, error) {
	messageType, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("message type = %v, want text", messageType)
	}
	return data, nil
}

func writeExchangeWebSocketMessage(ctx context.Context, conn *websocket.Conn, message string) error {
	return conn.Write(ctx, websocket.MessageText, []byte(message))
}
