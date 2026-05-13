package exchanges

import (
	"errors"
	"io"
	"syscall"

	"github.com/coder/websocket"
)

const maxWebSocketMessageBytes int64 = 16 << 20

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
