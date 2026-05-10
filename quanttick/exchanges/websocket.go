package exchanges

import "github.com/coder/websocket"

const maxWebSocketMessageBytes int64 = 16 << 20

func isNormalWebSocketClose(err error) bool {
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return true
	default:
		return false
	}
}
