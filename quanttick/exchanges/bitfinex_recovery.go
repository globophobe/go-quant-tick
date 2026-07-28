package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

type bitfinexWebSocketMessage struct {
	data       []byte
	receivedAt time.Time
}

func (b *Bitfinex) awaitSubscriptions(
	ctx context.Context,
	conn *websocket.Conn,
) ([]bitfinexWebSocketMessage, error) {
	ackCtx, cancel := context.WithTimeout(ctx, b.SubscriptionTimeout)
	defer cancel()
	buffered := make([]bitfinexWebSocketMessage, 0)
	for !b.subscriptionsReady() {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("bitfinex subscription acknowledgement timed out after %s", b.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read bitfinex subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}
		receivedAt := time.Now().UTC()
		trimmed := bytes.TrimSpace(data)
		if len(trimmed) > 0 && trimmed[0] == '{' {
			if _, err := b.ParseTradeMessage(data, receivedAt); err != nil {
				return nil, err
			}
			continue
		}
		if len(buffered) >= bitfinexSubscriptionBufferLimit {
			return nil, fmt.Errorf(
				"bitfinex pre-acknowledgement message buffer exceeded %d events",
				bitfinexSubscriptionBufferLimit,
			)
		}
		buffered = append(buffered, bitfinexWebSocketMessage{data: append([]byte(nil), data...), receivedAt: receivedAt})
	}
	return buffered, nil
}

func (b *Bitfinex) startMessageReader(
	ctx context.Context,
	conn *websocket.Conn,
	backlog *tradeBacklog,
) (<-chan bitfinexWebSocketMessage, <-chan error) {
	messages := make(chan bitfinexWebSocketMessage, bitfinexSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(messages)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read bitfinex websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}
			message := bitfinexWebSocketMessage{data: data, receivedAt: time.Now().UTC()}
			if !backlog.reserve() {
				errs <- fmt.Errorf(
					"bitfinex websocket message buffer exceeded %d events",
					bitfinexSubscriptionBufferLimit,
				)
				_ = conn.CloseNow()
				return
			}
			select {
			case messages <- message:
			case <-ctx.Done():
				backlog.release()
				errs <- ctx.Err()
				return
			}
		}
	}()
	return messages, errs
}

func (b *Bitfinex) emitMessageTrades(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	message bitfinexWebSocketMessage,
) error {
	parsed, err := b.ParseTradeMessage(message.data, message.receivedAt)
	if err != nil {
		return err
	}
	for _, trade := range parsed {
		if err := sendTrade(ctx, trades, trade); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bitfinex) emitRecoveredTrade(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	trade quanttick.TradeEvent,
) error {
	tradeID, err := strconv.ParseInt(trade.UID, 10, 64)
	if err != nil {
		return fmt.Errorf("parse recovered bitfinex trade id %q: %w", trade.UID, err)
	}
	previousID, hadPreviousID := b.lastIDs[trade.Symbol]
	if hadPreviousID && tradeID <= previousID {
		return nil
	}
	trade.IsSequential = b.bitfinexTradeIsSequential(trade.Symbol, hadPreviousID, previousID, tradeID)
	if err := sendTrade(ctx, trades, trade); err != nil {
		return err
	}
	b.lastIDs[trade.Symbol] = tradeID
	return nil
}

func (b *Bitfinex) recoverTrades(ctx context.Context) ([]quanttick.TradeEvent, error) {
	if len(b.lastIDs) == 0 || strings.TrimSpace(b.RESTURL) == "" {
		return nil, nil
	}
	recovered := make([]quanttick.TradeEvent, 0)
	var recoveryErrors []error
	for _, requested := range b.Symbols {
		symbol := b.APISymbol(requested)
		cursor, ok := b.lastIDs[symbol]
		if !ok {
			continue
		}
		rows, err := b.recoverSymbol(ctx, symbol, cursor)
		recovered = append(recovered, rows...)
		if err != nil {
			b.recoveryGaps[symbol] = true
			recoveryErrors = append(recoveryErrors, err)
		}
	}
	sort.SliceStable(recovered, func(left, right int) bool {
		if !recovered[left].Timestamp.Equal(recovered[right].Timestamp) {
			return recovered[left].Timestamp.Before(recovered[right].Timestamp)
		}
		if recovered[left].Symbol != recovered[right].Symbol {
			return recovered[left].Symbol < recovered[right].Symbol
		}
		leftID, _ := strconv.ParseInt(recovered[left].UID, 10, 64)
		rightID, _ := strconv.ParseInt(recovered[right].UID, 10, 64)
		return leftID < rightID
	})
	return recovered, errors.Join(recoveryErrors...)
}

func (b *Bitfinex) recoverSymbol(
	ctx context.Context,
	symbol string,
	cursor int64,
) ([]quanttick.TradeEvent, error) {
	recovered := make([]quanttick.TradeEvent, 0)
	var endMillis int64
	for page := 0; page < bitfinexRecoveryMaxPages; page++ {
		endpoint, err := url.Parse(
			strings.TrimRight(b.RESTURL, "/") + "/trades/" + url.PathEscape(symbol) + "/hist",
		)
		if err != nil {
			return nil, fmt.Errorf("build bitfinex recovery URL: %w", err)
		}
		query := endpoint.Query()
		query.Set("limit", strconv.Itoa(bitfinexRecoveryPageLimit))
		query.Set("sort", "-1")
		if endMillis > 0 {
			query.Set("end", strconv.FormatInt(endMillis, 10))
		}
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("build bitfinex recovery request: %w", err)
		}
		if err := b.recoveryThrottle.wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for bitfinex recovery rate limit: %w", err)
		}
		response, err := b.HTTPClient.Do(request)
		if err != nil {
			return nil, fmt.Errorf("fetch bitfinex recovery for %s: %w", symbol, err)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			delay, retryErr := retryAfterDelay(response.Header, time.Now())
			response.Body.Close()
			if retryErr != nil {
				return nil, fmt.Errorf("fetch bitfinex recovery for %s: %w", symbol, retryErr)
			}
			if delay <= 0 {
				delay = bitfinexRecoveryRequestInterval
			}
			b.recoveryThrottle.deferFor(delay)
			page--
			continue
		}
		var payloads []json.RawMessage
		if err := decodeRecoveryResponse(response, &payloads); err != nil {
			return nil, fmt.Errorf("fetch bitfinex recovery for %s: %w", symbol, err)
		}
		if len(payloads) == 0 {
			return nil, fmt.Errorf("bitfinex recovery for %s did not find cursor %d", symbol, cursor)
		}
		oldestMillis := int64(0)
		foundCursor := false
		for _, payload := range payloads {
			update, err := parseBitfinexTradeUpdate(payload, time.Now().UTC())
			if err != nil {
				return nil, err
			}
			if oldestMillis == 0 || update.TimestampMillis < oldestMillis {
				oldestMillis = update.TimestampMillis
			}
			if update.TradeID == cursor {
				foundCursor = true
				continue
			}
			if update.TradeID > cursor {
				recovered = append(recovered, newBitfinexTradeEvent(symbol, update, true))
			}
		}
		if foundCursor {
			sort.SliceStable(recovered, func(left, right int) bool {
				leftID, _ := strconv.ParseInt(recovered[left].UID, 10, 64)
				rightID, _ := strconv.ParseInt(recovered[right].UID, 10, 64)
				return leftID < rightID
			})
			return recovered, nil
		}
		if oldestMillis <= 0 || oldestMillis-1 == endMillis {
			return nil, fmt.Errorf("bitfinex recovery for %s pagination did not advance", symbol)
		}
		endMillis = oldestMillis - 1
	}
	return nil, fmt.Errorf(
		"bitfinex recovery for %s exceeded %d pages without cursor %d",
		symbol,
		bitfinexRecoveryMaxPages,
		cursor,
	)
}
