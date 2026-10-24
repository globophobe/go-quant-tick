package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const marketTradeBufferLimit = 10000

// marketStream owns subscription, bounded buffering and REST handoff for venues
// with individual fill IDs. Protocol parsing and recovery pagination belong to
// the venue; only emitted trades advance its recovery anchor.
type marketStream struct {
	Symbols             []string
	URL                 string
	RESTURL             string
	HTTPClient          *http.Client
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	HeartbeatInterval   time.Duration

	name          string
	prepare       func(context.Context) error
	subscription  func() any
	parse         func([]byte, time.Time) (marketMessage, error)
	recoverSymbol func(context.Context, string, string) ([]quanttick.TradeEvent, error)
	requireAck    bool
	textPing      bool
	last          map[string]quanttick.TradeEvent
	seen          map[string]*seenTradeIDs
	throttle      *restThrottle
}

type marketMessage struct {
	trades       []quanttick.TradeEvent
	acknowledged string
	pong         bool
}

func newMarketStream(name, endpoint, restURL string, symbols []string) *marketStream {
	return &marketStream{
		name:                name,
		URL:                 endpoint,
		RESTURL:             restURL,
		Symbols:             append([]string(nil), symbols...),
		HTTPClient:          defaultRecoveryHTTPClient,
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		HeartbeatInterval:   20 * time.Second,
		last:                make(map[string]quanttick.TradeEvent),
		seen:                make(map[string]*seenTradeIDs),
		throttle:            newRESTThrottle(time.Second / 2),
	}
}

func (m *marketStream) Name() string { return m.name }

func (m *marketStream) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(m.ReconnectDelay)
		for ctx.Err() == nil {
			started := time.Now()
			if err := m.run(ctx, trades, errs); err != nil && ctx.Err() == nil {
				sendError(ctx, errs, fmt.Errorf("%s: %w", m.name, err))
			}
			if err := sleepContext(ctx, backoff.Next(time.Since(started))); err != nil {
				return
			}
		}
	}()
	return trades, errs
}

func (m *marketStream) run(ctx context.Context, trades chan<- quanttick.TradeEvent, errs chan<- error) error {
	if m.prepare != nil {
		prepareCtx, cancel := context.WithTimeout(ctx, reconnectRecoveryTimeout)
		err := m.prepare(prepareCtx)
		cancel()
		if err != nil {
			return err
		}
	}
	conn, err := dialWebSocket(ctx, m.name, m.URL)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	subscribeCtx, cancel := context.WithTimeout(ctx, m.SubscriptionTimeout)
	data, err := json.Marshal(m.subscription())
	if err == nil {
		err = conn.Write(subscribeCtx, websocket.MessageText, data)
	}
	var buffered []quanttick.TradeEvent
	if err == nil && m.requireAck {
		buffered, err = m.awaitSubscription(subscribeCtx, conn)
	}
	cancel()
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	backlog, err := newTradeBacklog(marketTradeBufferLimit, len(buffered))
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	pongs := make(chan struct{}, 1)
	stream, streamErr := m.startReader(streamCtx, conn, backlog, pongs)
	if m.HeartbeatInterval > 0 {
		go m.heartbeat(streamCtx, conn, pongs)
	}
	cursors := make(map[string]string, len(m.last))
	for symbol, trade := range m.last {
		cursors[symbol] = trade.UID
	}
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, buffered, recoveryErr := recoverWithWebSocketOverlap(
		recoveryCtx, m.name, m.Symbols, cursors, m.recoverSymbol, time.Second/2,
		buffered, stream, streamErr)
	cancelRecovery()
	if recoveryErr != nil {
		sendError(ctx, errs, fmt.Errorf("%s recovery gap: %w", m.name, recoveryErr))
	}
	for _, trade := range recovered {
		if err := m.emit(ctx, trades, trade); err != nil {
			return err
		}
	}
	for _, trade := range buffered {
		err := m.emit(ctx, trades, trade)
		backlog.release()
		if err != nil {
			return err
		}
	}
	for trade := range stream {
		err := m.emit(ctx, trades, trade)
		backlog.release()
		if err != nil {
			return err
		}
	}
	err = <-streamErr
	if isNormalWebSocketClose(err) {
		return nil
	}
	return err
}

func (m *marketStream) awaitSubscription(ctx context.Context, conn *websocket.Conn) ([]quanttick.TradeEvent, error) {
	pending := make(map[string]bool, len(m.Symbols))
	for _, symbol := range m.Symbols {
		pending[symbol] = true
	}
	var buffered []quanttick.TradeEvent
	for len(pending) > 0 {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return nil, err
		}
		message, err := m.parse(data, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		delete(pending, message.acknowledged)
		if len(buffered)+len(message.trades) > marketTradeBufferLimit {
			return nil, fmt.Errorf("pre-acknowledgement trade buffer exceeded %d events", marketTradeBufferLimit)
		}
		buffered = append(buffered, message.trades...)
	}
	return buffered, nil
}

func (m *marketStream) startReader(ctx context.Context, conn *websocket.Conn, backlog *tradeBacklog, pongs chan<- struct{}) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent, marketTradeBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				errs <- err
				return
			}
			message, err := m.parse(data, time.Now().UTC())
			if err != nil {
				errs <- err
				return
			}
			if message.pong {
				select {
				case pongs <- struct{}{}:
				default:
				}
			}
			for _, trade := range message.trades {
				if !backlog.reserve() {
					errs <- fmt.Errorf("websocket trade buffer exceeded %d events", marketTradeBufferLimit)
					_ = conn.CloseNow()
					return
				}
				select {
				case trades <- trade:
				case <-ctx.Done():
					backlog.release()
					errs <- ctx.Err()
					return
				}
			}
		}
	}()
	return trades, errs
}

func (m *marketStream) heartbeat(ctx context.Context, conn *websocket.Conn, pongs <-chan struct{}) {
	ticker := time.NewTicker(m.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var err error
		if m.textPing {
			select {
			case <-pongs:
			default:
			}
			err = conn.Write(pingCtx, websocket.MessageText, []byte("ping"))
			if err == nil {
				select {
				case <-pongs:
				case <-pingCtx.Done():
					err = pingCtx.Err()
				}
			}
		} else {
			err = conn.Ping(pingCtx)
		}
		cancel()
		if err != nil {
			_ = conn.CloseNow()
			return
		}
	}
}

func (m *marketStream) emit(ctx context.Context, out chan<- quanttick.TradeEvent, trade quanttick.TradeEvent) error {
	if m.seen[trade.Symbol] == nil {
		m.seen[trade.Symbol] = newSeenTradeIDs(2 * marketTradeBufferLimit)
	}
	if !m.seen[trade.Symbol].Add(trade.Symbol, trade.UID) {
		return nil
	}
	if previous, ok := m.last[trade.Symbol]; ok && trade.Timestamp.Before(previous.Timestamp) {
		return fmt.Errorf("out-of-order trade %s for %s", trade.UID, trade.Symbol)
	}
	if err := sendTrade(ctx, out, trade); err != nil {
		return err
	}
	m.last[trade.Symbol] = trade
	return nil
}
