package exchanges

import (
	"context"
	"errors"
	"fmt"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

// Recovery must reach the replacement stream before buffered trades are published.
// A quiet symbol gets another drain interval to allow in-flight messages to arrive.
func recoverWithWebSocketOverlap(
	ctx context.Context,
	name string,
	symbols []string,
	cursors map[string]string,
	recoverSymbol func(context.Context, string, string) ([]quanttick.TradeEvent, error),
	retryDelay time.Duration,
	buffered []quanttick.TradeEvent,
	stream <-chan quanttick.TradeEvent,
	streamErr <-chan error,
) ([]quanttick.TradeEvent, []quanttick.TradeEvent, error) {
	if len(cursors) == 0 {
		return nil, buffered, nil
	}

	handoff := newRecoveryHandoff(buffered)
	pending := make(map[string]struct{})
	for _, symbol := range symbols {
		if _, ok := cursors[symbol]; ok {
			pending[symbol] = struct{}{}
		}
	}
	verified := make(map[string][]quanttick.TradeEvent)
	quietCandidates := make(map[string][]quanttick.TradeEvent)
	var recoveryErrors []error
	recoveredTrades := func() []quanttick.TradeEvent {
		recovered := make([]quanttick.TradeEvent, 0)
		for _, symbol := range symbols {
			recovered = append(recovered, verified[symbol]...)
		}
		sortTradeEventsChronologically(recovered)
		return recovered
	}
	verifyPass := func(pass map[string][]quanttick.TradeEvent) {
		for symbol, rows := range pass {
			if handoff.overlaps(symbol, rows) {
				verified[symbol] = rows
				delete(pending, symbol)
				delete(quietCandidates, symbol)
				continue
			}
			if !handoff.hasTrades(symbol) {
				quietCandidates[symbol] = rows
			}
		}
	}
	drainAvailable := func() error {
		for {
			select {
			case trade, ok := <-stream:
				if !ok {
					readErr := <-streamErr
					return fmt.Errorf(
						"%s websocket closed before REST handoff overlap: %w",
						name,
						readErr,
					)
				}
				handoff.add(trade)
			default:
				return nil
			}
		}
	}

	for len(pending) > 0 {
		for symbol, rows := range quietCandidates {
			if _, ok := pending[symbol]; !ok {
				continue
			}
			if !handoff.hasTrades(symbol) || handoff.overlaps(symbol, rows) {
				verified[symbol] = rows
				delete(pending, symbol)
			}
			delete(quietCandidates, symbol)
		}
		if len(pending) == 0 {
			break
		}

		pass := make(map[string][]quanttick.TradeEvent, len(pending))
		terminalError := false
		for _, symbol := range symbols {
			if _, ok := pending[symbol]; !ok {
				continue
			}
			rows, err := recoverSymbol(ctx, symbol, cursors[symbol])
			if err != nil {
				var cursorGap *recoveryCursorGapError
				if !errors.As(err, &cursorGap) {
					recoveryErrors = append(recoveryErrors, err)
					terminalError = true
					break
				}
				recoveryErrors = append(recoveryErrors, err)
			}
			pass[symbol] = rows
		}

		if err := drainAvailable(); err != nil {
			recoveryErrors = append(recoveryErrors, err)
			terminalError = true
		}

		verifyPass(pass)
		if terminalError {
			return recoveredTrades(), handoff.trades, errors.Join(recoveryErrors...)
		}
		if len(pending) == 0 {
			break
		}
		retry := time.NewTimer(retryDelay)
		for {
			select {
			case trade, ok := <-stream:
				if !ok {
					stopTimer(retry)
					readErr := <-streamErr
					recoveryErrors = append(recoveryErrors, fmt.Errorf(
						"%s websocket closed before REST handoff overlap: %w",
						name,
						readErr,
					))
					verifyPass(pass)
					return recoveredTrades(), handoff.trades, errors.Join(recoveryErrors...)
				}
				handoff.add(trade)
			case <-retry.C:
				if err := drainAvailable(); err != nil {
					recoveryErrors = append(recoveryErrors, err)
					verifyPass(pass)
					return recoveredTrades(), handoff.trades, errors.Join(recoveryErrors...)
				}
				verifyPass(pass)
				goto retryRecovery
			case <-ctx.Done():
				stopTimer(retry)
				recoveryErrors = append(recoveryErrors, fmt.Errorf(
					"%s REST-to-websocket handoff did not overlap: %w",
					name,
					ctx.Err(),
				))
				if err := drainAvailable(); err != nil {
					recoveryErrors = append(recoveryErrors, err)
				}
				verifyPass(pass)
				return recoveredTrades(), handoff.trades, errors.Join(recoveryErrors...)
			}
		}

	retryRecovery:
	}

	return recoveredTrades(), handoff.trades, errors.Join(recoveryErrors...)
}

type recoveryCursorGapError struct {
	exchange  string
	symbol    string
	cursorUID string
	rows      int
}

func (e *recoveryCursorGapError) Error() string {
	return fmt.Sprintf(
		"%s recovery for %s does not contain cursor %s in %d recent trades",
		e.exchange,
		e.symbol,
		e.cursorUID,
		e.rows,
	)
}

type recoveryHandoff struct {
	trades []quanttick.TradeEvent
	uids   map[string]map[string]struct{}
}

func newRecoveryHandoff(trades []quanttick.TradeEvent) *recoveryHandoff {
	handoff := &recoveryHandoff{
		trades: append([]quanttick.TradeEvent(nil), trades...),
		uids:   make(map[string]map[string]struct{}),
	}
	for _, trade := range trades {
		handoff.addUID(trade)
	}
	return handoff
}

func (h *recoveryHandoff) add(trade quanttick.TradeEvent) {
	h.trades = append(h.trades, trade)
	h.addUID(trade)
}

func (h *recoveryHandoff) addUID(trade quanttick.TradeEvent) {
	if h.uids[trade.Symbol] == nil {
		h.uids[trade.Symbol] = make(map[string]struct{})
	}
	h.uids[trade.Symbol][trade.UID] = struct{}{}
}

func (h *recoveryHandoff) hasTrades(symbol string) bool {
	return len(h.uids[symbol]) > 0
}

func (h *recoveryHandoff) overlaps(symbol string, recovered []quanttick.TradeEvent) bool {
	websocketUIDs := h.uids[symbol]
	if len(websocketUIDs) == 0 {
		return false
	}
	for _, trade := range recovered {
		if trade.Symbol != symbol {
			continue
		}
		if _, ok := websocketUIDs[trade.UID]; ok {
			return true
		}
	}
	return false
}
