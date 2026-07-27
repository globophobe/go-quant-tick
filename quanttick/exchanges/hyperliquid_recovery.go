package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func (h *Hyperliquid) recoverTrades(ctx context.Context) ([]quanttick.TradeEvent, error) {
	if len(h.lastUIDs) == 0 {
		return nil, nil
	}

	recovered := make([]quanttick.TradeEvent, 0)
	var recoveryErrors []error
	for _, symbol := range h.Symbols {
		cursorUID, ok := h.lastUIDs[symbol]
		if !ok {
			continue
		}
		rows, err := h.recoverSymbol(ctx, symbol, cursorUID)
		recovered = append(recovered, rows...)
		if err != nil {
			recoveryErrors = append(recoveryErrors, err)
		}
	}
	sortTradeEventsChronologically(recovered)
	return recovered, errors.Join(recoveryErrors...)
}

func (h *Hyperliquid) recoverSymbol(
	ctx context.Context,
	symbol string,
	cursorUID string,
) ([]quanttick.TradeEvent, error) {
	body, err := json.Marshal(map[string]string{
		"type": "recentTrades",
		"coin": symbol,
	})
	if err != nil {
		return nil, fmt.Errorf("build hyperliquid recovery payload: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(h.RESTURL, "/")+"/info",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build hyperliquid recovery request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	reservation, err := h.recoveryLimiter.reserve(ctx, hyperliquidRecentTradesMaxWeight)
	if err != nil {
		return nil, fmt.Errorf("wait for hyperliquid recovery rate limit: %w", err)
	}
	response, err := h.HTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch hyperliquid recovery for %s: %w", symbol, err)
	}
	var rawTrades []hyperliquidTrade
	if err := decodeRecoveryResponse(response, &rawTrades); err != nil {
		return nil, fmt.Errorf("fetch hyperliquid recovery for %s: %w", symbol, err)
	}
	if len(rawTrades) > hyperliquidRecentTradesMaxRows {
		return nil, fmt.Errorf(
			"fetch hyperliquid recovery for %s: recentTrades returned %d rows, expected at most %d",
			symbol,
			len(rawTrades),
			hyperliquidRecentTradesMaxRows,
		)
	}
	actualWeight := hyperliquidRecentTradesBaseWeight +
		hyperliquidRecentTradesExtraWeight(len(rawTrades))
	reservation.refund(hyperliquidRecentTradesMaxWeight - actualWeight)

	receivedAt := time.Now().UTC()
	newestFirst := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		trade, err := parseHyperliquidTrade(rawTrade, receivedAt)
		if err != nil {
			return nil, err
		}
		newestFirst = append(newestFirst, trade)
	}
	recovered, found := tradesAfterCursorNewestFirst(newestFirst, cursorUID)
	if !found {
		// Hyperliquid also replays missed trades in the WebSocket reconnect snapshot.
		return nil, nil
	}
	return recovered, nil
}

func hyperliquidRecentTradesExtraWeight(rows int) int {
	if rows <= 0 {
		return 0
	}
	return (rows + hyperliquidRecentTradesRowsPerWeight - 1) /
		hyperliquidRecentTradesRowsPerWeight
}
