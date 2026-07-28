package quanttick

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const websocketDataRetention = time.Hour

func (s *WebSocketDataStore) UpsertBuckets(ctx context.Context, buckets []WebSocketDataBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin websocket data transaction: %w", err)
	}
	defer tx.Rollback()

	for _, bucket := range buckets {
		bucket, err = mergeExistingWebSocketDataBucket(ctx, tx, bucket)
		if err != nil {
			return err
		}
		bucket, err = deriveWebSocketDataBucket(bucket)
		if err != nil {
			return err
		}
		rawTrades, err := marshalJSONList(bucket.RawTrades)
		if err != nil {
			return fmt.Errorf("marshal raw trades: %w", err)
		}
		aggregatedTrades, err := marshalJSONList(bucket.AggregatedTrades)
		if err != nil {
			return fmt.Errorf("marshal aggregated trades: %w", err)
		}
		filteredTrades, err := marshalJSONList(bucket.FilteredTrades)
		if err != nil {
			return fmt.Errorf("marshal filtered trades: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			upsertWebSocketDataSQL,
			bucket.Exchange,
			bucket.APISymbol,
			bucket.SignificantTradeFilter,
			bucket.Timestamp,
			string(rawTrades),
			string(aggregatedTrades),
			string(filteredTrades),
		); err != nil {
			return fmt.Errorf("upsert websocket data bucket %s %s %s: %w", bucket.Exchange, bucket.APISymbol, bucket.Timestamp.Format(time.RFC3339), err)
		}
	}
	cutoff := time.Now().UTC().Add(-websocketDataRetention).Truncate(time.Minute)
	if err := deleteOldWebSocketData(ctx, tx, cutoff); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit websocket data transaction: %w", err)
	}
	return nil
}

func mergeExistingWebSocketDataBucket(ctx context.Context, tx *sql.Tx, bucket WebSocketDataBucket) (WebSocketDataBucket, error) {
	var rawTradesPayload, aggregatedTradesPayload string
	err := tx.QueryRowContext(
		ctx,
		selectWebSocketDataSQL,
		bucket.Exchange,
		bucket.APISymbol,
		bucket.SignificantTradeFilter,
		bucket.Timestamp,
	).Scan(&rawTradesPayload, &aggregatedTradesPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return bucket, nil
	}
	if err != nil {
		return WebSocketDataBucket{}, fmt.Errorf("read existing websocket data bucket %s %s %s: %w", bucket.Exchange, bucket.APISymbol, bucket.Timestamp.Format(time.RFC3339), err)
	}

	var rawTrades []TradeEvent
	if err := unmarshalJSONList(rawTradesPayload, &rawTrades); err != nil {
		return WebSocketDataBucket{}, fmt.Errorf("parse existing raw trades: %w", err)
	}
	var aggregatedTrades []TradeEvent
	if err := unmarshalJSONList(aggregatedTradesPayload, &aggregatedTrades); err != nil {
		return WebSocketDataBucket{}, fmt.Errorf("parse existing aggregated trades: %w", err)
	}

	bucket.RawTrades = mergeWebSocketDataTrades(bucket.Exchange, RawTrades, rawTrades, bucket.RawTrades)
	bucket.AggregatedTrades = mergeWebSocketDataTrades(bucket.Exchange, AggregatedTrades, aggregatedTrades, bucket.AggregatedTrades)
	bucket.FilteredTrades = nil
	return bucket, nil
}

func deleteOldWebSocketData(ctx context.Context, tx *sql.Tx, cutoff time.Time) error {
	if _, err := tx.ExecContext(ctx, deleteOldWebSocketDataSQL, cutoff); err != nil {
		return fmt.Errorf("delete old websocket data rows: %w", err)
	}
	return nil
}

func marshalJSONList[T any](values []T) ([]byte, error) {
	if values == nil {
		values = []T{}
	}
	return json.Marshal(values)
}

func unmarshalJSONList[T any](payload string, values *[]T) error {
	if payload == "" {
		*values = nil
		return nil
	}
	return json.Unmarshal([]byte(payload), values)
}

const selectWebSocketDataSQL = `
SELECT raw_trades, aggregated_trades
FROM quant_tick_websocket_data
WHERE exchange = $1
	AND api_symbol = $2
	AND significant_trade_filter = $3
	AND timestamp = $4`

const upsertWebSocketDataSQL = `
INSERT INTO quant_tick_websocket_data (
	exchange,
	api_symbol,
	significant_trade_filter,
	timestamp,
	raw_trades,
	aggregated_trades,
	filtered_trades
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (exchange, api_symbol, significant_trade_filter, timestamp)
DO UPDATE SET
	raw_trades = excluded.raw_trades,
	aggregated_trades = excluded.aggregated_trades,
	filtered_trades = excluded.filtered_trades`

const deleteOldWebSocketDataSQL = `
DELETE FROM quant_tick_websocket_data
WHERE timestamp < $1`
