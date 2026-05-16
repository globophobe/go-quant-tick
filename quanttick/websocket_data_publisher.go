package quanttick

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const websocketDataRetention = time.Hour

type WebSocketDataStore struct {
	db *sql.DB
}

type WebSocketDataBufferConfig struct {
	DefaultSignificantTradeFilter Decimal
	SignificantThresholds         map[string]Decimal
}

type WebSocketDataBuffer struct {
	mu      sync.Mutex
	buckets map[websocketDataBucketKey]WebSocketDataBucket
	store   *WebSocketDataStore
	config  WebSocketDataBufferConfig
}

type WebSocketDataBucket struct {
	Exchange               string
	APISymbol              string
	SignificantTradeFilter int64
	Timestamp              time.Time
	RawTrades              []TradeEvent
	AggregatedTrades       []TradeEvent
	FilteredTrades         []SignificantTrade
}

type websocketDataBucketKey struct {
	exchange               string
	symbol                 string
	significantTradeFilter int64
	timestamp              time.Time
}

type websocketDataTradePublisher struct {
	stream Stream
	buffer *WebSocketDataBuffer
}

type websocketDataSignificantTradePublisher struct {
	buffer *WebSocketDataBuffer
}

func NewWebSocketDataStore(db *sql.DB) *WebSocketDataStore {
	return &WebSocketDataStore{db: db}
}

func NewWebSocketDataBuffer(store *WebSocketDataStore, config WebSocketDataBufferConfig) *WebSocketDataBuffer {
	return &WebSocketDataBuffer{
		buckets: make(map[websocketDataBucketKey]WebSocketDataBucket),
		store:   store,
		config: WebSocketDataBufferConfig{
			DefaultSignificantTradeFilter: config.DefaultSignificantTradeFilter,
			SignificantThresholds:         cloneThresholds(config.SignificantThresholds),
		},
	}
}

func (b *WebSocketDataBuffer) RawPublisher() Publisher[TradeEvent] {
	return websocketDataTradePublisher{stream: RawTrades, buffer: b}
}

func (b *WebSocketDataBuffer) AggregatedPublisher() Publisher[TradeEvent] {
	return websocketDataTradePublisher{stream: AggregatedTrades, buffer: b}
}

func (b *WebSocketDataBuffer) SignificantPublisher() Publisher[SignificantTrade] {
	return websocketDataSignificantTradePublisher{buffer: b}
}

func (p websocketDataTradePublisher) Publish(ctx context.Context, trade TradeEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return p.buffer.addTrade(p.stream, trade)
}

func (p websocketDataSignificantTradePublisher) Publish(ctx context.Context, trade SignificantTrade) error {
	return p.PublishBatch(ctx, []SignificantTrade{trade})
}

func (p websocketDataSignificantTradePublisher) PublishBatch(ctx context.Context, trades []SignificantTrade) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for _, trade := range trades {
		if err := p.buffer.addSignificantTrade(trade); err != nil {
			return err
		}
	}
	return nil
}

func (b *WebSocketDataBuffer) addTrade(stream Stream, trade TradeEvent) error {
	filter, err := b.significantTradeFilter(trade.Exchange, trade.Symbol)
	if err != nil {
		return err
	}
	key := newWebSocketDataBucketKey(trade.Exchange, trade.Symbol, filter, trade.Timestamp)

	b.mu.Lock()
	defer b.mu.Unlock()
	bucket := b.bucket(key)
	switch stream {
	case RawTrades:
		bucket.RawTrades = append(bucket.RawTrades, trade)
	case AggregatedTrades:
		bucket.AggregatedTrades = append(bucket.AggregatedTrades, trade)
	default:
		return fmt.Errorf("unsupported websocket data stream: %s", stream)
	}
	b.buckets[key] = bucket
	return nil
}

func (b *WebSocketDataBuffer) addSignificantTrade(trade SignificantTrade) error {
	filter, err := decimalToInt64(trade.SignificantTradeFilter)
	if err != nil {
		return err
	}
	key := newWebSocketDataBucketKey(trade.Exchange, trade.Symbol, filter, trade.Timestamp)

	b.mu.Lock()
	defer b.mu.Unlock()
	bucket := b.bucket(key)
	bucket.FilteredTrades = append(bucket.FilteredTrades, trade)
	b.buckets[key] = bucket
	return nil
}

func (b *WebSocketDataBuffer) bucket(key websocketDataBucketKey) WebSocketDataBucket {
	bucket, ok := b.buckets[key]
	if ok {
		return bucket
	}
	return WebSocketDataBucket{
		Exchange:               key.exchange,
		APISymbol:              key.symbol,
		SignificantTradeFilter: key.significantTradeFilter,
		Timestamp:              key.timestamp,
	}
}

func (b *WebSocketDataBuffer) FlushBefore(ctx context.Context, exchange string, symbol string, timestamp time.Time) (int, error) {
	boundary := timestamp.UTC().Truncate(time.Minute)
	return b.flush(ctx, func(key websocketDataBucketKey) bool {
		return key.exchange == exchange && key.symbol == symbol && key.timestamp.Before(boundary)
	})
}

func (b *WebSocketDataBuffer) Flush(ctx context.Context) (int, error) {
	return b.flush(ctx, func(websocketDataBucketKey) bool { return true })
}

func (b *WebSocketDataBuffer) flush(ctx context.Context, include func(websocketDataBucketKey) bool) (int, error) {
	keys, buckets := b.snapshot(include)
	if len(buckets) == 0 {
		return 0, nil
	}
	if err := b.store.UpsertBuckets(ctx, buckets); err != nil {
		return 0, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for _, key := range keys {
		delete(b.buckets, key)
	}
	return len(buckets), nil
}

func (b *WebSocketDataBuffer) snapshot(include func(websocketDataBucketKey) bool) ([]websocketDataBucketKey, []WebSocketDataBucket) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]websocketDataBucketKey, 0, len(b.buckets))
	for key := range b.buckets {
		if include(key) {
			keys = append(keys, key)
		}
	}
	sortWebSocketDataBucketKeys(keys)
	buckets := make([]WebSocketDataBucket, 0, len(keys))
	for _, key := range keys {
		buckets = append(buckets, b.buckets[key])
	}
	return keys, buckets
}

func (b *WebSocketDataBuffer) significantTradeFilter(exchange string, symbol string) (int64, error) {
	threshold := b.config.DefaultSignificantTradeFilter
	if override, ok := b.config.SignificantThresholds[ExchangeSymbolKey(exchange, symbol)]; ok {
		threshold = override
	}
	return decimalToInt64(threshold)
}

func newWebSocketDataBucketKey(exchange string, symbol string, significantTradeFilter int64, timestamp time.Time) websocketDataBucketKey {
	return websocketDataBucketKey{
		exchange:               exchange,
		symbol:                 symbol,
		significantTradeFilter: significantTradeFilter,
		timestamp:              timestamp.UTC().Truncate(time.Minute),
	}
}

func sortWebSocketDataBucketKeys(keys []websocketDataBucketKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].exchange != keys[j].exchange {
			return keys[i].exchange < keys[j].exchange
		}
		if keys[i].symbol != keys[j].symbol {
			return keys[i].symbol < keys[j].symbol
		}
		if keys[i].significantTradeFilter != keys[j].significantTradeFilter {
			return keys[i].significantTradeFilter < keys[j].significantTradeFilter
		}
		return keys[i].timestamp.Before(keys[j].timestamp)
	})
}

func decimalToInt64(value Decimal) (int64, error) {
	intPart := value.IntPart()
	if !value.Equal(decimal.NewFromInt(intPart)) {
		return 0, fmt.Errorf("significant trade filter must be an integer: %s", value)
	}
	if intPart < 0 {
		return 0, fmt.Errorf("significant trade filter must be non-negative: %s", value)
	}
	return intPart, nil
}

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
		bucket, err = deriveFilteredTradesFromAggregated(bucket)
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
	var rawTradesPayload, aggregatedTradesPayload, filteredTradesPayload string
	err := tx.QueryRowContext(
		ctx,
		selectWebSocketDataSQL,
		bucket.Exchange,
		bucket.APISymbol,
		bucket.SignificantTradeFilter,
		bucket.Timestamp,
	).Scan(&rawTradesPayload, &aggregatedTradesPayload, &filteredTradesPayload)
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
	var filteredTrades []SignificantTrade
	if err := unmarshalJSONList(filteredTradesPayload, &filteredTrades); err != nil {
		return WebSocketDataBucket{}, fmt.Errorf("parse existing filtered trades: %w", err)
	}

	bucket.RawTrades = mergeTradesByUID(rawTrades, bucket.RawTrades)
	bucket.AggregatedTrades = mergeTradesByUID(aggregatedTrades, bucket.AggregatedTrades)
	bucket.FilteredTrades = mergeSignificantTradesByUID(filteredTrades, bucket.FilteredTrades)
	return bucket, nil
}

func deriveFilteredTradesFromAggregated(bucket WebSocketDataBucket) (WebSocketDataBucket, error) {
	if len(bucket.AggregatedTrades) == 0 {
		return bucket, nil
	}

	threshold := decimal.NewFromInt(bucket.SignificantTradeFilter)
	aggregator := NewSignificantTradeAggregator(threshold, time.Minute)
	filteredTrades := make([]SignificantTrade, 0, len(bucket.AggregatedTrades))
	key := ExchangeSymbolKey(bucket.Exchange, bucket.APISymbol)
	for _, trade := range bucket.AggregatedTrades {
		trades, err := aggregator.Add(trade)
		if err != nil {
			return WebSocketDataBucket{}, fmt.Errorf("derive filtered trades for %s %s %s: %w", bucket.Exchange, bucket.APISymbol, bucket.Timestamp.Format(time.RFC3339), err)
		}
		filteredTrades = append(filteredTrades, trades...)
	}
	trades, err := aggregator.Flush(key)
	if err != nil {
		return WebSocketDataBucket{}, fmt.Errorf("flush derived filtered trades for %s %s %s: %w", bucket.Exchange, bucket.APISymbol, bucket.Timestamp.Format(time.RFC3339), err)
	}
	bucket.FilteredTrades = append(filteredTrades, trades...)
	return bucket, nil
}

func mergeTradesByUID(existing []TradeEvent, incoming []TradeEvent) []TradeEvent {
	return mergeByUID(existing, incoming, func(trade TradeEvent) string { return trade.UID })
}

func mergeSignificantTradesByUID(existing []SignificantTrade, incoming []SignificantTrade) []SignificantTrade {
	return mergeByUID(existing, incoming, func(trade SignificantTrade) string { return trade.UID })
}

func mergeByUID[T any](existing []T, incoming []T, getUID func(T) string) []T {
	if len(existing) == 0 {
		return incoming
	}
	if len(incoming) == 0 {
		return existing
	}
	merged := append([]T(nil), existing...)
	seen := make(map[string]struct{}, len(existing))
	for _, item := range existing {
		uid := getUID(item)
		if uid != "" {
			seen[uid] = struct{}{}
		}
	}
	for _, item := range incoming {
		uid := getUID(item)
		if uid == "" {
			merged = append(merged, item)
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		merged = append(merged, item)
	}
	return merged
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
SELECT raw_trades, aggregated_trades, filtered_trades
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
