package quanttick

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	flushMu sync.Mutex
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
	rawTradeUIDs           map[string]struct{}
	aggregatedTradeUIDs    map[string]struct{}
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

func (p websocketDataTradePublisher) Publish(ctx context.Context, trade TradeEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return p.buffer.addTrade(p.stream, trade)
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
		bucket.RawTrades, bucket.rawTradeUIDs = insertWebSocketDataTrade(
			bucket.Exchange,
			stream,
			bucket.RawTrades,
			bucket.rawTradeUIDs,
			trade,
		)
	case AggregatedTrades:
		bucket.AggregatedTrades, bucket.aggregatedTradeUIDs = insertWebSocketDataTrade(
			bucket.Exchange,
			stream,
			bucket.AggregatedTrades,
			bucket.aggregatedTradeUIDs,
			trade,
		)
	default:
		return fmt.Errorf("unsupported websocket data stream: %s", stream)
	}
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
	// Only one detached batch may be writing at a time. Appends remain available
	// while the database transaction runs and land in a fresh bucket generation.
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	buckets := b.detach(include)
	if len(buckets) == 0 {
		return 0, nil
	}
	if err := b.store.UpsertBuckets(ctx, buckets); err != nil {
		b.restore(buckets)
		return 0, err
	}
	return len(buckets), nil
}

func (b *WebSocketDataBuffer) detach(include func(websocketDataBucketKey) bool) []WebSocketDataBucket {
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
		delete(b.buckets, key)
	}
	return buckets
}

func (b *WebSocketDataBuffer) restore(buckets []WebSocketDataBucket) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, detached := range buckets {
		key := newWebSocketDataBucketKey(
			detached.Exchange,
			detached.APISymbol,
			detached.SignificantTradeFilter,
			detached.Timestamp,
		)
		current := b.bucket(key)
		current.RawTrades = mergeWebSocketDataTrades(
			detached.Exchange,
			RawTrades,
			detached.RawTrades,
			current.RawTrades,
		)
		current.AggregatedTrades = mergeWebSocketDataTrades(
			detached.Exchange,
			AggregatedTrades,
			detached.AggregatedTrades,
			current.AggregatedTrades,
		)
		current.FilteredTrades = nil
		current.rawTradeUIDs = tradeUIDSet(current.RawTrades)
		current.aggregatedTradeUIDs = tradeUIDSet(current.AggregatedTrades)
		b.buckets[key] = current
	}
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

func deriveWebSocketDataBucket(bucket WebSocketDataBucket) (WebSocketDataBucket, error) {
	bucket.RawTrades = canonicalWebSocketDataTrades(bucket.Exchange, RawTrades, bucket.RawTrades)
	bucket.AggregatedTrades = canonicalWebSocketDataTrades(bucket.Exchange, AggregatedTrades, bucket.AggregatedTrades)
	if shouldDeriveBitfinexTradesFromRaw(bucket) {
		aggregatedTrades, err := aggregateTradeEvents(bucket.RawTrades)
		if err != nil {
			return WebSocketDataBucket{}, fmt.Errorf("derive bitfinex aggregated trades for %s %s: %w", bucket.APISymbol, bucket.Timestamp.Format(time.RFC3339), err)
		}
		bucket.AggregatedTrades = aggregatedTrades
	} else {
		aggregatedTrades, err := aggregateTradeEvents(bucket.AggregatedTrades)
		if err != nil {
			return WebSocketDataBucket{}, fmt.Errorf("coalesce aggregated trades for %s %s %s: %w", bucket.Exchange, bucket.APISymbol, bucket.Timestamp.Format(time.RFC3339), err)
		}
		bucket.AggregatedTrades = aggregatedTrades
	}
	return deriveFilteredTradesFromAggregated(bucket)
}

func shouldDeriveBitfinexTradesFromRaw(bucket WebSocketDataBucket) bool {
	return bucket.Exchange == "bitfinex" && len(bucket.RawTrades) > 0
}

func bitfinexUIDLess(left string, right string) bool {
	leftID, leftErr := strconv.ParseInt(left, 10, 64)
	rightID, rightErr := strconv.ParseInt(right, 10, 64)
	if leftErr != nil || rightErr != nil {
		return left < right
	}
	return leftID < rightID
}

func aggregateTradeEvents(trades []TradeEvent) ([]TradeEvent, error) {
	if len(trades) == 0 {
		return nil, nil
	}

	aggregatedTrades := make([]TradeEvent, 0, len(trades))
	start := 0
	for index := 1; index < len(trades); index++ {
		if sameSample(trades[start], trades[index]) {
			continue
		}
		aggregated, err := aggregateTrades(trades[start:index])
		if err != nil {
			return nil, err
		}
		aggregatedTrades = append(aggregatedTrades, aggregated)
		start = index
	}

	aggregated, err := aggregateTrades(trades[start:])
	if err != nil {
		return nil, err
	}
	aggregatedTrades = append(aggregatedTrades, aggregated)
	return aggregatedTrades, nil
}

func deriveFilteredTradesFromAggregated(bucket WebSocketDataBucket) (WebSocketDataBucket, error) {
	if len(bucket.AggregatedTrades) == 0 {
		bucket.FilteredTrades = nil
		return bucket, nil
	}

	threshold := decimal.NewFromInt(bucket.SignificantTradeFilter)
	filteredTrades, err := volumeFilterAggregatedTrades(bucket.AggregatedTrades, threshold)
	if err != nil {
		return WebSocketDataBucket{}, fmt.Errorf("derive filtered trades for %s %s %s: %w", bucket.Exchange, bucket.APISymbol, bucket.Timestamp.Format(time.RFC3339), err)
	}
	bucket.FilteredTrades = filteredTrades
	return bucket, nil
}

func volumeFilterAggregatedTrades(trades []TradeEvent, threshold Decimal) ([]SignificantTrade, error) {
	filteredTrades := make([]SignificantTrade, 0, len(trades))
	start := 0
	for index, trade := range trades {
		isMinVolume := threshold.IsZero() || trade.Volume.GreaterThanOrEqual(threshold)
		if !isMinVolume {
			continue
		}
		filteredTrade, err := aggregateFilteredTradeSegment(trades[start:index+1], true, threshold)
		if err != nil {
			return nil, err
		}
		filteredTrades = append(filteredTrades, filteredTrade)
		start = index + 1
	}
	if start < len(trades) {
		filteredTrade, err := aggregateFilteredTradeSegment(trades[start:], false, threshold)
		if err != nil {
			return nil, err
		}
		filteredTrades = append(filteredTrades, filteredTrade)
	}
	return filteredTrades, nil
}

func aggregateFilteredTradeSegment(trades []TradeEvent, isMinVolume bool, threshold Decimal) (SignificantTrade, error) {
	if len(trades) == 0 {
		return SignificantTrade{}, fmt.Errorf("cannot filter empty trade set")
	}

	high := trades[0].Price
	low := trades[0].Price
	totalBuyVolume := Decimal{}
	totalVolume := Decimal{}
	totalBuyNotional := Decimal{}
	totalNotional := Decimal{}
	totalBuyTicks := 0
	totalTicks := 0
	allSequential := true
	for _, trade := range trades {
		if trade.Price.GreaterThan(high) {
			high = trade.Price
		}
		if trade.Price.LessThan(low) {
			low = trade.Price
		}
		if trade.TickRule == 1 {
			totalBuyVolume = totalBuyVolume.Add(trade.Volume)
			totalBuyNotional = totalBuyNotional.Add(trade.Notional)
			totalBuyTicks += trade.Ticks
		}
		totalVolume = totalVolume.Add(trade.Volume)
		totalNotional = totalNotional.Add(trade.Notional)
		totalTicks += trade.Ticks
		allSequential = allSequential && trade.IsSequential
	}

	last := trades[len(trades)-1]
	filteredTrade := SignificantTrade{
		Exchange:               last.Exchange,
		UID:                    last.UID,
		Symbol:                 last.Symbol,
		Timestamp:              last.Timestamp,
		Nanoseconds:            last.Nanoseconds,
		Price:                  last.Price,
		IsSequential:           allSequential,
		High:                   high,
		Low:                    low,
		TotalBuyVolume:         totalBuyVolume,
		TotalVolume:            totalVolume,
		TotalBuyNotional:       totalBuyNotional,
		TotalNotional:          totalNotional,
		TotalBuyTicks:          totalBuyTicks,
		TotalTicks:             totalTicks,
		SignificantTradeFilter: threshold,
	}
	if isMinVolume {
		filteredTrade.Volume = decimalPtr(last.Volume)
		filteredTrade.Notional = decimalPtr(last.Notional)
		filteredTrade.TickRule = intPtr(last.TickRule)
		filteredTrade.Ticks = intPtr(last.Ticks)
	}
	return filteredTrade, nil
}

func insertWebSocketDataTrade(
	exchange string,
	stream Stream,
	trades []TradeEvent,
	seen map[string]struct{},
	trade TradeEvent,
) ([]TradeEvent, map[string]struct{}) {
	if seen == nil {
		seen = tradeUIDSet(trades)
	}
	if trade.UID != "" {
		if _, ok := seen[trade.UID]; ok {
			if stream != AggregatedTrades {
				return trades, seen
			}
			duplicateIndex := -1
			for index := range trades {
				if trades[index].UID == trade.UID {
					duplicateIndex = index
					break
				}
			}
			if duplicateIndex < 0 || trade.Ticks <= trades[duplicateIndex].Ticks {
				return trades, seen
			}
			trades[duplicateIndex] = trade
			sort.SliceStable(trades, func(i, j int) bool {
				return webSocketDataTradeLess(exchange, stream, trades[i], trades[j])
			})
			return trades, seen
		} else {
			seen[trade.UID] = struct{}{}
		}
	}

	index := sort.Search(len(trades), func(index int) bool {
		return webSocketDataTradeLess(exchange, stream, trade, trades[index])
	})
	trades = append(trades, TradeEvent{})
	copy(trades[index+1:], trades[index:])
	trades[index] = trade
	return trades, seen
}

func mergeWebSocketDataTrades(exchange string, stream Stream, existing []TradeEvent, incoming []TradeEvent) []TradeEvent {
	merged := make([]TradeEvent, 0, len(existing)+len(incoming))
	merged = append(merged, existing...)
	merged = append(merged, incoming...)
	return canonicalWebSocketDataTrades(exchange, stream, merged)
}

func canonicalWebSocketDataTrades(exchange string, stream Stream, trades []TradeEvent) []TradeEvent {
	if len(trades) == 0 {
		return nil
	}

	canonical := make([]TradeEvent, 0, len(trades))
	seen := make(map[string]int, len(trades))
	for _, trade := range trades {
		if trade.UID != "" {
			if index, ok := seen[trade.UID]; ok {
				if stream == AggregatedTrades && trade.Ticks > canonical[index].Ticks {
					canonical[index] = trade
				}
				continue
			}
			seen[trade.UID] = len(canonical)
		}
		canonical = append(canonical, trade)
	}
	sort.SliceStable(canonical, func(i, j int) bool {
		return webSocketDataTradeLess(exchange, stream, canonical[i], canonical[j])
	})
	return canonical
}

func webSocketDataTradeLess(exchange string, stream Stream, left TradeEvent, right TradeEvent) bool {
	// Bitfinex spot REST display order is not fully inferable from websocket
	// timestamps, so retain the established deterministic raw UID ordering.
	if exchange == "bitfinex" && stream == RawTrades {
		return bitfinexUIDLess(left.UID, right.UID)
	}
	if !left.Timestamp.Equal(right.Timestamp) {
		return left.Timestamp.Before(right.Timestamp)
	}
	if left.Nanoseconds != right.Nanoseconds {
		return left.Nanoseconds < right.Nanoseconds
	}
	// BitMEX match IDs and Hyperliquid trade IDs are opaque, not sequences.
	// For equal exchange timestamps, retain stable feed order rather than inventing one.
	if exchange == "bitmex" || exchange == "hyperliquid" {
		return false
	}
	return tradeUIDLess(left.UID, right.UID)
}

func tradeUIDLess(left string, right string) bool {
	leftID, leftErr := strconv.ParseInt(left, 10, 64)
	rightID, rightErr := strconv.ParseInt(right, 10, 64)
	if leftErr == nil && rightErr == nil {
		return leftID < rightID
	}

	leftPrefix, leftSuffix, leftOK := splitNumericUIDSuffix(left)
	rightPrefix, rightSuffix, rightOK := splitNumericUIDSuffix(right)
	if leftOK && rightOK && leftPrefix == rightPrefix {
		return leftSuffix < rightSuffix
	}
	return left < right
}

func splitNumericUIDSuffix(uid string) (string, int64, bool) {
	separator := strings.LastIndexByte(uid, ':')
	if separator < 0 {
		return "", 0, false
	}
	suffix, err := strconv.ParseInt(uid[separator+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return uid[:separator], suffix, true
}

func tradeUIDSet(trades []TradeEvent) map[string]struct{} {
	seen := make(map[string]struct{}, len(trades))
	for _, trade := range trades {
		if trade.UID != "" {
			seen[trade.UID] = struct{}{}
		}
	}
	return seen
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
