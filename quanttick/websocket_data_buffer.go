package quanttick

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

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
