package quanttick

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

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
	// These venues use opaque trade IDs rather than sortable sequences. For exact
	// timestamp ties, retain stable feed order instead of inventing UUID ordering.
	if exchange == "bitmex" ||
		exchange == "bybit" ||
		exchange == "bybit-linear" ||
		exchange == "bybit-inverse" ||
		exchange == "hyperliquid" {
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
