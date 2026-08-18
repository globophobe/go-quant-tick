package quanttick

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestWebSocketDataBufferFlushesPreviousMinuteOnLaterTradeEvent(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{DefaultSignificantTradeFilter: MustDecimal("1000")},
	)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	raw := testTrade(
		"raw-1",
		timestamp.Add(10*time.Second),
		withExchange("coinbase"),
		withSymbol("BTC-USD"),
	)
	aggregated := testTrade(
		"aggregated-1",
		timestamp.Add(20*time.Second),
		withExchange("coinbase"),
		withSymbol("BTC-USD"),
	)

	if err := buffer.RawPublisher().Publish(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if err := buffer.AggregatedPublisher().Publish(context.Background(), aggregated); err != nil {
		t.Fatal(err)
	}

	count, err := buffer.FlushBefore(
		context.Background(),
		"coinbase",
		"BTC-USD",
		timestamp.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("flushed buckets = %d, want 1", count)
	}

	row := getWebSocketDataTestRow(t, db)
	if row.significantTradeFilter != 1000 {
		t.Fatalf("significant trade filter = %d, want 1000", row.significantTradeFilter)
	}
	if len(row.rawTrades) != 1 || row.rawTrades[0].UID != "raw-1" {
		t.Fatalf("raw trades = %#v", row.rawTrades)
	}
	if len(row.aggregatedTrades) != 1 || row.aggregatedTrades[0].UID != "aggregated-1" {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if len(row.filteredTrades) != 1 || row.filteredTrades[0].UID != "aggregated-1" {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
}

func TestWebSocketDataBufferKeepsCurrentEventMinuteInMemory(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{DefaultSignificantTradeFilter: MustDecimal("1000")},
	)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	raw := testTrade(
		"raw-1",
		timestamp.Add(10*time.Second),
		withExchange("coinbase"),
		withSymbol("BTC-USD"),
	)

	if err := buffer.RawPublisher().Publish(context.Background(), raw); err != nil {
		t.Fatal(err)
	}

	count, err := buffer.FlushBefore(
		context.Background(),
		"coinbase",
		"BTC-USD",
		timestamp.Add(30*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("flushed buckets = %d, want 0", count)
	}
	if countWebSocketDataTestRows(t, db) != 0 {
		t.Fatal("open minute should not be written")
	}

	count, err = buffer.FlushBefore(
		context.Background(),
		"coinbase",
		"BTC-USD",
		timestamp.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("flushed buckets = %d, want 1", count)
	}
	var aggregatedTrades, filteredTrades string
	err = db.QueryRow("SELECT aggregated_trades, filtered_trades FROM quant_tick_websocket_data").Scan(
		&aggregatedTrades,
		&filteredTrades,
	)
	if err != nil {
		t.Fatal(err)
	}
	if aggregatedTrades != "[]" || filteredTrades != "[]" {
		t.Fatalf("empty streams should be JSON arrays, got aggregated=%s filtered=%s", aggregatedTrades, filteredTrades)
	}
}

func TestWebSocketDataBufferOrdersAndDeduplicatesTradesInMemory(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{DefaultSignificantTradeFilter: MustDecimal("1000")},
	)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	trades := []TradeEvent{
		testTrade("20", timestamp.Add(20*time.Second), withExchange("coinbase"), withSymbol("BTC-USD")),
		testTrade("10", timestamp.Add(10*time.Second), withExchange("coinbase"), withSymbol("BTC-USD")),
		testTrade("15", timestamp.Add(15*time.Second), withExchange("coinbase"), withSymbol("BTC-USD")),
		testTrade("15", timestamp.Add(16*time.Second), withExchange("coinbase"), withSymbol("BTC-USD")),
	}
	trades[3].Ticks = 3
	for _, trade := range trades {
		if err := buffer.RawPublisher().Publish(context.Background(), trade); err != nil {
			t.Fatal(err)
		}
		if err := buffer.AggregatedPublisher().Publish(context.Background(), trade); err != nil {
			t.Fatal(err)
		}
	}

	key := newWebSocketDataBucketKey("coinbase", "BTC-USD", 1000, timestamp)
	buffer.mu.Lock()
	bucket := buffer.buckets[key]
	buffer.mu.Unlock()
	assertTradeUIDs(t, bucket.RawTrades, []string{"10", "15", "20"})
	assertTradeUIDs(t, bucket.AggregatedTrades, []string{"10", "15", "20"})
	if !bucket.RawTrades[1].Timestamp.Equal(timestamp.Add(15 * time.Second)) {
		t.Fatalf("raw duplicate UID should preserve first trade, got timestamp %s", bucket.RawTrades[1].Timestamp)
	}
	if !bucket.AggregatedTrades[1].Timestamp.Equal(timestamp.Add(16*time.Second)) || bucket.AggregatedTrades[1].Ticks != 3 {
		t.Fatalf("aggregated duplicate UID should preserve the greater-ticks variant: %#v", bucket.AggregatedTrades[1])
	}
}

func TestWebSocketDataTradesPreserveBybitFeedOrderForEqualTimestamps(t *testing.T) {
	for _, exchange := range []string{"bybit", "bybit-linear", "bybit-inverse"} {
		t.Run(exchange, func(t *testing.T) {
			timestamp := time.Now().UTC().Truncate(time.Millisecond)
			incoming := []TradeEvent{
				testTrade(
					"bbb107e9-f7e8-53f3-b18a-ac9781c6eae8",
					timestamp,
					withExchange(exchange),
					withSymbol("BTCUSDT"),
				),
				testTrade(
					"aaa107e9-f7e8-53f3-b18a-ac9781c6eae8",
					timestamp,
					withExchange(exchange),
					withSymbol("BTCUSDT"),
				),
			}

			var inserted []TradeEvent
			var seen map[string]struct{}
			for _, trade := range incoming {
				inserted, seen = insertWebSocketDataTrade(exchange, RawTrades, inserted, seen, trade)
			}
			assertTradeUIDs(t, inserted, []string{incoming[0].UID, incoming[1].UID})

			canonical := canonicalWebSocketDataTrades(exchange, RawTrades, incoming)
			assertTradeUIDs(t, canonical, []string{incoming[0].UID, incoming[1].UID})
		})
	}
}

func TestWebSocketDataTradesPreserveHyperliquidFeedOrderForEqualTimestamps(t *testing.T) {
	timestamp := time.Now().UTC().Truncate(time.Millisecond)
	incoming := []TradeEvent{
		testTrade(
			"1775606400000:BTC:20",
			timestamp,
			withExchange("hyperliquid"),
			withSymbol("BTC"),
		),
		testTrade(
			"1775606400000:BTC:10",
			timestamp,
			withExchange("hyperliquid"),
			withSymbol("BTC"),
		),
	}
	canonical := canonicalWebSocketDataTrades("hyperliquid", RawTrades, incoming)
	assertTradeUIDs(t, canonical, []string{"1775606400000:BTC:20", "1775606400000:BTC:10"})
}

func TestWebSocketDataBufferPreservesTradePublishedDuringFlush(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	db.SetMaxOpenConns(1)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{DefaultSignificantTradeFilter: MustDecimal("1000")},
	)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	first := testTrade("10", timestamp.Add(10*time.Second), withExchange("coinbase"), withSymbol("BTC-USD"))
	late := testTrade("15", timestamp.Add(15*time.Second), withExchange("coinbase"), withSymbol("BTC-USD"))
	if err := buffer.RawPublisher().Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}

	heldConnection, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			_ = heldConnection.Close()
		}
	}()

	type flushResult struct {
		count int
		err   error
	}
	flushContext, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFlush()
	result := make(chan flushResult, 1)
	initialWaitCount := db.Stats().WaitCount
	go func() {
		count, err := buffer.Flush(flushContext)
		result <- flushResult{count: count, err: err}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for db.Stats().WaitCount == initialWaitCount {
		if time.Now().After(deadline) {
			t.Fatal("flush did not reach the blocked database write")
		}
		time.Sleep(time.Millisecond)
	}
	if err := buffer.RawPublisher().Publish(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if err := heldConnection.Close(); err != nil {
		t.Fatal(err)
	}
	released = true

	flush := <-result
	if flush.err != nil {
		t.Fatal(flush.err)
	}
	if flush.count != 1 {
		t.Fatalf("flushed buckets = %d, want 1", flush.count)
	}
	if count, err := buffer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("second flush buckets = %d, want 1", count)
	}

	row := getWebSocketDataTestRow(t, db)
	assertTradeUIDs(t, row.rawTrades, []string{"10", "15"})
}

func TestWebSocketDataBufferRestoresBucketAfterFlushFailure(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{DefaultSignificantTradeFilter: MustDecimal("1000")},
	)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	trade := testTrade("10", timestamp.Add(10*time.Second), withExchange("coinbase"), withSymbol("BTC-USD"))
	if err := buffer.RawPublisher().Publish(context.Background(), trade); err != nil {
		t.Fatal(err)
	}

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if count, err := buffer.Flush(canceledContext); err == nil {
		t.Fatal("expected canceled flush to fail")
	} else if count != 0 {
		t.Fatalf("flushed buckets = %d, want 0", count)
	}
	if count, err := buffer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("retry flushed buckets = %d, want 1", count)
	}

	row := getWebSocketDataTestRow(t, db)
	assertTradeUIDs(t, row.rawTrades, []string{"10"})
}

func TestWebSocketDataBufferUsesSymbolThreshold(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{
			DefaultSignificantTradeFilter: MustDecimal("1000"),
			SignificantThresholds: map[string]Decimal{
				ExchangeSymbolKey("coinbase", "BTC-USD"): MustDecimal("500"),
			},
		},
	)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	raw := testTrade(
		"raw-1",
		timestamp.Add(10*time.Second),
		withExchange("coinbase"),
		withSymbol("BTC-USD"),
	)

	if err := buffer.RawPublisher().Publish(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.FlushBefore(
		context.Background(),
		"coinbase",
		"BTC-USD",
		timestamp.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	if row.significantTradeFilter != 500 {
		t.Fatalf("significant trade filter = %d, want 500", row.significantTradeFilter)
	}
}

func TestWebSocketDataStoreMergesExistingBucketOnConflict(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	existing := WebSocketDataBucket{
		Exchange:               "coinbase",
		APISymbol:              "BTC-USD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"raw-existing",
				timestamp.Add(10*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
			),
		},
	}
	incoming := WebSocketDataBucket{
		Exchange:               "coinbase",
		APISymbol:              "BTC-USD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"raw-incoming",
				timestamp.Add(20*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
			),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{existing}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{incoming}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	if len(row.rawTrades) != 2 {
		t.Fatalf("raw trades = %#v", row.rawTrades)
	}
	if row.rawTrades[0].UID != "raw-existing" || row.rawTrades[1].UID != "raw-incoming" {
		t.Fatalf("raw trades = %#v", row.rawTrades)
	}
	if len(row.filteredTrades) != 0 {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
}

func TestWebSocketDataStoreChronologicallyMergesLateTradesAndDeduplicatesUIDs(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	trade := func(uid string, offset time.Duration, price string) TradeEvent {
		return testTrade(
			uid,
			timestamp.Add(offset),
			withExchange("coinbase"),
			withSymbol("BTC-USD"),
			withPrice(price),
			withNotional("12"),
		)
	}
	first := WebSocketDataBucket{
		Exchange:               "coinbase",
		APISymbol:              "BTC-USD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades:              []TradeEvent{trade("10", 10*time.Second, "100"), trade("20", 20*time.Second, "102")},
		AggregatedTrades:       []TradeEvent{trade("10", 10*time.Second, "100"), trade("20", 20*time.Second, "102")},
	}
	late := WebSocketDataBucket{
		Exchange:               "coinbase",
		APISymbol:              "BTC-USD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			trade("20", 5*time.Second, "999"),
			trade("15", 15*time.Second, "101"),
			trade("15", 16*time.Second, "998"),
		},
		AggregatedTrades: []TradeEvent{
			trade("20", 5*time.Second, "999"),
			trade("15", 15*time.Second, "101"),
			trade("15", 16*time.Second, "998"),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{first}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{late}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{late}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	assertTradeUIDs(t, row.rawTrades, []string{"10", "15", "20"})
	assertTradeUIDs(t, row.aggregatedTrades, []string{"10", "15", "20"})
	assertSignificantTradeUIDs(t, row.filteredTrades, []string{"10", "15", "20"})
	assertDecimal(t, row.rawTrades[1].Price, "101")
	assertDecimal(t, row.rawTrades[2].Price, "102")
}

func TestWebSocketDataStoreCoalescesLateSameSampleAggregatesAfterSorting(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	bucket := WebSocketDataBucket{
		Exchange:               "hyperliquid",
		APISymbol:              "BTC",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		AggregatedTrades: []TradeEvent{
			testTrade(
				"10",
				timestamp.Add(10*time.Second),
				withExchange("hyperliquid"),
				withSymbol("BTC"),
				withNotional("6"),
			),
			testTrade(
				"20",
				timestamp.Add(20*time.Second),
				withExchange("hyperliquid"),
				withSymbol("BTC"),
				withNotional("1"),
			),
			testTrade(
				"11",
				timestamp.Add(10*time.Second),
				withExchange("hyperliquid"),
				withSymbol("BTC"),
				withNotional("6"),
			),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{bucket}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	assertTradeUIDs(t, row.aggregatedTrades, []string{"10", "20"})
	if row.aggregatedTrades[0].Ticks != 2 {
		t.Fatalf("coalesced ticks = %d, want 2", row.aggregatedTrades[0].Ticks)
	}
	assertDecimal(t, row.aggregatedTrades[0].Volume, "1200")
	if len(row.filteredTrades) != 2 || row.filteredTrades[0].UID != "10" {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].Volume == nil {
		t.Fatal("coalesced significant trade should retain per-trade volume")
	}
	assertDecimal(t, *row.filteredTrades[0].Volume, "1200")
}

func TestWebSocketDataStorePrefersGreaterTicksForDuplicateAggregateUID(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	partial := testTrade(
		"10",
		timestamp.Add(10*time.Second),
		withExchange("coinbase"),
		withSymbol("BTC-USD"),
		withNotional("1"),
	)
	complete := testTrade(
		"10",
		timestamp.Add(10*time.Second),
		withExchange("coinbase"),
		withSymbol("BTC-USD"),
		withNotional("3"),
	)
	complete.Ticks = 3

	for _, trade := range []TradeEvent{partial, complete} {
		if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{{
			Exchange:               "coinbase",
			APISymbol:              "BTC-USD",
			SignificantTradeFilter: 1000,
			Timestamp:              timestamp,
			AggregatedTrades:       []TradeEvent{trade},
		}}); err != nil {
			t.Fatal(err)
		}
	}

	row := getWebSocketDataTestRow(t, db)
	if len(row.aggregatedTrades) != 1 {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if row.aggregatedTrades[0].Ticks != 3 {
		t.Fatalf("aggregate ticks = %d, want 3", row.aggregatedTrades[0].Ticks)
	}
	assertDecimal(t, row.aggregatedTrades[0].Volume, "300")
}

func TestWebSocketDataStoreDerivesFilteredTradesFromAggregatedBucket(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	existing := WebSocketDataBucket{
		Exchange:               "coinbase",
		APISymbol:              "BTC-USD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		AggregatedTrades: []TradeEvent{
			testTrade(
				"context-before",
				timestamp.Add(10*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
				withPrice("100"),
				withNotional("1"),
			),
		},
		FilteredTrades: []SignificantTrade{testSignificantTrade("stale-filtered", timestamp.Add(10*time.Second))},
	}
	incoming := WebSocketDataBucket{
		Exchange:               "coinbase",
		APISymbol:              "BTC-USD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		AggregatedTrades: []TradeEvent{
			testTrade(
				"significant",
				timestamp.Add(20*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
				withPrice("100"),
				withNotional("12"),
			),
			testTrade(
				"context-1",
				timestamp.Add(30*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
				withPrice("101"),
				withNotional("2"),
			),
			testTrade(
				"context-2",
				timestamp.Add(40*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
				withPrice("99"),
				withNotional("3"),
			),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{existing}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{incoming}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	if len(row.aggregatedTrades) != 4 {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if len(row.filteredTrades) != 2 {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].UID != "significant" {
		t.Fatalf("filtered trades should be re-derived from aggregated trades: %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].Volume == nil {
		t.Fatalf("significant filtered trade volume = nil, want value")
	}
	assertDecimal(t, *row.filteredTrades[0].Volume, "1200")
	assertDecimal(t, row.filteredTrades[0].TotalVolume, "1300")

	contextRow := row.filteredTrades[1]
	if contextRow.UID != "context-2" {
		t.Fatalf("context row uid = %s, want context-2", contextRow.UID)
	}
	if contextRow.Volume != nil || contextRow.Notional != nil || contextRow.TickRule != nil || contextRow.Ticks != nil {
		t.Fatalf("context row should not have per-trade fields: %#v", contextRow)
	}
	assertDecimal(t, contextRow.High, "101")
	assertDecimal(t, contextRow.Low, "99")
	assertDecimal(t, contextRow.TotalVolume, "499")
	assertDecimal(t, contextRow.TotalNotional, "5")
}

func TestWebSocketDataStoreDerivesBitfinexDerivativeTradesByUID(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	bucket := WebSocketDataBucket{
		Exchange:               "bitfinex",
		APISymbol:              "tBTCF0:USTF0",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"102",
				timestamp.Add(20*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCF0:USTF0"),
				withPrice("100"),
				withNotional("12"),
			),
			testTrade(
				"101",
				timestamp.Add(10*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCF0:USTF0"),
				withPrice("101"),
				withNotional("2"),
			),
			testTrade(
				"100",
				timestamp.Add(10*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCF0:USTF0"),
				withPrice("100"),
				withNotional("1"),
			),
			testTrade(
				"103",
				timestamp.Add(30*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCF0:USTF0"),
				withPrice("99"),
				withNotional("3"),
			),
		},
		AggregatedTrades: []TradeEvent{
			testTrade("stale-aggregated", timestamp, withExchange("bitfinex"), withSymbol("tBTCF0:USTF0")),
		},
		FilteredTrades: []SignificantTrade{
			testSignificantTrade("stale-filtered", timestamp),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{bucket}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	if len(row.rawTrades) != 4 || row.rawTrades[0].UID != "100" {
		t.Fatalf("raw trades should be canonicalized by Bitfinex UID order: %#v", row.rawTrades)
	}
	if len(row.aggregatedTrades) != 3 {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if row.aggregatedTrades[0].UID != "100" || row.aggregatedTrades[1].UID != "102" || row.aggregatedTrades[2].UID != "103" {
		t.Fatalf("aggregated trades should be rebuilt from Bitfinex UID order: %#v", row.aggregatedTrades)
	}
	assertDecimal(t, row.aggregatedTrades[0].Price, "101")
	assertDecimal(t, row.aggregatedTrades[0].Volume, "302")
	if row.aggregatedTrades[0].Ticks != 2 {
		t.Fatalf("first aggregate ticks = %d, want 2", row.aggregatedTrades[0].Ticks)
	}

	if len(row.filteredTrades) != 2 {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].UID != "102" {
		t.Fatalf("filtered trades should be derived from rebuilt aggregated trades: %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].Volume == nil {
		t.Fatalf("significant filtered trade volume = nil, want value")
	}
	assertDecimal(t, *row.filteredTrades[0].Volume, "1200")
	assertDecimal(t, row.filteredTrades[0].TotalVolume, "1502")
	if row.filteredTrades[1].UID != "103" {
		t.Fatalf("context filtered uid = %s, want 103", row.filteredTrades[1].UID)
	}
	assertDecimal(t, row.filteredTrades[1].TotalVolume, "297")
}

func TestWebSocketDataStoreDerivesBitfinexSpotTradesFromRaw(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	bucket := WebSocketDataBucket{
		Exchange:               "bitfinex",
		APISymbol:              "tBTCUSD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"102",
				timestamp.Add(20*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("100"),
				withNotional("12"),
			),
			testTrade(
				"101",
				timestamp.Add(10*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("101"),
				withNotional("2"),
			),
			testTrade(
				"100",
				timestamp.Add(10*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("100"),
				withNotional("1"),
			),
			testTrade(
				"103",
				timestamp.Add(30*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("99"),
				withNotional("3"),
			),
		},
		AggregatedTrades: []TradeEvent{
			testTrade(
				"spot-aggregated",
				timestamp.Add(10*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("105"),
				withNotional("12"),
			),
		},
		FilteredTrades: []SignificantTrade{
			testSignificantTrade("stale-filtered", timestamp),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{bucket}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	if len(row.rawTrades) != 4 || row.rawTrades[0].UID != "100" || row.rawTrades[1].UID != "101" {
		t.Fatalf("spot raw trades should be canonicalized by Bitfinex UID order: %#v", row.rawTrades)
	}
	if len(row.aggregatedTrades) != 3 {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if row.aggregatedTrades[0].UID != "100" || row.aggregatedTrades[1].UID != "102" || row.aggregatedTrades[2].UID != "103" {
		t.Fatalf("spot aggregated trades should be rebuilt from canonical raw order: %#v", row.aggregatedTrades)
	}
	assertDecimal(t, row.aggregatedTrades[0].Price, "101")
	assertDecimal(t, row.aggregatedTrades[0].Volume, "302")
	if row.aggregatedTrades[0].Ticks != 2 {
		t.Fatalf("first aggregate ticks = %d, want 2", row.aggregatedTrades[0].Ticks)
	}
	assertDecimal(t, row.aggregatedTrades[1].Price, "100")
	assertDecimal(t, row.aggregatedTrades[1].Volume, "1200")
	assertDecimal(t, row.aggregatedTrades[2].Price, "99")
	assertDecimal(t, row.aggregatedTrades[2].Volume, "297")

	if len(row.filteredTrades) != 2 {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].UID != "102" {
		t.Fatalf("filtered trades should be derived from rebuilt spot aggregated trades: %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].Volume == nil {
		t.Fatalf("significant filtered trade volume = nil, want value")
	}
	assertDecimal(t, *row.filteredTrades[0].Volume, "1200")
	assertDecimal(t, row.filteredTrades[0].TotalVolume, "1502")
	if row.filteredTrades[1].UID != "103" {
		t.Fatalf("context filtered uid = %s, want 103", row.filteredTrades[1].UID)
	}
	assertDecimal(t, row.filteredTrades[1].TotalVolume, "297")
}

func TestWebSocketDataStoreDerivesBitfinexSpotExactTiesByUID(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	bucket := WebSocketDataBucket{
		Exchange:               "bitfinex",
		APISymbol:              "tBTCUSD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"1919344204",
				timestamp.Add(17*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("78161"),
				withNotional("0.0002"),
				withTickRule(-1),
			),
			testTrade(
				"1919344205",
				timestamp.Add(17*time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("78162"),
				withNotional("0.064"),
				withTickRule(-1),
			),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{bucket}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	if len(row.rawTrades) != 2 || row.rawTrades[0].UID != "1919344204" || row.rawTrades[1].UID != "1919344205" {
		t.Fatalf("spot exact timestamp ties should be sorted by UID: %#v", row.rawTrades)
	}
	if len(row.aggregatedTrades) != 1 {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if row.aggregatedTrades[0].UID != "1919344204" {
		t.Fatalf("aggregate uid = %s, want first trade uid", row.aggregatedTrades[0].UID)
	}
	assertDecimal(t, row.aggregatedTrades[0].Price, "78162")
	assertDecimal(t, row.aggregatedTrades[0].Notional, "0.0642")
	if row.aggregatedTrades[0].Ticks != 2 {
		t.Fatalf("aggregate ticks = %d, want 2", row.aggregatedTrades[0].Ticks)
	}
}

func TestWebSocketDataStorePreservesBitfinexSpotUIDOrderAcrossMerge(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	firstBucket := WebSocketDataBucket{
		Exchange:               "bitfinex",
		APISymbol:              "tBTCUSD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"1919360086",
				timestamp.Add(2*time.Second+754*time.Millisecond),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("78076"),
				withNotional("0.0002"),
			),
			testTrade(
				"1919360087",
				timestamp.Add(2*time.Second+754*time.Millisecond),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("78078"),
				withNotional("0.00004175"),
			),
		},
	}

	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{firstBucket}); err != nil {
		t.Fatal(err)
	}

	secondBucket := WebSocketDataBucket{
		Exchange:               "bitfinex",
		APISymbol:              "tBTCUSD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"1919360087",
				timestamp.Add(time.Second),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("1"),
				withNotional("1"),
			),
			testTrade(
				"1919360090",
				timestamp.Add(2*time.Second+757*time.Millisecond),
				withExchange("bitfinex"),
				withSymbol("tBTCUSD"),
				withPrice("78079"),
				withNotional("0.00503741"),
			),
		},
	}
	if err := store.UpsertBuckets(context.Background(), []WebSocketDataBucket{secondBucket}); err != nil {
		t.Fatal(err)
	}

	row := getWebSocketDataTestRow(t, db)
	if len(row.rawTrades) != 3 || row.rawTrades[0].UID != "1919360086" || row.rawTrades[1].UID != "1919360087" {
		t.Fatalf("spot UID order should survive bucket merge: %#v", row.rawTrades)
	}
	assertDecimal(t, row.rawTrades[1].Price, "78078")
	if len(row.aggregatedTrades) != 2 {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if row.aggregatedTrades[0].UID != "1919360086" {
		t.Fatalf("aggregate uid = %s, want first persisted tie uid", row.aggregatedTrades[0].UID)
	}
	assertDecimal(t, row.aggregatedTrades[0].Price, "78078")
	assertDecimal(t, row.aggregatedTrades[0].Volume, "18.87495650")
}

func TestWebSocketDataStoreDeletesRowsOlderThanRetention(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	store := NewWebSocketDataStore(db)
	oldTimestamp := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	_, err := db.Exec(
		`INSERT INTO quant_tick_websocket_data (
			exchange, api_symbol, significant_trade_filter, timestamp,
			raw_trades, aggregated_trades, filtered_trades
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"coinbase",
		"BTC-USD",
		int64(1000),
		oldTimestamp,
		"[]",
		"[]",
		"[]",
	)
	if err != nil {
		t.Fatal(err)
	}

	newTimestamp := time.Now().UTC().Truncate(time.Minute)
	err = store.UpsertBuckets(context.Background(), []WebSocketDataBucket{
		{
			Exchange:               "coinbase",
			APISymbol:              "BTC-USD",
			SignificantTradeFilter: 1000,
			Timestamp:              newTimestamp,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if countWebSocketDataTestRows(t, db) != 1 {
		t.Fatal("old websocket data row should be deleted")
	}
}

func TestWebSocketDataBufferRejectsNonIntegerThreshold(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{DefaultSignificantTradeFilter: MustDecimal("1000.5")},
	)
	trade := testTrade("raw-1", time.Now().UTC().Truncate(time.Minute))

	err := buffer.RawPublisher().Publish(context.Background(), trade)
	if err == nil {
		t.Fatal("expected non-integer threshold error")
	}
}

type websocketDataTestRow struct {
	significantTradeFilter int64
	rawTrades              []TradeEvent
	aggregatedTrades       []TradeEvent
	filteredTrades         []SignificantTrade
}

func newWebSocketDataTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
		CREATE TABLE quant_tick_websocket_data (
			exchange TEXT NOT NULL,
			api_symbol TEXT NOT NULL,
			significant_trade_filter INTEGER NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			raw_trades TEXT NOT NULL DEFAULT '[]',
			aggregated_trades TEXT NOT NULL DEFAULT '[]',
			filtered_trades TEXT NOT NULL DEFAULT '[]',
			UNIQUE(exchange, api_symbol, significant_trade_filter, timestamp)
		)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func getWebSocketDataTestRow(t *testing.T, db *sql.DB) websocketDataTestRow {
	t.Helper()
	var row websocketDataTestRow
	var rawTrades, aggregatedTrades, filteredTrades string
	err := db.QueryRow(`
		SELECT significant_trade_filter, raw_trades, aggregated_trades, filtered_trades
		FROM quant_tick_websocket_data`).Scan(
		&row.significantTradeFilter,
		&rawTrades,
		&aggregatedTrades,
		&filteredTrades,
	)
	if err != nil {
		t.Fatal(err)
	}
	unmarshalWebSocketDataTestPayload(t, rawTrades, &row.rawTrades)
	unmarshalWebSocketDataTestPayload(t, aggregatedTrades, &row.aggregatedTrades)
	unmarshalWebSocketDataTestPayload(t, filteredTrades, &row.filteredTrades)
	return row
}

func countWebSocketDataTestRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM quant_tick_websocket_data").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func unmarshalWebSocketDataTestPayload(t *testing.T, payload string, dst any) {
	t.Helper()
	if err := json.Unmarshal([]byte(payload), dst); err != nil {
		t.Fatal(err)
	}
}

func assertTradeUIDs(t *testing.T, trades []TradeEvent, want []string) {
	t.Helper()
	if len(trades) != len(want) {
		t.Fatalf("trade count = %d, want %d: %#v", len(trades), len(want), trades)
	}
	for index, uid := range want {
		if trades[index].UID != uid {
			t.Fatalf("trade UIDs[%d] = %s, want %s: %#v", index, trades[index].UID, uid, trades)
		}
	}
}

func assertSignificantTradeUIDs(t *testing.T, trades []SignificantTrade, want []string) {
	t.Helper()
	if len(trades) != len(want) {
		t.Fatalf("significant trade count = %d, want %d: %#v", len(trades), len(want), trades)
	}
	for index, uid := range want {
		if trades[index].UID != uid {
			t.Fatalf("significant trade UIDs[%d] = %s, want %s: %#v", index, trades[index].UID, uid, trades)
		}
	}
}

func testSignificantTrade(uid string, timestamp time.Time) SignificantTrade {
	return SignificantTrade{
		Exchange:               "coinbase",
		UID:                    uid,
		Symbol:                 "BTC-USD",
		Timestamp:              timestamp,
		Price:                  MustDecimal("100"),
		High:                   MustDecimal("100"),
		Low:                    MustDecimal("100"),
		TotalBuyVolume:         MustDecimal("100"),
		TotalVolume:            MustDecimal("100"),
		TotalBuyNotional:       MustDecimal("1"),
		TotalNotional:          MustDecimal("1"),
		TotalBuyTicks:          1,
		TotalTicks:             1,
		SignificantTradeFilter: MustDecimal("1000"),
	}
}
