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
	filtered := testSignificantTrade("filtered-1", timestamp.Add(30*time.Second))

	if err := buffer.RawPublisher().Publish(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if err := buffer.AggregatedPublisher().Publish(context.Background(), aggregated); err != nil {
		t.Fatal(err)
	}
	if err := buffer.SignificantPublisher().Publish(context.Background(), filtered); err != nil {
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

func TestWebSocketDataBufferKeepsSameMinuteEventsInSameBucketBeforeNextMinute(t *testing.T) {
	db := newWebSocketDataTestDB(t)
	buffer := NewWebSocketDataBuffer(
		NewWebSocketDataStore(db),
		WebSocketDataBufferConfig{DefaultSignificantTradeFilter: MustDecimal("1000")},
	)
	timestamp := time.Now().UTC().Truncate(time.Minute)
	filtered := testSignificantTrade("filtered-1", timestamp.Add(10*time.Second))

	if err := buffer.SignificantPublisher().Publish(context.Background(), filtered); err != nil {
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

	raw := testTrade(
		"raw-1",
		timestamp.Add(20*time.Second),
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
	if len(row.rawTrades) != 1 || row.rawTrades[0].UID != "raw-1" {
		t.Fatalf("raw trades = %#v", row.rawTrades)
	}
	if len(row.filteredTrades) != 1 || row.filteredTrades[0].UID != "filtered-1" {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
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
		FilteredTrades:         []SignificantTrade{testSignificantTrade("filtered-1", timestamp.Add(10*time.Second))},
	}
	incoming := WebSocketDataBucket{
		Exchange:               "coinbase",
		APISymbol:              "BTC-USD",
		SignificantTradeFilter: 1000,
		Timestamp:              timestamp,
		RawTrades: []TradeEvent{
			testTrade(
				"raw-1",
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
	if len(row.rawTrades) != 1 || row.rawTrades[0].UID != "raw-1" {
		t.Fatalf("raw trades = %#v", row.rawTrades)
	}
	if len(row.filteredTrades) != 1 || row.filteredTrades[0].UID != "filtered-1" {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
}

func TestWebSocketDataStoreDerivesFilteredTradesFromMergedAggregatedTrades(t *testing.T) {
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
				"aggregated-1",
				timestamp.Add(10*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
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
				"aggregated-2",
				timestamp.Add(20*time.Second),
				withExchange("coinbase"),
				withSymbol("BTC-USD"),
				withNotional("10"),
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
	if len(row.aggregatedTrades) != 2 {
		t.Fatalf("aggregated trades = %#v", row.aggregatedTrades)
	}
	if len(row.filteredTrades) != 1 {
		t.Fatalf("filtered trades = %#v", row.filteredTrades)
	}
	if row.filteredTrades[0].UID != "aggregated-2" {
		t.Fatalf("filtered trades should be re-derived from aggregated trades: %#v", row.filteredTrades)
	}
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
