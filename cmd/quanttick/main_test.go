package main

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
	_ "modernc.org/sqlite"
)

func TestWebSocketDataStreamsDefaultsToSignificantTrades(t *testing.T) {
	t.Setenv("WEBSOCKET_DATA_STREAMS", "")

	streams, err := websocketDataStreams()
	if err != nil {
		t.Fatal(err)
	}
	if !streams[quanttick.SignificantTrades] {
		t.Fatalf("expected significant trades stream to be enabled by default: %#v", streams)
	}
}

func TestWebSocketDataStreamsRejectsUnknownStream(t *testing.T) {
	t.Setenv("WEBSOCKET_DATA_STREAMS", "raw-trades,unknown")

	if _, err := websocketDataStreams(); err == nil {
		t.Fatal("expected error for unknown stream")
	}
}

func TestPublisherModeDefaultsToDB(t *testing.T) {
	if got := publisherMode(""); got != "db" {
		t.Fatalf("publisher mode = %s, want db", got)
	}
}

func TestPublisherModeNormalizesValue(t *testing.T) {
	if got := publisherMode(" STDOUT "); got != "stdout" {
		t.Fatalf("publisher mode = %s, want stdout", got)
	}
}

func TestUpdateFlushWatermarkKeepsPerSymbolHighWater(t *testing.T) {
	watermarks := make(map[string]time.Time)
	start := time.Date(2026, 5, 16, 2, 12, 0, 0, time.UTC)
	latest := start.Add(6 * time.Minute)
	late := start.Add(46 * time.Second)

	if got := updateFlushWatermark(watermarks, testCommandTrade("1", "bitmex", "XBTUSD", start)); !got.Equal(start) {
		t.Fatalf("first watermark = %s, want %s", got, start)
	}
	if got := updateFlushWatermark(watermarks, testCommandTrade("2", "bitmex", "XBTUSD", latest)); !got.Equal(latest) {
		t.Fatalf("advanced watermark = %s, want %s", got, latest)
	}
	if got := updateFlushWatermark(watermarks, testCommandTrade("3", "bitmex", "XBTUSD", late)); !got.Equal(latest) {
		t.Fatalf("late watermark = %s, want %s", got, latest)
	}

	other := start.Add(time.Minute)
	if got := updateFlushWatermark(watermarks, testCommandTrade("4", "coinbase", "BTC-USD", other)); !got.Equal(other) {
		t.Fatalf("other symbol watermark = %s, want %s", got, other)
	}
}

func TestNewPipelineFromEnvUsesStdoutPublisher(t *testing.T) {
	t.Setenv("WEBSOCKET_DATA_STREAMS", "raw-trades")

	var output bytes.Buffer
	pipeline, cleanup, _, err := newPipelineFromEnv(context.Background(), &output, nil, "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	if pipeline.RawPublisher == nil {
		t.Fatal("raw publisher should be configured")
	}
}

func TestNewPipelineFromEnvUsesDatabasePublisher(t *testing.T) {
	oldOpenDatabase := openDatabaseFunc
	t.Cleanup(func() { openDatabaseFunc = oldOpenDatabase })
	openDatabaseFunc = func(context.Context) (*sql.DB, func() error, error) {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			return nil, nil, err
		}
		return db, db.Close, nil
	}

	pipeline, cleanup, flusher, err := newPipelineFromEnv(context.Background(), &bytes.Buffer{}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	if pipeline.AggregatedPublisher == nil {
		t.Fatal("aggregated publisher should be configured so DB filtered trades can be derived")
	}
	if pipeline.SignificantPublisher != nil {
		t.Fatal("DB publisher should derive filtered trades from aggregated buckets, not stream significant trades directly")
	}
	if flusher == nil {
		t.Fatal("database publisher should return a bucket flusher")
	}
}

func TestNewPipelineFromEnvDefaultsToDatabase(t *testing.T) {
	if _, _, _, err := newPipelineFromEnv(context.Background(), &bytes.Buffer{}, nil, ""); err == nil {
		t.Fatal("expected missing database config error")
	}
}

func TestCloudSQLInstanceConnectionNameStripsSocketPrefix(t *testing.T) {
	t.Setenv("PRODUCTION_DATABASE_HOST", "/cloudsql/project:region:dqt")

	if got := cloudSQLInstanceConnectionName(); got != "project:region:dqt" {
		t.Fatalf("instance connection name = %s, want project:region:dqt", got)
	}
}

func TestNewPipelineFromEnvRejectsUnknownPublisher(t *testing.T) {
	if _, _, _, err := newPipelineFromEnv(context.Background(), &bytes.Buffer{}, nil, "unknown"); err == nil {
		t.Fatal("expected unknown publisher error")
	}
}

func TestExchangeSymbolsEnvParsesThresholdOverrides(t *testing.T) {
	t.Setenv("BINANCE_SYMBOLS", "BTCUSDT=50000,ETHUSDT")

	symbols, thresholds, err := exchangeSymbolsEnv("BINANCE_SYMBOLS", exchanges.BinanceName)
	if err != nil {
		t.Fatal(err)
	}
	if len(symbols) != 2 || symbols[0] != "BTCUSDT" || symbols[1] != "ETHUSDT" {
		t.Fatalf("symbols = %#v, want BTCUSDT and ETHUSDT", symbols)
	}

	threshold, ok := thresholds[quanttick.ExchangeSymbolKey(exchanges.BinanceName, "BTCUSDT")]
	if !ok {
		t.Fatal("expected BTCUSDT threshold")
	}
	if !threshold.Equal(quanttick.MustDecimal("50000")) {
		t.Fatalf("threshold = %s, want 50000", threshold)
	}
	if _, ok := thresholds[quanttick.ExchangeSymbolKey(exchanges.BinanceName, "ETHUSDT")]; ok {
		t.Fatal("ETHUSDT should use the default threshold")
	}
}

func TestExchangesFromEnvBuildsClientsAndThresholds(t *testing.T) {
	t.Setenv("BINANCE_SYMBOLS", "BTCUSDT=50000,ETHUSDT")

	clients, thresholds, err := exchangesFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 1 {
		t.Fatalf("clients = %d, want 1", len(clients))
	}

	threshold, ok := thresholds[quanttick.ExchangeSymbolKey(exchanges.BinanceName, "BTCUSDT")]
	if !ok {
		t.Fatal("expected BTCUSDT threshold")
	}
	if !threshold.Equal(quanttick.MustDecimal("50000")) {
		t.Fatalf("threshold = %s, want 50000", threshold)
	}
}

func TestExchangesFromEnvAppliesConfiguredMarketThresholds(t *testing.T) {
	t.Setenv("BITMEX_SYMBOLS", "XBTUSD,XBT_USDT=25000")
	t.Setenv("HYPERLIQUID_SYMBOLS", "BTC,PURR/USDC=25000")

	clients, thresholds, err := exchangesFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	wantClients := 2
	if len(clients) != wantClients {
		t.Fatalf("clients = %d, want %d", len(clients), wantClients)
	}

	threshold, ok := thresholds[quanttick.ExchangeSymbolKey(exchanges.BitmexName, "XBT_USDT")]
	if !ok {
		t.Fatal("expected XBT_USDT threshold")
	}
	if !threshold.Equal(quanttick.MustDecimal("25000")) {
		t.Fatalf("threshold = %s, want 25000", threshold)
	}
	threshold, ok = thresholds[quanttick.ExchangeSymbolKey(exchanges.HyperliquidName, "PURR/USDC")]
	if !ok {
		t.Fatal("expected PURR/USDC threshold")
	}
	if !threshold.Equal(quanttick.MustDecimal("25000")) {
		t.Fatalf("threshold = %s, want 25000", threshold)
	}
}

func TestSignificantThresholdReadsEnv(t *testing.T) {
	t.Setenv("SIGNIFICANT_TRADE_FILTER", "2500.5")

	threshold, err := significantThreshold()
	if err != nil {
		t.Fatal(err)
	}
	if !threshold.Equal(quanttick.MustDecimal("2500.5")) {
		t.Fatalf("threshold = %s, want 2500.5", threshold)
	}
}

func TestSignificantThresholdAllowsZero(t *testing.T) {
	t.Setenv("SIGNIFICANT_TRADE_FILTER", "0")

	threshold, err := significantThreshold()
	if err != nil {
		t.Fatal(err)
	}
	if !threshold.Equal(quanttick.MustDecimal("0")) {
		t.Fatalf("threshold = %s, want 0", threshold)
	}
}

func TestShutdownFlushTimeoutReadsDuration(t *testing.T) {
	t.Setenv("SHUTDOWN_FLUSH_TIMEOUT", "3s")

	if got := shutdownFlushTimeout(); got != 3*time.Second {
		t.Fatalf("timeout = %s, want 3s", got)
	}
}

func TestShutdownFlushTimeoutFallsBackForInvalidDuration(t *testing.T) {
	t.Setenv("SHUTDOWN_FLUSH_TIMEOUT", "bad")

	if got := shutdownFlushTimeout(); got != 10*time.Second {
		t.Fatalf("timeout = %s, want 10s", got)
	}
}

func TestExchangesFromEnvWithBlankSymbolsDoesNothing(t *testing.T) {
	t.Setenv("BINANCE_SYMBOLS", "")

	clients, thresholds, err := exchangesFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 0 {
		t.Fatalf("clients = %d, want 0", len(clients))
	}
	if len(thresholds) != 0 {
		t.Fatalf("thresholds = %#v, want empty", thresholds)
	}
}

func testCommandTrade(uid string, exchange string, symbol string, timestamp time.Time) quanttick.TradeEvent {
	return quanttick.NewTradeEvent(quanttick.TradeEventInput{
		Exchange:     exchange,
		UID:          uid,
		Symbol:       symbol,
		Timestamp:    timestamp,
		ReceivedAt:   timestamp,
		Price:        quanttick.MustDecimal("100"),
		Notional:     quanttick.MustDecimal("1"),
		TickRule:     1,
		IsSequential: true,
	})
}
