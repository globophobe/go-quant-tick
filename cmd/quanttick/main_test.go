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

	if pipeline.SignificantPublisher == nil {
		t.Fatal("significant publisher should be configured")
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

	symbols, thresholds, err := exchangeSymbolsEnv("BINANCE_SYMBOLS", exchanges.BinanceName, []string{"BTCUSDT"})
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
	for _, name := range []string{
		"BINANCE_SYMBOLS",
		"BINANCE_FUTURES_SYMBOLS",
		"COINBASE_SYMBOLS",
		"BITFINEX_SYMBOLS",
		"BITMEX_SYMBOLS",
		"HYPERLIQUID_SYMBOLS",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("BINANCE_SYMBOLS", "BTCUSDT=50000,ETHUSDT")

	clients, thresholds, err := exchangesFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != defaultExchangeConfigCount() {
		t.Fatalf("clients = %d, want %d", len(clients), defaultExchangeConfigCount())
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
	for _, name := range []string{
		"BINANCE_SYMBOLS",
		"BINANCE_FUTURES_SYMBOLS",
		"COINBASE_SYMBOLS",
		"BITFINEX_SYMBOLS",
		"BITMEX_SYMBOLS",
		"HYPERLIQUID_SYMBOLS",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("BITMEX_SYMBOLS", "XBTUSD,XBT_USDT=25000")
	t.Setenv("HYPERLIQUID_SYMBOLS", "BTC,PURR/USDC=25000")

	clients, thresholds, err := exchangesFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	wantClients := defaultExchangeConfigCount()
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

func TestRuntimeFlushTimeoutReadsDuration(t *testing.T) {
	t.Setenv("FLUSH_TIMEOUT", "3s")

	if got := runtimeFlushTimeout(); got != 3*time.Second {
		t.Fatalf("timeout = %s, want 3s", got)
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

func defaultExchangeConfigCount() int {
	var count int
	for _, config := range exchangeEnvConfigs {
		if len(config.defaults) != 0 {
			count++
		}
	}
	return count
}
