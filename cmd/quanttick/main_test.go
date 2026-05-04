package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func TestPublishStreamsDefaultsToSignificantTrades(t *testing.T) {
	t.Setenv("PUBLISH_STREAMS", "")

	streams, err := publishStreams()
	if err != nil {
		t.Fatal(err)
	}
	if !streams[quanttick.SignificantTrades] {
		t.Fatalf("expected significant trades stream to be enabled by default: %#v", streams)
	}
}

func TestPublishStreamsRejectsUnknownStream(t *testing.T) {
	t.Setenv("PUBLISH_STREAMS", "raw-trades,unknown")

	if _, err := publishStreams(); err == nil {
		t.Fatal("expected error for unknown stream")
	}
}

func TestPublisherModeDefaultsToPubSub(t *testing.T) {
	if got := publisherMode(""); got != "pubsub" {
		t.Fatalf("publisher mode = %s, want pubsub", got)
	}
}

func TestPublisherModeNormalizesValue(t *testing.T) {
	if got := publisherMode(" STDOUT "); got != "stdout" {
		t.Fatalf("publisher mode = %s, want stdout", got)
	}
}

func TestNewPipelineFromEnvUsesStdoutPublisher(t *testing.T) {
	t.Setenv("PUBLISH_STREAMS", "raw-trades")

	var output bytes.Buffer
	pipeline, cleanup, err := newPipelineFromEnv(context.Background(), &output, nil, "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	if pipeline.RawPublisher == nil {
		t.Fatal("raw publisher should be configured")
	}
}

func TestNewPipelineFromEnvDefaultsToPubSub(t *testing.T) {
	t.Setenv("PROJECT_ID", "")

	if _, _, err := newPipelineFromEnv(context.Background(), &bytes.Buffer{}, nil, ""); err == nil {
		t.Fatal("expected missing project id error")
	}
}

func TestNewPipelineFromEnvRejectsUnknownPublisher(t *testing.T) {
	if _, _, err := newPipelineFromEnv(context.Background(), &bytes.Buffer{}, nil, "unknown"); err == nil {
		t.Fatal("expected unknown publisher error")
	}
}

func TestExchangeSymbolsEnvParsesThresholdOverrides(t *testing.T) {
	t.Setenv("BINANCE_SYMBOLS", "BTCUSDT:50000,ETHUSDT")

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
		"COINBASE_SYMBOLS",
		"BITFINEX_SYMBOLS",
		"BITMEX_SYMBOLS",
		"HYPERLIQUID_SYMBOLS",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("BINANCE_SYMBOLS", "BTCUSDT:50000,ETHUSDT")

	clients, thresholds, err := exchangesFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != len(exchangeEnvConfigs) {
		t.Fatalf("clients = %d, want %d", len(clients), len(exchangeEnvConfigs))
	}

	threshold, ok := thresholds[quanttick.ExchangeSymbolKey(exchanges.BinanceName, "BTCUSDT")]
	if !ok {
		t.Fatal("expected BTCUSDT threshold")
	}
	if !threshold.Equal(quanttick.MustDecimal("50000")) {
		t.Fatalf("threshold = %s, want 50000", threshold)
	}
}

func TestExchangeSymbolsEnvRejectsInvalidThreshold(t *testing.T) {
	t.Setenv("BINANCE_SYMBOLS", "BTCUSDT:bad")

	if _, _, err := exchangeSymbolsEnv("BINANCE_SYMBOLS", exchanges.BinanceName, []string{"BTCUSDT"}); err == nil {
		t.Fatal("expected invalid threshold error")
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
