package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	publisher := flag.String("publisher", "pubsub", "publisher: pubsub or stdout")
	flag.Parse()

	reporter, err := newErrorReporterFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Stdout, *publisher, reporter); err != nil {
		reporter.Capture(err)
		reporter.Flush(sentryFlushTimeout)
		log.Fatal(err)
	}
	reporter.Flush(sentryFlushTimeout)
}

func run(ctx context.Context, output io.Writer, publisher string, reporter errorReporter) error {
	clients, thresholds, err := exchangesFromEnv()
	if err != nil {
		return err
	}

	pipeline, cleanup, err := newPipelineFromEnv(ctx, output, thresholds, publisher)
	if err != nil {
		return err
	}
	alignedFlushCtx, stopAlignedFlush := context.WithCancel(ctx)
	quanttick.StartAlignedFlush(alignedFlushCtx, pipeline, quanttick.AlignedFlushConfig{
		Interval: time.Minute,
		Timeout:  runtimeFlushTimeout(),
		ErrorHandler: func(err error) {
			log.Printf("pipeline timer flush error: %v", err)
			reporter.Capture(err)
		},
	})

	runErr := quanttick.RunExchanges(ctx, clients, pipeline.Handle, func(err error) {
		log.Printf("exchange error: %v", err)
		reporter.Capture(err)
	})
	stopAlignedFlush()
	if flushErr := flushPipeline(pipeline); flushErr != nil {
		log.Printf("pipeline flush error: %v", flushErr)
		if runErr == nil {
			runErr = flushErr
		} else {
			reporter.Capture(flushErr)
		}
	}
	if cleanupErr := cleanup(); cleanupErr != nil {
		log.Printf("publisher cleanup error: %v", cleanupErr)
		if runErr == nil {
			runErr = cleanupErr
		} else {
			reporter.Capture(cleanupErr)
		}
	}
	if runErr != nil {
		return runErr
	}
	return nil
}

const sentryFlushTimeout = 2 * time.Second

type errorReporter interface {
	Capture(error)
	Flush(time.Duration)
}

type noopErrorReporter struct{}

func (noopErrorReporter) Capture(error) {}

func (noopErrorReporter) Flush(time.Duration) {}

type sentryErrorReporter struct{}

func (sentryErrorReporter) Capture(err error) {
	if err != nil {
		sentry.CaptureException(err)
	}
}

func (sentryErrorReporter) Flush(timeout time.Duration) {
	sentry.Flush(timeout)
}

func newErrorReporterFromEnv() (errorReporter, error) {
	dsn := strings.TrimSpace(os.Getenv("SENTRY_DSN"))
	if dsn == "" {
		return noopErrorReporter{}, nil
	}
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		AttachStacktrace: true,
	}); err != nil {
		return nil, fmt.Errorf("init sentry: %w", err)
	}
	return sentryErrorReporter{}, nil
}

type exchangeEnvConfig struct {
	envName     string
	exchange    string
	defaults    []string
	newExchange func([]string) quanttick.Exchange
}

var exchangeEnvConfigs = []exchangeEnvConfig{
	{
		envName:     "BINANCE_SYMBOLS",
		exchange:    exchanges.BinanceName,
		defaults:    []string{"BTCUSDT"},
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBinance(symbols) },
	},
	{
		envName:     "BINANCE_FUTURES_SYMBOLS",
		exchange:    exchanges.BinanceFuturesName,
		defaults:    []string{"BTCUSDT"},
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBinanceFutures(symbols) },
	},
	{
		envName:     "COINBASE_SYMBOLS",
		exchange:    exchanges.CoinbaseName,
		defaults:    []string{"BTC-USD"},
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewCoinbase(symbols) },
	},
	{
		envName:     "BITFINEX_SYMBOLS",
		exchange:    exchanges.BitfinexName,
		defaults:    []string{"tBTCF0:USTF0"},
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBitfinex(symbols) },
	},
	{
		envName:     "BITMEX_SYMBOLS",
		exchange:    exchanges.BitmexName,
		defaults:    []string{"XBTUSD"},
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBitmex(symbols) },
	},
	{
		envName:     "HYPERLIQUID_SYMBOLS",
		exchange:    exchanges.HyperliquidName,
		defaults:    []string{"BTC"},
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewHyperliquid(symbols) },
	},
}

func exchangesFromEnv() ([]quanttick.Exchange, map[string]quanttick.Decimal, error) {
	clients := make([]quanttick.Exchange, 0, len(exchangeEnvConfigs))
	thresholds := make(map[string]quanttick.Decimal)
	for _, config := range exchangeEnvConfigs {
		symbols, symbolThresholds, err := exchangeSymbolsEnv(config.envName, config.exchange, config.defaults)
		if err != nil {
			return nil, nil, err
		}
		if len(symbols) == 0 {
			continue
		}
		clients = append(clients, config.newExchange(symbols))
		mergeThresholds(thresholds, symbolThresholds)
	}
	return clients, thresholds, nil
}

func newPipelineFromEnv(
	ctx context.Context,
	output io.Writer,
	significantThresholds map[string]quanttick.Decimal,
	publisher string,
) (*quanttick.TradePipeline, func() error, error) {
	streams, err := publishStreams()
	if err != nil {
		return nil, nil, err
	}

	threshold, err := significantThreshold()
	if err != nil {
		return nil, nil, err
	}

	config := quanttick.TradePipelineConfig{
		SignificantThreshold:  threshold,
		SignificantThresholds: significantThresholds,
		WindowDuration:        time.Minute,
	}

	cleanup := func() error { return nil }
	mode := publisherMode(publisher)
	switch mode {
	case "stdout":
		configureStdoutPublishers(&config, streams, output)
	case "pubsub":
		var err error
		cleanup, err = configurePubSubPublishers(ctx, &config, streams)
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fmt.Errorf("unknown publisher: %s", mode)
	}

	return quanttick.NewTradePipeline(config), cleanup, nil
}

func configureStdoutPublishers(config *quanttick.TradePipelineConfig, streams map[quanttick.Stream]bool, output io.Writer) {
	var outputMu sync.Mutex
	if streams[quanttick.RawTrades] {
		config.RawPublisher = quanttick.NewJSONLinesPublisher[quanttick.TradeEvent](output, string(quanttick.RawTrades), &outputMu)
	}
	if streams[quanttick.AggregatedTrades] {
		config.AggregatedPublisher = quanttick.NewJSONLinesPublisher[quanttick.TradeEvent](output, string(quanttick.AggregatedTrades), &outputMu)
	}
	if streams[quanttick.SignificantTrades] {
		config.SignificantPublisher = quanttick.NewJSONLinesPublisher[quanttick.SignificantTrade](output, string(quanttick.SignificantTrades), &outputMu)
	}
}

func configurePubSubPublishers(
	ctx context.Context,
	config *quanttick.TradePipelineConfig,
	streams map[quanttick.Stream]bool,
) (func() error, error) {
	projectID := os.Getenv("PROJECT_ID")
	if projectID == "" {
		return nil, fmt.Errorf("PROJECT_ID is required when -publisher=pubsub")
	}

	publisherConfig, err := pubSubPublisherConfigFromEnv()
	if err != nil {
		return nil, err
	}

	var cleanups []func() error
	cleanupAll := func() error {
		var cleanupErr error
		for _, cleanup := range cleanups {
			if err := cleanup(); err != nil && cleanupErr == nil {
				cleanupErr = err
			}
		}
		return cleanupErr
	}
	fail := func(err error) (func() error, error) {
		_ = cleanupAll()
		return nil, err
	}

	if streams[quanttick.RawTrades] {
		publisher, cleanup, err := quanttick.NewCloudPubSubPublisher[quanttick.TradeEvent](
			ctx,
			projectID,
			envDefault("RAW_TRADES_TOPIC", string(quanttick.RawTrades)),
			publisherConfig,
		)
		if err != nil {
			return fail(err)
		}
		config.RawPublisher = publisher
		cleanups = append(cleanups, cleanup)
	}
	if streams[quanttick.AggregatedTrades] {
		publisher, cleanup, err := quanttick.NewCloudPubSubPublisher[quanttick.TradeEvent](
			ctx,
			projectID,
			envDefault("AGGREGATED_TRADES_TOPIC", string(quanttick.AggregatedTrades)),
			publisherConfig,
		)
		if err != nil {
			return fail(err)
		}
		config.AggregatedPublisher = publisher
		cleanups = append(cleanups, cleanup)
	}
	if streams[quanttick.SignificantTrades] {
		publisher, cleanup, err := quanttick.NewCloudPubSubPublisher[quanttick.SignificantTrade](
			ctx,
			projectID,
			envDefault("SIGNIFICANT_TRADES_TOPIC", string(quanttick.SignificantTrades)),
			publisherConfig,
		)
		if err != nil {
			return fail(err)
		}
		config.SignificantPublisher = publisher
		cleanups = append(cleanups, cleanup)
	}

	return cleanupAll, nil
}

func publisherMode(value string) string {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode == "" {
		return "pubsub"
	}
	return mode
}

func publishStreams() (map[quanttick.Stream]bool, error) {
	values := csvEnv("PUBLISH_STREAMS", []string{string(quanttick.SignificantTrades)})
	streams := make([]quanttick.Stream, 0, len(values))
	selected := make(map[quanttick.Stream]bool, len(values))
	for _, value := range values {
		stream := quanttick.Stream(value)
		streams = append(streams, stream)
		selected[stream] = true
	}
	if err := quanttick.ValidateStreams(streams); err != nil {
		return nil, err
	}
	return selected, nil
}

func significantThreshold() (quanttick.Decimal, error) {
	value := envDefault("SIGNIFICANT_TRADE_FILTER", "1000")
	threshold, err := quanttick.ParseDecimal(value)
	if err != nil {
		return quanttick.Decimal{}, fmt.Errorf("parse SIGNIFICANT_TRADE_FILTER: %w", err)
	}
	return threshold, nil
}

func csvEnv(name string, defaults []string) []string {
	value := os.Getenv(name)
	if value == "" {
		return append([]string(nil), defaults...)
	}

	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func exchangeSymbolsEnv(
	name string,
	exchange string,
	defaults []string,
) ([]string, map[string]quanttick.Decimal, error) {
	config, err := quanttick.ParseSymbolThresholds(exchange, csvEnv(name, defaults))
	if err != nil {
		return nil, nil, fmt.Errorf("parse %s: %w", name, err)
	}
	return config.Symbols, config.Thresholds, nil
}

func mergeThresholds(dst map[string]quanttick.Decimal, src map[string]quanttick.Decimal) {
	for key, threshold := range src {
		dst[key] = threshold
	}
}

func envDefault(name string, defaultValue string) string {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	return value
}

func flushPipeline(pipeline *quanttick.TradePipeline) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout())
	defer cancel()
	return pipeline.Flush(ctx)
}

func pubSubPublisherConfigFromEnv() (quanttick.PubSubPublisherConfig, error) {
	timeout, err := durationEnv("PUBLISH_TIMEOUT", time.Second)
	if err != nil {
		return quanttick.PubSubPublisherConfig{}, err
	}
	return quanttick.PubSubPublisherConfig{Timeout: timeout}, nil
}

func runtimeFlushTimeout() time.Duration {
	return durationEnvDefault("FLUSH_TIMEOUT", 5*time.Second)
}

func shutdownFlushTimeout() time.Duration {
	return durationEnvDefault("SHUTDOWN_FLUSH_TIMEOUT", 10*time.Second)
}

func durationEnvDefault(name string, defaultValue time.Duration) time.Duration {
	duration, err := durationEnv(name, defaultValue)
	if err != nil {
		return defaultValue
	}
	return duration
}

func durationEnv(name string, defaultValue time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("parse %s: %s", name, value)
	}
	return duration, nil
}
