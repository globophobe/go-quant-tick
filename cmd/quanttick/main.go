package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/getsentry/sentry-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	publisher := flag.String("publisher", "", "publisher: db or stdout")
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

	pipeline, cleanup, bucketFlusher, err := newPipelineFromEnv(ctx, output, thresholds, publisher)
	if err != nil {
		return err
	}
	streams, err := websocketDataStreams()
	if err != nil {
		return err
	}
	handler := pipeline.Handle
	if eventFlusher, ok := bucketFlusher.(interface {
		FlushBefore(context.Context, string, string, time.Time) (int, error)
	}); ok {
		flushWatermarks := make(map[string]time.Time)
		handler = func(ctx context.Context, trade quanttick.TradeEvent) error {
			if err := pipeline.Handle(ctx, trade); err != nil {
				return err
			}
			flushTimestamp := updateFlushWatermark(flushWatermarks, trade)
			if err := pipeline.FlushBefore(ctx, trade.Exchange, trade.Symbol, flushTimestamp); err != nil {
				return err
			}
			_, err := eventFlusher.FlushBefore(ctx, trade.Exchange, trade.Symbol, flushTimestamp)
			return err
		}
	}

	var runErr error
	if hasTradeStreams(streams) && len(clients) > 0 {
		runErr = quanttick.RunExchanges(ctx, clients, handler, func(err error) {
			log.Printf("exchange error: %v", err)
			reporter.Capture(err)
		})
	} else {
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.Canceled) {
			runErr = ctx.Err()
		}
	}
	if flushErr := flushPipeline(pipeline); flushErr != nil {
		log.Printf("pipeline flush error: %v", flushErr)
		if runErr == nil {
			runErr = flushErr
		} else {
			reporter.Capture(flushErr)
		}
	}
	if bucketFlusher != nil {
		if flushErr := flushBuckets(bucketFlusher); flushErr != nil {
			log.Printf("bucket flush error: %v", flushErr)
			if runErr == nil {
				runErr = flushErr
			} else {
				reporter.Capture(flushErr)
			}
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

func updateFlushWatermark(watermarks map[string]time.Time, trade quanttick.TradeEvent) time.Time {
	key := quanttick.ExchangeSymbolKey(trade.Exchange, trade.Symbol)
	timestamp := trade.Timestamp.UTC()
	if watermark, ok := watermarks[key]; ok && watermark.After(timestamp) {
		return watermark
	}
	watermarks[key] = timestamp
	return timestamp
}

func hasTradeStreams(streams map[quanttick.Stream]bool) bool {
	return streams[quanttick.RawTrades] || streams[quanttick.AggregatedTrades] || streams[quanttick.SignificantTrades]
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
	newExchange func([]string) quanttick.Exchange
}

var exchangeEnvConfigs = []exchangeEnvConfig{
	{
		envName:     "BINANCE_SYMBOLS",
		exchange:    exchanges.BinanceName,
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBinance(symbols) },
	},
	{
		envName:     "BINANCE_FUTURES_SYMBOLS",
		exchange:    exchanges.BinanceFuturesName,
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBinanceFutures(symbols) },
	},
	{
		envName:     "COINBASE_SYMBOLS",
		exchange:    exchanges.CoinbaseName,
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewCoinbase(symbols) },
	},
	{
		envName:     "BITFINEX_SYMBOLS",
		exchange:    exchanges.BitfinexName,
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBitfinex(symbols) },
	},
	{
		envName:     "BITMEX_SYMBOLS",
		exchange:    exchanges.BitmexName,
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewBitmex(symbols) },
	},
	{
		envName:     "HYPERLIQUID_SYMBOLS",
		exchange:    exchanges.HyperliquidName,
		newExchange: func(symbols []string) quanttick.Exchange { return exchanges.NewHyperliquid(symbols) },
	},
}

func exchangesFromEnv() ([]quanttick.Exchange, map[string]quanttick.Decimal, error) {
	clients := make([]quanttick.Exchange, 0, len(exchangeEnvConfigs))
	thresholds := make(map[string]quanttick.Decimal)
	for _, config := range exchangeEnvConfigs {
		symbols, symbolThresholds, err := exchangeSymbolsEnv(config.envName, config.exchange)
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
) (*quanttick.TradePipeline, func() error, bucketFlusher, error) {
	streams, err := websocketDataStreams()
	if err != nil {
		return nil, nil, nil, err
	}

	threshold, err := significantThreshold()
	if err != nil {
		return nil, nil, nil, err
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
		return quanttick.NewTradePipeline(config), cleanup, nil, nil
	case "db":
		bucketBuffer, cleanup, err := configureDatabasePublishers(
			ctx,
			&config,
			streams,
			threshold,
			significantThresholds,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		return quanttick.NewTradePipeline(config), cleanup, bucketBuffer, nil
	default:
		return nil, nil, nil, fmt.Errorf("unknown publisher: %s", mode)
	}
}

type bucketFlusher interface {
	Flush(context.Context) (int, error)
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

func configureDatabasePublishers(
	ctx context.Context,
	config *quanttick.TradePipelineConfig,
	streams map[quanttick.Stream]bool,
	defaultThreshold quanttick.Decimal,
	significantThresholds map[string]quanttick.Decimal,
) (bucketFlusher, func() error, error) {
	db, cleanup, err := openDatabaseFunc(ctx)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (bucketFlusher, func() error, error) {
		_ = cleanup()
		return nil, nil, err
	}
	store := quanttick.NewWebSocketDataStore(db)
	buffer := quanttick.NewWebSocketDataBuffer(
		store,
		quanttick.WebSocketDataBufferConfig{
			DefaultSignificantTradeFilter: defaultThreshold,
			SignificantThresholds:         significantThresholds,
		},
	)
	if streams[quanttick.RawTrades] {
		config.RawPublisher = buffer.RawPublisher()
	}
	if streams[quanttick.AggregatedTrades] || streams[quanttick.SignificantTrades] {
		config.AggregatedPublisher = buffer.AggregatedPublisher()
	}
	if err := db.PingContext(ctx); err != nil {
		return fail(fmt.Errorf("ping database: %w", err))
	}
	return buffer, cleanup, nil
}

var openDatabaseFunc = openDatabase

func openDatabase(ctx context.Context) (*sql.DB, func() error, error) {
	instanceConnectionName := cloudSQLInstanceConnectionName()
	dbUser := strings.TrimSpace(os.Getenv("DATABASE_USER"))
	dbName := os.Getenv("DATABASE_NAME")
	if instanceConnectionName == "" {
		return nil, nil, fmt.Errorf("PRODUCTION_DATABASE_HOST is required when -publisher=db")
	}
	if dbUser == "" {
		return nil, nil, fmt.Errorf("DATABASE_USER is required when -publisher=db")
	}
	if dbName == "" {
		return nil, nil, fmt.Errorf("DATABASE_NAME is required when -publisher=db")
	}

	dialer, err := cloudsqlconn.NewDialer(
		ctx,
		cloudsqlconn.WithIAMAuthN(),
		cloudsqlconn.WithLazyRefresh(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create cloud sql dialer: %w", err)
	}
	pgxConfig, err := pgx.ParseConfig(fmt.Sprintf("user=%s dbname=%s sslmode=disable", dbUser, dbName))
	if err != nil {
		_ = dialer.Close()
		return nil, nil, fmt.Errorf("parse pgx config: %w", err)
	}
	pgxConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pgxConfig.DialFunc = func(ctx context.Context, network string, _addr string) (net.Conn, error) {
		return dialer.Dial(ctx, instanceConnectionName)
	}
	dbURI := stdlib.RegisterConnConfig(pgxConfig)
	db, err := sql.Open("pgx", dbURI)
	if err != nil {
		_ = dialer.Close()
		return nil, nil, fmt.Errorf("open cloud sql database: %w", err)
	}
	cleanup := func() error {
		return errors.Join(db.Close(), dialer.Close())
	}
	return db, cleanup, nil
}

func cloudSQLInstanceConnectionName() string {
	value := os.Getenv("PRODUCTION_DATABASE_HOST")
	return strings.TrimPrefix(strings.TrimSpace(value), "/cloudsql/")
}

func flushBuckets(flusher bucketFlusher) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout())
	defer cancel()
	_, err := flusher.Flush(ctx)
	return err
}

func publisherMode(value string) string {
	mode := strings.ToLower(strings.TrimSpace(value))
	if mode != "" {
		return mode
	}
	return "db"
}

func websocketDataStreams() (map[quanttick.Stream]bool, error) {
	values := csvEnvDefault(
		"WEBSOCKET_DATA_STREAMS",
		[]string{string(quanttick.SignificantTrades)},
	)
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

func csvEnv(name string) []string {
	value := os.Getenv(name)
	if value == "" {
		return nil
	}
	return parseCSV(value)
}

func csvEnvDefault(name string, defaults []string) []string {
	value := os.Getenv(name)
	if value == "" {
		return append([]string(nil), defaults...)
	}
	return parseCSV(value)
}

func parseCSV(value string) []string {
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
) ([]string, map[string]quanttick.Decimal, error) {
	config, err := quanttick.ParseSymbolThresholds(exchange, csvEnv(name))
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
