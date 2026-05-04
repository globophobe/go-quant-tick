package example

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func Run(exchange quanttick.Exchange, thresholdOverrides ...map[string]quanttick.Decimal) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pipeline := quanttick.NewTradePipeline(quanttick.TradePipelineConfig{
		SignificantPublisher: quanttick.NewJSONLinesPublisher[quanttick.SignificantTrade](
			os.Stdout,
			"",
			nil,
		),
		SignificantThreshold:  significantThreshold(),
		SignificantThresholds: mergeThresholds(thresholdOverrides...),
		WindowDuration:        time.Minute,
	})
	alignedFlushCtx, stopAlignedFlush := context.WithCancel(ctx)
	quanttick.StartAlignedFlush(alignedFlushCtx, pipeline, quanttick.AlignedFlushConfig{
		Interval: time.Minute,
		Timeout:  10 * time.Second,
		ErrorHandler: func(err error) {
			log.Printf("%s flush error: %v", exchange.Name(), err)
		},
	})

	runErr := quanttick.RunExchanges(ctx, []quanttick.Exchange{exchange}, pipeline.Handle, func(err error) {
		log.Printf("%s error: %v", exchange.Name(), err)
	})
	stopAlignedFlush()

	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pipeline.Flush(flushCtx); err != nil {
		log.Fatal(err)
	}
	if runErr != nil {
		log.Fatal(runErr)
	}
}

func CSVEnv(name string, defaults []string) []string {
	config := symbolsEnv(name, "", defaults)
	return config.Symbols
}

func SymbolsEnv(name string, exchange string, defaults []string) quanttick.SymbolThresholds {
	return symbolsEnv(name, exchange, defaults)
}

func symbolsEnv(name string, exchange string, defaults []string) quanttick.SymbolThresholds {
	value := os.Getenv(name)
	if value == "" {
		config, err := quanttick.ParseSymbolThresholds(exchange, defaults)
		if err != nil {
			log.Fatal(fmt.Errorf("parse %s: %w", name, err))
		}
		return config
	}

	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	config, err := quanttick.ParseSymbolThresholds(exchange, result)
	if err != nil {
		log.Fatal(fmt.Errorf("parse %s: %w", name, err))
	}
	return config
}

func significantThreshold() quanttick.Decimal {
	value := os.Getenv("SIGNIFICANT_TRADE_FILTER")
	if value == "" {
		value = "1000"
	}
	threshold, err := quanttick.ParseDecimal(value)
	if err != nil {
		log.Fatal(fmt.Errorf("parse SIGNIFICANT_TRADE_FILTER: %w", err))
	}
	return threshold
}

func mergeThresholds(thresholds ...map[string]quanttick.Decimal) map[string]quanttick.Decimal {
	result := make(map[string]quanttick.Decimal)
	for _, source := range thresholds {
		for key, threshold := range source {
			result[key] = threshold
		}
	}
	return result
}
