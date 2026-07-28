package quanttick

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Stream string

const (
	RawTrades         Stream = "raw-trades"
	AggregatedTrades  Stream = "aggregated-trades"
	SignificantTrades Stream = "significant-trades"
)

var validStreams = map[Stream]struct{}{
	RawTrades:         {},
	AggregatedTrades:  {},
	SignificantTrades: {},
}

type TradeHandler func(context.Context, TradeEvent) error
type ErrorHandler func(error)

// TradePipeline derives and publishes trade streams. Calls must be serialized,
// and publishers must not call back into the pipeline.
type TradePipeline struct {
	RawPublisher         Publisher[TradeEvent]
	AggregatedPublisher  Publisher[TradeEvent]
	SignificantPublisher Publisher[SignificantTrade]

	tradeAggregator       *TradeAggregator
	significantAggregator *SignificantTradeAggregator
}

type TradePipelineConfig struct {
	RawPublisher          Publisher[TradeEvent]
	AggregatedPublisher   Publisher[TradeEvent]
	SignificantPublisher  Publisher[SignificantTrade]
	SignificantThreshold  Decimal
	SignificantThresholds map[string]Decimal
	WindowDuration        time.Duration
}

func NewTradePipeline(config TradePipelineConfig) *TradePipeline {
	pipeline := &TradePipeline{
		RawPublisher:         config.RawPublisher,
		AggregatedPublisher:  config.AggregatedPublisher,
		SignificantPublisher: config.SignificantPublisher,
	}

	if config.AggregatedPublisher != nil || config.SignificantPublisher != nil {
		pipeline.tradeAggregator = NewTradeAggregator()
	}
	if config.SignificantPublisher != nil {
		pipeline.significantAggregator = NewSignificantTradeAggregatorWithThresholds(
			config.SignificantThreshold,
			config.SignificantThresholds,
			config.WindowDuration,
		)
	}

	return pipeline
}

func (p *TradePipeline) Handle(ctx context.Context, trade TradeEvent) error {
	if p.RawPublisher != nil {
		if err := p.RawPublisher.Publish(ctx, trade); err != nil {
			return fmt.Errorf("publish raw trade: %w", err)
		}
	}

	if p.tradeAggregator == nil {
		return nil
	}

	aggregatedTrades, err := p.tradeAggregator.Add(trade)
	if err != nil {
		return fmt.Errorf("aggregate trade: %w", err)
	}
	for _, aggregatedTrade := range aggregatedTrades {
		if p.AggregatedPublisher != nil {
			if err := p.AggregatedPublisher.Publish(ctx, aggregatedTrade); err != nil {
				return fmt.Errorf("publish aggregated trade: %w", err)
			}
		}

		if p.significantAggregator != nil {
			significantTrades, err := p.significantAggregator.Add(aggregatedTrade)
			if err != nil {
				return fmt.Errorf("aggregate significant trade: %w", err)
			}
			for _, significantTrade := range significantTrades {
				if err := p.SignificantPublisher.Publish(ctx, significantTrade); err != nil {
					return fmt.Errorf("publish significant trade: %w", err)
				}
			}
		}
	}

	return nil
}

func (p *TradePipeline) Flush(ctx context.Context) error {
	if err := p.flushAggregatedTrades(ctx); err != nil {
		return err
	}
	return p.flushSignificantTrades(ctx)
}

func (p *TradePipeline) FlushBefore(ctx context.Context, exchange string, symbol string, timestamp time.Time) error {
	boundary := timestamp.UTC().Truncate(time.Minute)
	key := ExchangeSymbolKey(exchange, symbol)
	if err := p.flushAggregatedTradeKeyBefore(ctx, key, boundary); err != nil {
		return err
	}
	return p.flushSignificantTradeKeyBefore(ctx, key, boundary)
}

func (p *TradePipeline) flushAggregatedTrades(ctx context.Context) error {
	if p.tradeAggregator == nil {
		return nil
	}
	for _, key := range pendingKeys(p.tradeAggregator.trades) {
		if err := p.flushAggregatedTradeKey(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (p *TradePipeline) flushAggregatedTradeKeyBefore(ctx context.Context, key string, boundary time.Time) error {
	if p.tradeAggregator == nil || !hasPendingTradesBefore(p.tradeAggregator.trades, key, boundary) {
		return nil
	}
	return p.flushAggregatedTradeKey(ctx, key)
}

func (p *TradePipeline) flushAggregatedTradeKey(ctx context.Context, key string) error {
	aggregatedTrades, err := p.tradeAggregator.Flush(key)
	if err != nil {
		return fmt.Errorf("flush aggregated trades: %w", err)
	}
	for _, aggregatedTrade := range aggregatedTrades {
		if p.AggregatedPublisher != nil {
			if err := p.AggregatedPublisher.Publish(ctx, aggregatedTrade); err != nil {
				return fmt.Errorf("publish flushed aggregated trade: %w", err)
			}
		}
		if p.significantAggregator != nil {
			significantTrades, err := p.significantAggregator.Add(aggregatedTrade)
			if err != nil {
				return fmt.Errorf("aggregate flushed significant trade: %w", err)
			}
			if err := p.publishSignificantTrades(ctx, significantTrades); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *TradePipeline) flushSignificantTrades(ctx context.Context) error {
	if p.significantAggregator == nil {
		return nil
	}
	for _, key := range pendingKeys(p.significantAggregator.trades) {
		if err := p.flushSignificantTradeKey(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (p *TradePipeline) flushSignificantTradeKeyBefore(ctx context.Context, key string, boundary time.Time) error {
	if p.significantAggregator == nil || !hasPendingTradesBefore(p.significantAggregator.trades, key, boundary) {
		return nil
	}
	return p.flushSignificantTradeKey(ctx, key)
}

func (p *TradePipeline) flushSignificantTradeKey(ctx context.Context, key string) error {
	significantTrades, err := p.significantAggregator.Flush(key)
	if err != nil {
		return fmt.Errorf("flush significant trades: %w", err)
	}
	return p.publishSignificantTrades(ctx, significantTrades)
}

func (p *TradePipeline) publishSignificantTrades(ctx context.Context, trades []SignificantTrade) error {
	if len(trades) == 0 {
		return nil
	}
	if batchPublisher, ok := p.SignificantPublisher.(BatchPublisher[SignificantTrade]); ok {
		if err := batchPublisher.PublishBatch(ctx, trades); err != nil {
			return fmt.Errorf("publish significant trade bucket: %w", err)
		}
		return nil
	}
	for _, significantTrade := range trades {
		if err := p.SignificantPublisher.Publish(ctx, significantTrade); err != nil {
			return fmt.Errorf("publish significant trade: %w", err)
		}
	}
	return nil
}

func hasPendingTradesBefore(trades map[string][]TradeEvent, key string, boundary time.Time) bool {
	pendingTrades := trades[key]
	return len(pendingTrades) != 0 && pendingTrades[len(pendingTrades)-1].Timestamp.Before(boundary)
}

func pendingKeys(trades map[string][]TradeEvent) []string {
	keys := make([]string, 0, len(trades))
	for key, pendingTrades := range trades {
		if len(pendingTrades) != 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func ValidateStreams(streams []Stream) error {
	for _, stream := range streams {
		if _, ok := validStreams[stream]; !ok {
			return fmt.Errorf("unknown websocket data stream: %s", stream)
		}
	}
	return nil
}

func RunExchanges(ctx context.Context, clients []Exchange, handler TradeHandler, errorHandler ErrorHandler) error {
	adapterCtx, cancelAdapters := context.WithCancel(ctx)
	defer cancelAdapters()

	// Once a trade has entered the fan-in, finish handling it even if the
	// caller cancels. The adapter context still cancels immediately so no new
	// network work is admitted while the already-queued output drains.
	handlerCtx := context.WithoutCancel(ctx)
	forwardCtx, cancelForwarding := context.WithCancel(context.Background())
	defer cancelForwarding()

	trades := make(chan TradeEvent, len(clients))
	errs := make(chan error, len(clients))

	var wg sync.WaitGroup
	for _, client := range clients {
		tradeC, errC := client.Trades(adapterCtx)
		wg.Add(2)
		go forwardTrades(forwardCtx, &wg, tradeC, trades)
		go forwardErrors(forwardCtx, &wg, errC, errs)
	}

	go func() {
		wg.Wait()
		close(trades)
		close(errs)
	}()

	ctxDone := ctx.Done()
	var shutdownErr error
	for trades != nil || errs != nil {
		select {
		case trade, ok := <-trades:
			if !ok {
				trades = nil
				continue
			}
			if err := handler(handlerCtx, trade); err != nil {
				return err
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil && errorHandler != nil {
				errorHandler(err)
			}
		case <-ctxDone:
			shutdownErr = ctx.Err()
			cancelAdapters()
			ctxDone = nil
		}
	}

	if shutdownErr == nil {
		shutdownErr = ctx.Err()
	}
	if errors.Is(shutdownErr, context.Canceled) {
		return nil
	}
	return shutdownErr
}

func forwardTrades(ctx context.Context, wg *sync.WaitGroup, input <-chan TradeEvent, output chan<- TradeEvent) {
	defer wg.Done()
	for {
		select {
		case trade, ok := <-input:
			if !ok {
				return
			}
			select {
			case output <- trade:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func forwardErrors(ctx context.Context, wg *sync.WaitGroup, input <-chan error, output chan<- error) {
	defer wg.Done()
	for {
		select {
		case err, ok := <-input:
			if !ok {
				return
			}
			select {
			case output <- err:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
