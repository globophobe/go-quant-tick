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

type TradePipeline struct {
	RawPublisher         Publisher[TradeEvent]
	AggregatedPublisher  Publisher[TradeEvent]
	SignificantPublisher Publisher[SignificantTrade]

	mu                    sync.Mutex
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
	p.mu.Lock()
	defer p.mu.Unlock()

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
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.flushAggregatedTrades(ctx); err != nil {
		return err
	}
	return p.flushSignificantTrades(ctx)
}

func (p *TradePipeline) FlushDue(ctx context.Context, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.significantAggregator == nil || p.significantAggregator.windowDuration <= 0 {
		return nil
	}
	if err := p.flushAggregatedTrades(ctx); err != nil {
		return err
	}
	for _, key := range p.significantAggregator.DueKeys(now) {
		significantTrades, err := p.significantAggregator.Flush(key)
		if err != nil {
			return fmt.Errorf("flush due significant trades: %w", err)
		}
		if err := p.publishSignificantTrades(ctx, significantTrades); err != nil {
			return err
		}
	}
	return nil
}

func (p *TradePipeline) flushAggregatedTrades(ctx context.Context) error {
	if p.tradeAggregator != nil {
		for _, key := range pendingKeys(p.tradeAggregator.trades) {
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
		}
	}
	return nil
}

func (p *TradePipeline) flushSignificantTrades(ctx context.Context) error {
	if p.significantAggregator != nil {
		for _, key := range pendingKeys(p.significantAggregator.trades) {
			significantTrades, err := p.significantAggregator.Flush(key)
			if err != nil {
				return fmt.Errorf("flush significant trades: %w", err)
			}
			if err := p.publishSignificantTrades(ctx, significantTrades); err != nil {
				return err
			}
		}
	}

	return nil
}

type AlignedFlushConfig struct {
	Interval     time.Duration
	Timeout      time.Duration
	ErrorHandler ErrorHandler
}

func StartAlignedFlush(ctx context.Context, pipeline *TradePipeline, config AlignedFlushConfig) {
	if pipeline == nil {
		return
	}

	interval := config.Interval
	if interval <= 0 {
		return
	}
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	go func() {
		for {
			next := nextAlignedTime(time.Now(), interval)
			timer := time.NewTimer(time.Until(next))
			select {
			case <-timer.C:
				flushCtx, cancel := context.WithTimeout(context.Background(), timeout)
				err := pipeline.FlushDue(flushCtx, time.Now())
				cancel()
				if err != nil && config.ErrorHandler != nil {
					config.ErrorHandler(err)
				}
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
	}()
}

func nextAlignedTime(now time.Time, interval time.Duration) time.Time {
	return now.Truncate(interval).Add(interval)
}

func (p *TradePipeline) publishSignificantTrades(ctx context.Context, trades []SignificantTrade) error {
	for _, significantTrade := range trades {
		if err := p.SignificantPublisher.Publish(ctx, significantTrade); err != nil {
			return fmt.Errorf("publish significant trade: %w", err)
		}
	}
	return nil
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
			return fmt.Errorf("unknown publish stream: %s", stream)
		}
	}
	return nil
}

func RunExchanges(ctx context.Context, clients []Exchange, handler TradeHandler, errorHandler ErrorHandler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trades := make(chan TradeEvent, len(clients))
	errs := make(chan error, len(clients))

	var wg sync.WaitGroup
	for _, client := range clients {
		tradeC, errC := client.Trades(ctx)
		wg.Add(2)
		go forwardTrades(ctx, &wg, tradeC, trades)
		go forwardErrors(ctx, &wg, errC, errs)
	}

	go func() {
		wg.Wait()
		close(trades)
		close(errs)
	}()

	for trades != nil || errs != nil {
		select {
		case trade, ok := <-trades:
			if !ok {
				trades = nil
				continue
			}
			if err := handler(ctx, trade); err != nil {
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
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		}
	}

	return nil
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
