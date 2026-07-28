package quanttick

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestTradePipelinePublishesRawAggregatedAndSignificantTrades(t *testing.T) {
	raw := &memoryPublisher[TradeEvent]{}
	aggregated := &memoryPublisher[TradeEvent]{}
	significant := &memoryPublisher[SignificantTrade]{}
	pipeline := NewTradePipeline(TradePipelineConfig{
		RawPublisher:         raw,
		AggregatedPublisher:  aggregated,
		SignificantPublisher: significant,
		SignificantThreshold: MustDecimal("300"),
	})
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	trades := []TradeEvent{
		testTrade("1", timestamp, withNanoseconds(1), withPrice("100"), withNotional("1")),
		testTrade("2", timestamp, withNanoseconds(1), withPrice("101"), withNotional("2")),
		testTrade("3", timestamp, withNanoseconds(2), withPrice("102"), withNotional("1")),
	}
	for _, trade := range trades {
		if err := pipeline.Handle(context.Background(), trade); err != nil {
			t.Fatal(err)
		}
	}

	if len(raw.payloads) != 3 {
		t.Fatalf("raw payloads = %d, want 3", len(raw.payloads))
	}
	if len(aggregated.payloads) != 1 {
		t.Fatalf("aggregated payloads = %d, want 1", len(aggregated.payloads))
	}
	if len(significant.payloads) != 1 {
		t.Fatalf("significant payloads = %d, want 1", len(significant.payloads))
	}

	aggregatedTrade := aggregated.payloads[0]
	if aggregatedTrade.UID != "1" {
		t.Fatalf("aggregated uid = %s, want 1", aggregatedTrade.UID)
	}
	assertDecimal(t, aggregatedTrade.Price, "101")
	assertDecimal(t, aggregatedTrade.Notional, "3")
	assertDecimal(t, aggregatedTrade.Volume, "302")

	significantTrade := significant.payloads[0]
	if significantTrade.UID != "1" {
		t.Fatalf("significant uid = %s, want 1", significantTrade.UID)
	}
	assertDecimal(t, derefDecimal(t, significantTrade.Volume), "302")
	assertDecimal(t, significantTrade.TotalVolume, "302")
	assertDecimal(t, significantTrade.SignificantTradeFilter, "300")
}

func TestTradePipelineUsesSymbolSignificantThresholdOverride(t *testing.T) {
	significant := &memoryPublisher[SignificantTrade]{}
	pipeline := NewTradePipeline(TradePipelineConfig{
		SignificantPublisher: significant,
		SignificantThreshold: MustDecimal("1000"),
		SignificantThresholds: map[string]Decimal{
			ExchangeSymbolKey("test", "BTCUSD"): MustDecimal("50"),
		},
	})
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	if err := pipeline.Handle(context.Background(), testTrade("1", timestamp, withNanoseconds(1))); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Handle(context.Background(), testTrade("2", timestamp, withNanoseconds(2))); err != nil {
		t.Fatal(err)
	}

	if len(significant.payloads) != 1 {
		t.Fatalf("significant payloads = %d, want 1", len(significant.payloads))
	}
	if significant.payloads[0].UID != "1" {
		t.Fatalf("significant uid = %s, want 1", significant.payloads[0].UID)
	}
	assertDecimal(t, derefDecimal(t, significant.payloads[0].Volume), "100")
	assertDecimal(t, significant.payloads[0].SignificantTradeFilter, "50")
}

func TestTradePipelineSkipsAggregationWhenOnlyRawPublisherIsConfigured(t *testing.T) {
	raw := &memoryPublisher[TradeEvent]{}
	pipeline := NewTradePipeline(TradePipelineConfig{RawPublisher: raw})

	if err := pipeline.Handle(context.Background(), testTrade("1", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if len(raw.payloads) != 1 {
		t.Fatalf("raw payloads = %d, want 1", len(raw.payloads))
	}
	if pipeline.tradeAggregator != nil {
		t.Fatal("trade aggregator should not be configured for raw-only pipeline")
	}
}

func TestTradePipelineFlushPublishesPendingAggregatedAndSignificantTrades(t *testing.T) {
	aggregated := &memoryPublisher[TradeEvent]{}
	significant := &memoryPublisher[SignificantTrade]{}
	pipeline := NewTradePipeline(TradePipelineConfig{
		AggregatedPublisher:  aggregated,
		SignificantPublisher: significant,
		SignificantThreshold: MustDecimal("50"),
	})

	if err := pipeline.Handle(
		context.Background(),
		testTrade("1", time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC), withPrice("100"), withNotional("1")),
	); err != nil {
		t.Fatal(err)
	}
	if len(aggregated.payloads) != 0 {
		t.Fatalf("aggregated payloads before flush = %d, want 0", len(aggregated.payloads))
	}

	if err := pipeline.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(aggregated.payloads) != 1 {
		t.Fatalf("aggregated payloads after flush = %d, want 1", len(aggregated.payloads))
	}
	if len(significant.payloads) != 1 {
		t.Fatalf("significant payloads after flush = %d, want 1", len(significant.payloads))
	}
	if aggregated.payloads[0].UID != "1" {
		t.Fatalf("aggregated uid = %s, want 1", aggregated.payloads[0].UID)
	}
	if significant.payloads[0].UID != "1" {
		t.Fatalf("significant uid = %s, want 1", significant.payloads[0].UID)
	}
}

func TestTradePipelineFlushPublishesPendingSignificantContextTick(t *testing.T) {
	significant := &memoryPublisher[SignificantTrade]{}
	pipeline := NewTradePipeline(TradePipelineConfig{
		SignificantPublisher: significant,
		SignificantThreshold: MustDecimal("1000"),
	})

	if err := pipeline.Handle(
		context.Background(),
		testTrade("1", time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC), withPrice("100"), withNotional("1")),
	); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(significant.payloads) != 1 {
		t.Fatalf("significant payloads after flush = %d, want 1", len(significant.payloads))
	}
	if significant.payloads[0].Volume != nil {
		t.Fatalf("significant context tick volume = %v, want nil", significant.payloads[0].Volume)
	}
	assertDecimal(t, significant.payloads[0].TotalVolume, "100")
}

func TestTradePipelineFlushBeforePublishesOldSignificantContextTick(t *testing.T) {
	significant := &memoryPublisher[SignificantTrade]{}
	pipeline := NewTradePipeline(TradePipelineConfig{
		SignificantPublisher: significant,
		SignificantThreshold: MustDecimal("1000"),
		WindowDuration:       time.Minute,
	})
	timestamp := time.Date(2026, 4, 8, 0, 32, 0, 0, time.UTC)

	if err := pipeline.Handle(
		context.Background(),
		testTrade("large", timestamp.Add(10*time.Second), withPrice("100"), withNotional("20")),
	); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Handle(
		context.Background(),
		testTrade("tail", timestamp.Add(57*time.Second), withPrice("101"), withNotional("1")),
	); err != nil {
		t.Fatal(err)
	}
	if len(significant.payloads) != 1 {
		t.Fatalf("significant payloads before flush-before = %d, want 1", len(significant.payloads))
	}

	if err := pipeline.FlushBefore(context.Background(), "test", "BTCUSD", timestamp.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	if len(significant.payloads) != 2 {
		t.Fatalf("significant payloads after flush-before = %d, want 2", len(significant.payloads))
	}
	contextTick := significant.payloads[1]
	if contextTick.UID != "tail" {
		t.Fatalf("context uid = %s, want tail", contextTick.UID)
	}
	if contextTick.Volume != nil {
		t.Fatalf("context volume = %v, want nil", contextTick.Volume)
	}
	assertDecimal(t, contextTick.TotalVolume, "101")
}

func TestTradePipelineFlushBeforeKeepsCurrentMinutePending(t *testing.T) {
	significant := &memoryPublisher[SignificantTrade]{}
	pipeline := NewTradePipeline(TradePipelineConfig{
		SignificantPublisher: significant,
		SignificantThreshold: MustDecimal("1000"),
		WindowDuration:       time.Minute,
	})
	timestamp := time.Date(2026, 4, 8, 0, 32, 10, 0, time.UTC)

	if err := pipeline.Handle(
		context.Background(),
		testTrade("tail", timestamp, withPrice("100"), withNotional("1")),
	); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.FlushBefore(context.Background(), "test", "BTCUSD", timestamp.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}

	if len(significant.payloads) != 0 {
		t.Fatalf("significant payloads after current-minute flush-before = %d, want 0", len(significant.payloads))
	}
}

func TestJSONLinesPublisherWritesEnvelope(t *testing.T) {
	var output bytes.Buffer
	publisher := NewJSONLinesPublisher[TradeEvent](&output, string(RawTrades), nil)

	if err := publisher.Publish(context.Background(), testTrade("1", time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC))); err != nil {
		t.Fatal(err)
	}

	var line JSONLine[TradeEvent]
	if err := json.Unmarshal(output.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	if line.Stream != string(RawTrades) {
		t.Fatalf("stream = %s, want %s", line.Stream, RawTrades)
	}
	if line.Payload.UID != "1" {
		t.Fatalf("uid = %s, want 1", line.Payload.UID)
	}
}

func TestValidateStreamsRejectsUnknownStream(t *testing.T) {
	err := ValidateStreams([]Stream{RawTrades, "unknown"})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestRunExchangesForwardsTradesAndErrors(t *testing.T) {
	tradeInput := make(chan TradeEvent, 1)
	errorInput := make(chan error, 1)
	wantTrade := testTrade("forwarded", time.Date(2026, 4, 8, 1, 2, 3, 0, time.UTC))
	wantError := errors.New("temporary exchange error")
	tradeInput <- wantTrade
	errorInput <- wantError
	close(tradeInput)
	close(errorInput)

	var handled []TradeEvent
	var reported []error
	err := RunExchanges(
		context.Background(),
		[]Exchange{testChannelExchange{trades: tradeInput, errs: errorInput}},
		func(_ context.Context, trade TradeEvent) error {
			handled = append(handled, trade)
			return nil
		},
		func(err error) {
			reported = append(reported, err)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(handled) != 1 || handled[0].UID != wantTrade.UID {
		t.Fatalf("handled trades = %#v, want UID %q", handled, wantTrade.UID)
	}
	if len(reported) != 1 || !errors.Is(reported[0], wantError) {
		t.Fatalf("reported errors = %v, want %v", reported, wantError)
	}
}

func TestRunExchangesReturnsHandlerError(t *testing.T) {
	tradeInput := make(chan TradeEvent, 1)
	errorInput := make(chan error)
	tradeInput <- testTrade("rejected", time.Date(2026, 4, 8, 1, 2, 3, 0, time.UTC))
	close(tradeInput)
	close(errorInput)
	want := errors.New("handler failed")

	err := RunExchanges(
		context.Background(),
		[]Exchange{testChannelExchange{trades: tradeInput, errs: errorInput}},
		func(context.Context, TradeEvent) error { return want },
		nil,
	)
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestRunExchangesTreatsCancellationAsCleanShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	trades := make(chan TradeEvent)
	errs := make(chan error)
	close(trades)
	close(errs)

	err := RunExchanges(
		ctx,
		[]Exchange{testChannelExchange{trades: trades, errs: errs}},
		func(context.Context, TradeEvent) error {
			t.Fatal("handler called without a queued trade")
			return nil
		},
		func(error) { t.Fatal("error handler called without a queued error") },
	)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestRunExchangesDrainsBufferedTradesAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	trades := make(chan TradeEvent, 2)
	errs := make(chan error, 1)
	trades <- testTrade("first", time.Date(2026, 4, 8, 1, 2, 3, 0, time.UTC))
	trades <- testTrade("second", time.Date(2026, 4, 8, 1, 2, 4, 0, time.UTC))
	errs <- errors.New("queued exchange error")
	close(trades)
	close(errs)
	cancel()

	var handled []string
	var reported int
	err := RunExchanges(
		ctx,
		[]Exchange{testChannelExchange{trades: trades, errs: errs}},
		func(handlerCtx context.Context, trade TradeEvent) error {
			if handlerCtx.Err() != nil {
				t.Fatalf("drain handler context = %v, want active", handlerCtx.Err())
			}
			handled = append(handled, trade.UID)
			return nil
		},
		func(error) { reported++ },
	)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !reflect.DeepEqual(handled, []string{"first", "second"}) {
		t.Fatalf("handled trades = %#v", handled)
	}
	if reported != 1 {
		t.Fatalf("reported errors = %d, want 1", reported)
	}
}

func TestRunExchangesDrainsBufferedTradesBeforeReturningDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	trades := make(chan TradeEvent, 1)
	errs := make(chan error)
	trades <- testTrade("deadline", time.Date(2026, 4, 8, 1, 2, 3, 0, time.UTC))
	close(trades)
	close(errs)

	var handled []string
	err := RunExchanges(
		ctx,
		[]Exchange{testChannelExchange{trades: trades, errs: errs}},
		func(handlerCtx context.Context, trade TradeEvent) error {
			if handlerCtx.Err() != nil {
				t.Fatalf("drain handler context = %v, want active", handlerCtx.Err())
			}
			handled = append(handled, trade.UID)
			return nil
		},
		nil,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if !reflect.DeepEqual(handled, []string{"deadline"}) {
		t.Fatalf("handled trades = %#v", handled)
	}
}

type testChannelExchange struct {
	trades <-chan TradeEvent
	errs   <-chan error
}

func (testChannelExchange) Name() string { return "test" }

func (e testChannelExchange) Trades(context.Context) (<-chan TradeEvent, <-chan error) {
	return e.trades, e.errs
}

type memoryPublisher[T any] struct {
	err      error
	payloads []T
}

func (p *memoryPublisher[T]) Publish(ctx context.Context, payload T) error {
	if p.err != nil {
		return p.err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.payloads = append(p.payloads, payload)
	return nil
}

func TestTradePipelineReturnsPublisherErrors(t *testing.T) {
	want := errors.New("publish failed")
	pipeline := NewTradePipeline(TradePipelineConfig{
		RawPublisher: &memoryPublisher[TradeEvent]{err: want},
	})

	err := pipeline.Handle(context.Background(), testTrade("1", time.Now().UTC()))
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func derefDecimal(t *testing.T, value *Decimal) Decimal {
	t.Helper()
	if value == nil {
		t.Fatal("decimal pointer is nil")
	}
	return *value
}
