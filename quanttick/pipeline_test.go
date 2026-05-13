package quanttick

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
