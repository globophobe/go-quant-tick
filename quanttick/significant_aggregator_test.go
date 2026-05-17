package quanttick

import (
	"testing"
	"time"
)

func TestSignificantTradeAggregatorEmitsContextTick(t *testing.T) {
	aggregator := NewSignificantTradeAggregator(MustDecimal("1000"), time.Minute)
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	out, err := aggregator.Add(testTrade("1", timestamp, withPrice("100"), withNotional("1")))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output for first trade, got %d", len(out))
	}

	out, err = aggregator.Add(testTrade(
		"2",
		timestamp.Add(time.Second),
		withPrice("101"),
		withNotional("2"),
		withTickRule(-1),
		withSequential(false),
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output before window boundary, got %d", len(out))
	}

	out, err = aggregator.Add(testTrade("3", timestamp.Add(time.Minute), withPrice("102"), withNotional("1")))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected one context tick, got %d", len(out))
	}

	payload := out[0]
	if payload.UID != "2" {
		t.Fatalf("uid = %s, want 2", payload.UID)
	}
	if !payload.Timestamp.Equal(timestamp.Add(time.Second)) {
		t.Fatalf("timestamp = %s, want %s", payload.Timestamp, timestamp.Add(time.Second))
	}
	assertDecimal(t, payload.Price, "101")
	if payload.Volume != nil {
		t.Fatalf("volume = %v, want nil", payload.Volume)
	}
	if payload.Notional != nil {
		t.Fatalf("notional = %v, want nil", payload.Notional)
	}
	if payload.TickRule != nil {
		t.Fatalf("tick rule = %v, want nil", payload.TickRule)
	}
	if payload.Ticks != nil {
		t.Fatalf("ticks = %v, want nil", payload.Ticks)
	}
	assertDecimal(t, payload.High, "101")
	assertDecimal(t, payload.Low, "100")
	assertDecimal(t, payload.TotalBuyVolume, "100")
	assertDecimal(t, payload.TotalVolume, "302")
	assertDecimal(t, payload.TotalBuyNotional, "1")
	assertDecimal(t, payload.TotalNotional, "3")
	if payload.TotalBuyTicks != 1 {
		t.Fatalf("total buy ticks = %d, want 1", payload.TotalBuyTicks)
	}
	if payload.TotalTicks != 2 {
		t.Fatalf("total ticks = %d, want 2", payload.TotalTicks)
	}
	if payload.IsSequential {
		t.Fatal("is sequential = true, want false")
	}
	assertDecimal(t, payload.SignificantTradeFilter, "1000")
}

func TestSignificantTradeAggregatorKeepsExchangeWindowsSeparate(t *testing.T) {
	aggregator := NewSignificantTradeAggregator(MustDecimal("1000"), time.Minute)
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	if out, err := aggregator.Add(testTrade("1", timestamp, withExchange("alpha"), withPrice("100"), withNotional("1"))); err != nil {
		t.Fatal(err)
	} else if len(out) != 0 {
		t.Fatalf("expected no output for first alpha trade, got %d", len(out))
	}

	if out, err := aggregator.Add(testTrade("2", timestamp.Add(time.Second), withExchange("beta"), withPrice("200"), withNotional("1"))); err != nil {
		t.Fatal(err)
	} else if len(out) != 0 {
		t.Fatalf("expected no output for first beta trade, got %d", len(out))
	}

	out, err := aggregator.Add(testTrade("3", timestamp.Add(time.Minute), withExchange("alpha"), withPrice("101"), withNotional("1")))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected alpha context tick, got %d", len(out))
	}
	if out[0].Exchange != "alpha" {
		t.Fatalf("exchange = %s, want alpha", out[0].Exchange)
	}
	if out[0].UID != "1" {
		t.Fatalf("uid = %s, want 1", out[0].UID)
	}
	assertDecimal(t, out[0].TotalVolume, "100")
}

func TestSignificantTradeAggregatorMarksSignificantBucketNonSequentialWhenContextHasGap(t *testing.T) {
	aggregator := NewSignificantTradeAggregator(MustDecimal("1000"), time.Minute)
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	out, err := aggregator.Add(testTrade("1", timestamp, withPrice("100"), withNotional("1"), withSequential(false)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output before significant trade, got %d", len(out))
	}

	out, err = aggregator.Add(testTrade("2", timestamp.Add(time.Second), withPrice("101"), withNotional("10")))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected one significant bucket, got %d", len(out))
	}
	if out[0].IsSequential {
		t.Fatal("is sequential = true, want false")
	}
}

func TestSignificantTradeAggregatorUsesSymbolThresholdOverride(t *testing.T) {
	aggregator := NewSignificantTradeAggregatorWithThresholds(
		MustDecimal("1000"),
		map[string]Decimal{
			ExchangeSymbolKey("test", "BTCUSD"): MustDecimal("50"),
		},
		0,
	)

	out, err := aggregator.Add(testTrade("1", time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected one significant trade, got %d", len(out))
	}
	if out[0].UID != "1" {
		t.Fatalf("uid = %s, want 1", out[0].UID)
	}
	assertDecimal(t, derefDecimal(t, out[0].Volume), "100")
	assertDecimal(t, out[0].SignificantTradeFilter, "50")
}
