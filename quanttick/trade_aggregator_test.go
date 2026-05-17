package quanttick

import (
	"testing"
	"time"
)

func TestTradeAggregatorAggregatesTrades(t *testing.T) {
	aggregator := NewTradeAggregator()
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	out, err := aggregator.Add(testTrade("1", timestamp, withNanoseconds(1)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output for first trade, got %d", len(out))
	}

	out, err = aggregator.Add(testTrade("2", timestamp, withNanoseconds(1), withNotional("2")))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output for same sample, got %d", len(out))
	}

	out, err = aggregator.Add(testTrade("3", timestamp, withNanoseconds(2)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected one flushed aggregate, got %d", len(out))
	}

	flushed := out[0]
	if flushed.UID != "1" {
		t.Fatalf("uid = %s, want 1", flushed.UID)
	}
	if !flushed.Timestamp.Equal(timestamp) {
		t.Fatalf("timestamp = %s, want %s", flushed.Timestamp, timestamp)
	}
	if flushed.Nanoseconds != 1 {
		t.Fatalf("nanoseconds = %d, want 1", flushed.Nanoseconds)
	}
	if flushed.TickRule != 1 {
		t.Fatalf("tick rule = %d, want 1", flushed.TickRule)
	}
	if flushed.Ticks != 2 {
		t.Fatalf("ticks = %d, want 2", flushed.Ticks)
	}
	assertDecimal(t, flushed.Notional, "3")
	assertDecimal(t, flushed.Volume, "300")
}

func TestTradeAggregatorMarksAggregateNonSequentialWhenAnyTradeHasGap(t *testing.T) {
	aggregator := NewTradeAggregator()
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	_, err := aggregator.Add(testTrade("1", timestamp, withNanoseconds(1)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = aggregator.Add(testTrade("2", timestamp, withNanoseconds(1), withSequential(false)))
	if err != nil {
		t.Fatal(err)
	}
	out, err := aggregator.Add(testTrade("3", timestamp, withNanoseconds(2)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected one flushed aggregate, got %d", len(out))
	}
	if out[0].IsSequential {
		t.Fatal("is sequential = true, want false")
	}
}

func TestTradeAggregatorKeepsExchangesSeparate(t *testing.T) {
	aggregator := NewTradeAggregator()
	timestamp := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)

	out, err := aggregator.Add(testTrade("1", timestamp, withExchange("alpha"), withNanoseconds(1)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output for first alpha trade, got %d", len(out))
	}

	out, err = aggregator.Add(testTrade("2", timestamp, withExchange("beta"), withNanoseconds(1)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no output for first beta trade, got %d", len(out))
	}

	out, err = aggregator.Add(testTrade("3", timestamp, withExchange("alpha"), withNanoseconds(2)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected alpha aggregate, got %d", len(out))
	}
	if out[0].Exchange != "alpha" {
		t.Fatalf("exchange = %s, want alpha", out[0].Exchange)
	}
	if out[0].UID != "1" {
		t.Fatalf("uid = %s, want 1", out[0].UID)
	}

	flushed, err := aggregator.Flush(ExchangeSymbolKey("beta", "BTCUSD"))
	if err != nil {
		t.Fatal(err)
	}
	if len(flushed) != 1 {
		t.Fatalf("expected pending beta aggregate, got %d", len(flushed))
	}
	if flushed[0].Exchange != "beta" {
		t.Fatalf("exchange = %s, want beta", flushed[0].Exchange)
	}
	if flushed[0].UID != "2" {
		t.Fatalf("uid = %s, want 2", flushed[0].UID)
	}
}

func testTrade(uid string, timestamp time.Time, options ...func(*TradeEventInput)) TradeEvent {
	input := TradeEventInput{
		Exchange:     "test",
		UID:          uid,
		Symbol:       "BTCUSD",
		Timestamp:    timestamp,
		ReceivedAt:   timestamp,
		Price:        MustDecimal("100"),
		Notional:     MustDecimal("1"),
		TickRule:     1,
		Ticks:        1,
		IsSequential: true,
	}
	for _, option := range options {
		option(&input)
	}
	return NewTradeEvent(input)
}

func withExchange(exchange string) func(*TradeEventInput) {
	return func(input *TradeEventInput) {
		input.Exchange = exchange
	}
}

func withSymbol(symbol string) func(*TradeEventInput) {
	return func(input *TradeEventInput) {
		input.Symbol = symbol
	}
}

func withNanoseconds(nanoseconds int) func(*TradeEventInput) {
	return func(input *TradeEventInput) {
		input.Nanoseconds = nanoseconds
	}
}

func withNotional(notional string) func(*TradeEventInput) {
	return func(input *TradeEventInput) {
		input.Notional = MustDecimal(notional)
	}
}

func withPrice(price string) func(*TradeEventInput) {
	return func(input *TradeEventInput) {
		input.Price = MustDecimal(price)
	}
}

func withTickRule(tickRule int) func(*TradeEventInput) {
	return func(input *TradeEventInput) {
		input.TickRule = tickRule
	}
}

func withSequential(isSequential bool) func(*TradeEventInput) {
	return func(input *TradeEventInput) {
		input.IsSequential = isSequential
	}
}

func assertDecimal(t *testing.T, got Decimal, want string) {
	t.Helper()
	expected := MustDecimal(want)
	if !got.Equal(expected) {
		t.Fatalf("decimal = %s, want %s", got.String(), expected.String())
	}
}
