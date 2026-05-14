package quanttick

import "fmt"

// TradeAggregator aggregates sequential trades that share exchange, symbol,
// timestamp, nanoseconds, and tick rule.
type TradeAggregator struct {
	trades map[string][]TradeEvent
}

func NewTradeAggregator() *TradeAggregator {
	return &TradeAggregator{trades: make(map[string][]TradeEvent)}
}

func (a *TradeAggregator) Add(trade TradeEvent) ([]TradeEvent, error) {
	key := tradeKey(trade)
	trades := a.trades[key]
	if len(trades) == 0 {
		a.trades[key] = []TradeEvent{trade}
		return nil, nil
	}

	last := trades[len(trades)-1]
	if sameSample(last, trade) {
		a.trades[key] = append(trades, trade)
		return nil, nil
	}

	aggregated, err := aggregateTrades(trades)
	if err != nil {
		return nil, err
	}
	a.trades[key] = []TradeEvent{trade}
	return []TradeEvent{aggregated}, nil
}

func (a *TradeAggregator) Flush(key string) ([]TradeEvent, error) {
	trades := a.trades[key]
	if len(trades) == 0 {
		return nil, nil
	}
	aggregated, err := aggregateTrades(trades)
	if err != nil {
		return nil, err
	}
	delete(a.trades, key)
	return []TradeEvent{aggregated}, nil
}

func sameSample(left TradeEvent, right TradeEvent) bool {
	return left.Exchange == right.Exchange &&
		left.Symbol == right.Symbol &&
		left.Timestamp.Equal(right.Timestamp) &&
		left.Nanoseconds == right.Nanoseconds &&
		left.TickRule == right.TickRule
}

func aggregateTrades(trades []TradeEvent) (TradeEvent, error) {
	if len(trades) == 0 {
		return TradeEvent{}, fmt.Errorf("cannot aggregate empty trade set")
	}

	first := trades[0]
	last := trades[len(trades)-1]

	if len(trades) == 1 {
		return last, nil
	}

	totalVolume := Decimal{}
	totalNotional := Decimal{}
	totalTicks := 0
	allSequential := true
	for _, trade := range trades {
		if !sameSample(first, trade) {
			return TradeEvent{}, fmt.Errorf("trade sample mismatch for %s", tradeKey(first))
		}
		totalVolume = totalVolume.Add(trade.Volume)
		totalNotional = totalNotional.Add(trade.Notional)
		totalTicks += trade.Ticks
		allSequential = allSequential && trade.IsSequential
	}

	last.UID = first.UID
	last.Volume = totalVolume
	last.Notional = totalNotional
	last.Ticks = totalTicks
	last.IsSequential = allSequential
	return last, nil
}

func tradeKey(trade TradeEvent) string {
	return ExchangeSymbolKey(trade.Exchange, trade.Symbol)
}
