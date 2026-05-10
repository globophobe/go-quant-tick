package quanttick

import (
	"fmt"
	"time"
)

type SignificantTrade struct {
	Exchange               string    `json:"exchange"`
	UID                    string    `json:"uid"`
	Symbol                 string    `json:"symbol"`
	Timestamp              time.Time `json:"timestamp"`
	Nanoseconds            int       `json:"nanoseconds"`
	Price                  Decimal   `json:"price"`
	Volume                 *Decimal  `json:"volume"`
	Notional               *Decimal  `json:"notional"`
	TickRule               *int      `json:"tickRule"`
	Ticks                  *int      `json:"ticks"`
	IsSequential           bool      `json:"isSequential"`
	High                   Decimal   `json:"high"`
	Low                    Decimal   `json:"low"`
	TotalBuyVolume         Decimal   `json:"totalBuyVolume"`
	TotalVolume            Decimal   `json:"totalVolume"`
	TotalBuyNotional       Decimal   `json:"totalBuyNotional"`
	TotalNotional          Decimal   `json:"totalNotional"`
	TotalBuyTicks          int       `json:"totalBuyTicks"`
	TotalTicks             int       `json:"totalTicks"`
	SignificantTradeFilter Decimal   `json:"-"`
	IsLate                 bool      `json:"isLate,omitempty"`
}

func (t SignificantTrade) ExchangeSymbol() (string, string) {
	return t.Exchange, t.Symbol
}

func (t SignificantTrade) PubSubAttributes() map[string]string {
	return map[string]string{"significant_trade_filter": t.SignificantTradeFilter.String()}
}

type SignificantTradeAggregator struct {
	threshold      Decimal
	thresholds     map[string]Decimal
	windowDuration time.Duration
	trades         map[string][]TradeEvent
	windows        map[string]window
}

type window struct {
	start time.Time
	stop  time.Time
}

func NewSignificantTradeAggregator(threshold Decimal, windowDuration time.Duration) *SignificantTradeAggregator {
	return NewSignificantTradeAggregatorWithThresholds(threshold, nil, windowDuration)
}

func NewSignificantTradeAggregatorWithThresholds(
	threshold Decimal,
	thresholds map[string]Decimal,
	windowDuration time.Duration,
) *SignificantTradeAggregator {
	return &SignificantTradeAggregator{
		threshold:      threshold,
		thresholds:     cloneThresholds(thresholds),
		windowDuration: windowDuration,
		trades:         make(map[string][]TradeEvent),
		windows:        make(map[string]window),
	}
}

func (a *SignificantTradeAggregator) Add(trade TradeEvent) ([]SignificantTrade, error) {
	key := tradeKey(trade)
	if _, ok := a.trades[key]; !ok {
		a.trades[key] = nil
	}

	if a.windowDuration <= 0 {
		return a.getSignificantTradeOrTick(trade)
	}

	win, ok := a.windows[key]
	if !ok {
		win = a.setWindow(key, trade.Timestamp)
	}

	if trade.Timestamp.Before(win.start) {
		tick, err := a.aggregate([]TradeEvent{trade}, true)
		if err != nil {
			return nil, err
		}
		return []SignificantTrade{tick}, nil
	}

	if !trade.Timestamp.Before(win.stop) {
		ticks := make([]SignificantTrade, 0, 2)
		tick, ok, err := a.getTick(key)
		if err != nil {
			return nil, err
		}
		if ok {
			ticks = append(ticks, tick)
		}

		a.trades[key] = append(a.trades[key], trade)
		if a.isSignificant(trade) {
			tick, ok, err = a.getTick(key)
			if err != nil {
				return nil, err
			}
			if ok {
				ticks = append(ticks, tick)
			}
		}

		a.setWindow(key, trade.Timestamp)
		return ticks, nil
	}

	return a.getSignificantTradeOrTick(trade)
}

func (a *SignificantTradeAggregator) Flush(key string) ([]SignificantTrade, error) {
	tick, ok, err := a.getTick(key)
	if err != nil || !ok {
		return nil, err
	}
	return []SignificantTrade{tick}, nil
}

// DueKeys returns pending exchange-symbol aggregation keys with expired windows.
func (a *SignificantTradeAggregator) DueKeys(now time.Time) []string {
	if a.windowDuration <= 0 {
		return nil
	}

	keys := pendingKeys(a.trades)
	due := keys[:0]
	for _, key := range keys {
		win, ok := a.windows[key]
		if ok && !now.Before(win.stop) {
			due = append(due, key)
		}
	}
	return due
}

// DueSymbols returns due aggregation keys.
//
// Deprecated: use DueKeys.
func (a *SignificantTradeAggregator) DueSymbols(now time.Time) []string {
	return a.DueKeys(now)
}

func (a *SignificantTradeAggregator) getSignificantTradeOrTick(trade TradeEvent) ([]SignificantTrade, error) {
	key := tradeKey(trade)
	a.trades[key] = append(a.trades[key], trade)
	if !a.isSignificant(trade) {
		return nil, nil
	}

	tick, ok, err := a.getTick(key)
	if err != nil || !ok {
		return nil, err
	}
	return []SignificantTrade{tick}, nil
}

func (a *SignificantTradeAggregator) getTick(key string) (SignificantTrade, bool, error) {
	trades := a.trades[key]
	if len(trades) == 0 {
		return SignificantTrade{}, false, nil
	}
	tick, err := a.aggregate(trades, false)
	if err != nil {
		return SignificantTrade{}, false, err
	}
	a.trades[key] = nil
	return tick, true, nil
}

func (a *SignificantTradeAggregator) aggregate(trades []TradeEvent, isLate bool) (SignificantTrade, error) {
	if len(trades) == 0 {
		return SignificantTrade{}, fmt.Errorf("cannot aggregate empty trade set")
	}

	high := trades[0].Price
	low := trades[0].Price
	totalBuyVolume := Decimal{}
	totalVolume := Decimal{}
	totalBuyNotional := Decimal{}
	totalNotional := Decimal{}
	totalBuyTicks := 0
	totalTicks := 0
	allSequential := true
	var significant *TradeEvent

	for i := range trades {
		trade := trades[i]
		if trade.Price.GreaterThan(high) {
			high = trade.Price
		}
		if trade.Price.LessThan(low) {
			low = trade.Price
		}
		if trade.TickRule == 1 {
			totalBuyVolume = totalBuyVolume.Add(trade.Volume)
			totalBuyNotional = totalBuyNotional.Add(trade.Notional)
			totalBuyTicks += trade.Ticks
		}
		totalVolume = totalVolume.Add(trade.Volume)
		totalNotional = totalNotional.Add(trade.Notional)
		totalTicks += trade.Ticks
		allSequential = allSequential && trade.IsSequential

		if a.isSignificant(trade) {
			if significant != nil {
				return SignificantTrade{}, fmt.Errorf("more than one significant trade in aggregate for symbol %s", trade.Symbol)
			}
			significant = &trades[i]
		}
	}

	base := trades[len(trades)-1]
	filter := a.thresholdFor(base)
	result := SignificantTrade{
		Exchange:               base.Exchange,
		UID:                    base.UID,
		Symbol:                 base.Symbol,
		Timestamp:              base.Timestamp,
		Nanoseconds:            base.Nanoseconds,
		Price:                  base.Price,
		IsSequential:           allSequential,
		High:                   high,
		Low:                    low,
		TotalBuyVolume:         totalBuyVolume,
		TotalVolume:            totalVolume,
		TotalBuyNotional:       totalBuyNotional,
		TotalNotional:          totalNotional,
		TotalBuyTicks:          totalBuyTicks,
		TotalTicks:             totalTicks,
		SignificantTradeFilter: filter,
		IsLate:                 isLate,
	}

	if significant != nil {
		result.Exchange = significant.Exchange
		result.UID = significant.UID
		result.Symbol = significant.Symbol
		result.Timestamp = significant.Timestamp
		result.Nanoseconds = significant.Nanoseconds
		result.Price = significant.Price
		result.Volume = decimalPtr(significant.Volume)
		result.Notional = decimalPtr(significant.Notional)
		result.TickRule = intPtr(significant.TickRule)
		result.Ticks = intPtr(significant.Ticks)
		result.IsSequential = significant.IsSequential
	}

	return result, nil
}

func (a *SignificantTradeAggregator) setWindow(key string, timestamp time.Time) window {
	start := time.Date(
		timestamp.Year(),
		timestamp.Month(),
		timestamp.Day(),
		timestamp.Hour(),
		timestamp.Minute(),
		0,
		0,
		timestamp.Location(),
	)
	win := window{start: start, stop: start.Add(a.windowDuration)}
	a.windows[key] = win
	return win
}

func isSignificant(trade TradeEvent, threshold Decimal) bool {
	return trade.Volume.GreaterThanOrEqual(threshold)
}

func (a *SignificantTradeAggregator) isSignificant(trade TradeEvent) bool {
	return isSignificant(trade, a.thresholdFor(trade))
}

func (a *SignificantTradeAggregator) thresholdFor(trade TradeEvent) Decimal {
	if threshold, ok := a.thresholds[ExchangeSymbolKey(trade.Exchange, trade.Symbol)]; ok {
		return threshold
	}
	return a.threshold
}

func cloneThresholds(thresholds map[string]Decimal) map[string]Decimal {
	if len(thresholds) == 0 {
		return nil
	}
	clone := make(map[string]Decimal, len(thresholds))
	for key, threshold := range thresholds {
		clone[key] = threshold
	}
	return clone
}

func decimalPtr(value Decimal) *Decimal {
	return &value
}

func intPtr(value int) *int {
	return &value
}
