package quanttick

import (
	"time"

	"github.com/shopspring/decimal"
)

// Decimal is the package-wide exact numeric type for prices and sizes.
type Decimal = decimal.Decimal

// TradeEvent is the normalized exchange trade payload shared by exchanges,
// aggregators, and publishers.
type TradeEvent struct {
	Exchange     string    `json:"exchange"`
	UID          string    `json:"uid"`
	Symbol       string    `json:"symbol"`
	Timestamp    time.Time `json:"timestamp"`
	Nanoseconds  int       `json:"nanoseconds"`
	Price        Decimal   `json:"price"`
	Volume       Decimal   `json:"volume"`
	Notional     Decimal   `json:"notional"`
	TickRule     int       `json:"tickRule"`
	Ticks        int       `json:"ticks"`
	IsSequential bool      `json:"isSequential"`
	ReceivedAt   time.Time `json:"-"`
}

func (t TradeEvent) ExchangeSymbol() (string, string) {
	return t.Exchange, t.Symbol
}

// NewTradeEvent builds a normalized trade and computes volume as price*notional.
func NewTradeEvent(input TradeEventInput) TradeEvent {
	ticks := input.Ticks
	if ticks == 0 {
		ticks = 1
	}
	volume := input.Price.Mul(input.Notional)
	if input.Volume != nil {
		volume = *input.Volume
	}

	return TradeEvent{
		Exchange:     input.Exchange,
		UID:          input.UID,
		Symbol:       input.Symbol,
		Timestamp:    input.Timestamp,
		ReceivedAt:   input.ReceivedAt,
		Price:        input.Price,
		Volume:       volume,
		Notional:     input.Notional,
		TickRule:     input.TickRule,
		Nanoseconds:  input.Nanoseconds,
		Ticks:        ticks,
		IsSequential: input.IsSequential,
	}
}

type TradeEventInput struct {
	Exchange     string
	UID          string
	Symbol       string
	Timestamp    time.Time
	ReceivedAt   time.Time
	Price        Decimal
	Volume       *Decimal
	Notional     Decimal
	TickRule     int
	Nanoseconds  int
	Ticks        int
	IsSequential bool
}

func MustDecimal(value string) Decimal {
	amount, err := ParseDecimal(value)
	if err != nil {
		panic(err)
	}
	return amount
}

func ParseDecimal(value string) (Decimal, error) {
	return decimal.NewFromString(value)
}
