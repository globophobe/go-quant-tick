package quanttick

import "context"

// Exchange streams normalized trade events from one market data source.
type Exchange interface {
	Name() string
	// Trades starts one stateful stream. Adapter instances support one active call.
	// The error channel contains best-effort diagnostics and may drop errors.
	Trades(ctx context.Context) (<-chan TradeEvent, <-chan error)
}

// Aggregator accepts one normalized trade and emits zero or more derived events.
type Aggregator[T any] interface {
	Add(trade TradeEvent) ([]T, error)
	Flush(key string) ([]T, error)
}

// Publisher writes one payload to an external sink.
type Publisher[T any] interface {
	Publish(ctx context.Context, payload T) error
}
