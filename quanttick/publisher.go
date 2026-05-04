package quanttick

import (
	"context"
	"encoding/json"
	"io"
	"sync"
)

type PublisherFunc[T any] func(context.Context, T) error

func (f PublisherFunc[T]) Publish(ctx context.Context, payload T) error {
	return f(ctx, payload)
}

type JSONLinesPublisher[T any] struct {
	writer io.Writer
	stream string
	mu     *sync.Mutex
}

type JSONLine[T any] struct {
	Stream  string `json:"stream,omitempty"`
	Payload T      `json:"payload"`
}

func NewJSONLinesPublisher[T any](writer io.Writer, stream string, mu *sync.Mutex) *JSONLinesPublisher[T] {
	if mu == nil {
		mu = &sync.Mutex{}
	}
	return &JSONLinesPublisher[T]{writer: writer, stream: stream, mu: mu}
}

func (p *JSONLinesPublisher[T]) Publish(ctx context.Context, payload T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	encoder := json.NewEncoder(p.writer)
	if p.stream == "" {
		return encoder.Encode(payload)
	}
	return encoder.Encode(JSONLine[T]{Stream: p.stream, Payload: payload})
}
