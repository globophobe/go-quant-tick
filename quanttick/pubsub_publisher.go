package quanttick

import (
	"context"
	"encoding/json"
	"fmt"

	gcppubsub "cloud.google.com/go/pubsub"
)

type ExchangeSymbolPayload interface {
	ExchangeSymbol() (exchange string, symbol string)
}

type PubSubPublisher[T ExchangeSymbolPayload] struct {
	topic pubSubTopic
}

type pubSubTopic interface {
	Publish(context.Context, *gcppubsub.Message) pubSubPublishResult
}

type pubSubPublishResult interface {
	Get(context.Context) (string, error)
}

type cloudPubSubTopic struct {
	topic *gcppubsub.Topic
}

func (t cloudPubSubTopic) Publish(ctx context.Context, message *gcppubsub.Message) pubSubPublishResult {
	return t.topic.Publish(ctx, message)
}

func NewPubSubPublisher[T ExchangeSymbolPayload](topic pubSubTopic) *PubSubPublisher[T] {
	return &PubSubPublisher[T]{topic: topic}
}

func NewCloudPubSubPublisher[T ExchangeSymbolPayload](
	ctx context.Context,
	projectID string,
	topicID string,
) (*PubSubPublisher[T], func() error, error) {
	client, err := gcppubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, fmt.Errorf("create pubsub client: %w", err)
	}

	topic := client.Topic(topicID)
	topic.EnableMessageOrdering = true

	cleanup := func() error {
		topic.Stop()
		return client.Close()
	}

	return NewPubSubPublisher[T](cloudPubSubTopic{topic: topic}), cleanup, nil
}

func (p *PubSubPublisher[T]) Publish(ctx context.Context, payload T) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pubsub payload: %w", err)
	}

	exchange, symbol := payload.ExchangeSymbol()
	result := p.topic.Publish(ctx, &gcppubsub.Message{
		Data:        data,
		OrderingKey: GetOrderingKey(exchange, symbol),
		Attributes: map[string]string{
			"exchange": exchange,
			"symbol":   symbol,
		},
	})
	if _, err := result.Get(ctx); err != nil {
		return fmt.Errorf("publish pubsub payload: %w", err)
	}
	return nil
}

func GetOrderingKey(exchange string, symbol string) string {
	return ExchangeSymbolKey(exchange, symbol)
}
