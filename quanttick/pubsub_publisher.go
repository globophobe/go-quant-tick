package quanttick

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gcppubsub "cloud.google.com/go/pubsub"
)

const defaultPublishTimeout = time.Second

type ExchangeSymbolPayload interface {
	ExchangeSymbol() (exchange string, symbol string)
}

type PubSubPublisherConfig struct {
	Timeout time.Duration
}

type PubSubPublisher[T ExchangeSymbolPayload] struct {
	topic   pubSubTopic
	timeout time.Duration
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

func NewPubSubPublisher[T ExchangeSymbolPayload](topic pubSubTopic, configs ...PubSubPublisherConfig) *PubSubPublisher[T] {
	config := PubSubPublisherConfig{}
	if len(configs) != 0 {
		config = configs[0]
	}
	return &PubSubPublisher[T]{
		topic:   topic,
		timeout: publishTimeoutOrDefault(config.Timeout),
	}
}

func NewCloudPubSubPublisher[T ExchangeSymbolPayload](
	ctx context.Context,
	projectID string,
	topicID string,
	config PubSubPublisherConfig,
) (*PubSubPublisher[T], func() error, error) {
	client, err := gcppubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, fmt.Errorf("create pubsub client: %w", err)
	}

	topic := client.Topic(topicID)
	topic.PublishSettings.CountThreshold = 1
	topic.PublishSettings.DelayThreshold = time.Millisecond

	cleanup := func() error {
		topic.Stop()
		return client.Close()
	}

	return NewPubSubPublisher[T](cloudPubSubTopic{topic: topic}, config), cleanup, nil
}

func (p *PubSubPublisher[T]) Publish(ctx context.Context, payload T) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pubsub payload: %w", err)
	}

	exchange, symbol := payload.ExchangeSymbol()
	message := &gcppubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"exchange": exchange,
			"symbol":   symbol,
		},
	}
	publishCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	started := time.Now()
	result := p.topic.Publish(publishCtx, message)
	_, err = result.Get(publishCtx)
	elapsed := time.Since(started)
	if err != nil {
		return fmt.Errorf(
			"publish pubsub payload exchange=%s symbol=%s elapsed=%s: %w",
			exchange,
			symbol,
			elapsed.Round(time.Millisecond),
			err,
		)
	}
	return nil
}

func publishTimeoutOrDefault(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return defaultPublishTimeout
	}
	return timeout
}
