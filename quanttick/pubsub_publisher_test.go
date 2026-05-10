package quanttick

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gcppubsub "cloud.google.com/go/pubsub"
)

func TestPubSubPublisherPublishesPayloadWithAttributes(t *testing.T) {
	topic := &fakePubSubTopic{}
	publisher := NewPubSubPublisher[TradeEvent](topic)
	trade := testTrade("1", time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC))

	if err := publisher.Publish(context.Background(), trade); err != nil {
		t.Fatal(err)
	}

	if topic.message == nil {
		t.Fatal("expected pubsub message")
	}
	if topic.message.OrderingKey != "" {
		t.Fatalf("ordering key = %s, want empty", topic.message.OrderingKey)
	}
	if topic.message.Attributes["exchange"] != "test" {
		t.Fatalf("exchange attribute = %s, want test", topic.message.Attributes["exchange"])
	}
	if topic.message.Attributes["symbol"] != "BTCUSD" {
		t.Fatalf("symbol attribute = %s, want BTCUSD", topic.message.Attributes["symbol"])
	}
	if _, ok := topic.message.Attributes["significant_trade_filter"]; ok {
		t.Fatal("raw trade should not include significant_trade_filter attribute")
	}

	var payload TradeEvent
	if err := json.Unmarshal(topic.message.Data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.UID != "1" {
		t.Fatalf("payload uid = %s, want 1", payload.UID)
	}
}

func TestPubSubPublisherPublishesSignificantTradeFilterAttribute(t *testing.T) {
	topic := &fakePubSubTopic{}
	publisher := NewPubSubPublisher[SignificantTrade](topic)
	trade := SignificantTrade{
		Exchange:               "coinbase",
		Symbol:                 "BTC-USD",
		SignificantTradeFilter: MustDecimal("1000"),
	}

	if err := publisher.Publish(context.Background(), trade); err != nil {
		t.Fatal(err)
	}

	if topic.message.Attributes["exchange"] != "coinbase" {
		t.Fatalf("exchange attribute = %s, want coinbase", topic.message.Attributes["exchange"])
	}
	if topic.message.Attributes["symbol"] != "BTC-USD" {
		t.Fatalf("symbol attribute = %s, want BTC-USD", topic.message.Attributes["symbol"])
	}
	if topic.message.Attributes["significant_trade_filter"] != "1000" {
		t.Fatalf(
			"significant_trade_filter attribute = %s, want 1000",
			topic.message.Attributes["significant_trade_filter"],
		)
	}
}

func TestPubSubPublisherReturnsPublishResultError(t *testing.T) {
	want := errors.New("publish failed")
	topic := &fakePubSubTopic{result: fakePubSubResult{err: want}}
	publisher := NewPubSubPublisher[TradeEvent](topic)

	err := publisher.Publish(context.Background(), testTrade("1", time.Now().UTC()))
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestPubSubPublisherUsesPublishTimeout(t *testing.T) {
	topic := &fakePubSubTopic{result: fakePubSubResult{wait: time.Second}}
	publisher := NewPubSubPublisher[TradeEvent](topic, PubSubPublisherConfig{Timeout: time.Nanosecond})

	err := publisher.Publish(context.Background(), testTrade("1", time.Now().UTC()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}

type fakePubSubTopic struct {
	message *gcppubsub.Message
	result  fakePubSubResult
}

func (t *fakePubSubTopic) Publish(ctx context.Context, message *gcppubsub.Message) pubSubPublishResult {
	t.message = message
	return t.result
}

type fakePubSubResult struct {
	err  error
	wait time.Duration
}

func (r fakePubSubResult) Get(ctx context.Context) (string, error) {
	if r.wait > 0 {
		select {
		case <-time.After(r.wait):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "message-id", r.err
}
