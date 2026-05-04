package quanttick

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gcppubsub "cloud.google.com/go/pubsub"
)

func TestPubSubPublisherPublishesPayloadWithOrderingKeyAndAttributes(t *testing.T) {
	topic := &fakePubSubTopic{}
	publisher := NewPubSubPublisher[TradeEvent](topic)
	trade := testTrade("1", time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC))

	if err := publisher.Publish(context.Background(), trade); err != nil {
		t.Fatal(err)
	}

	if topic.message == nil {
		t.Fatal("expected pubsub message")
	}
	if topic.message.OrderingKey != "test:BTCUSD" {
		t.Fatalf("ordering key = %s, want test:BTCUSD", topic.message.OrderingKey)
	}
	if topic.message.Attributes["exchange"] != "test" {
		t.Fatalf("exchange attribute = %s, want test", topic.message.Attributes["exchange"])
	}
	if topic.message.Attributes["symbol"] != "BTCUSD" {
		t.Fatalf("symbol attribute = %s, want BTCUSD", topic.message.Attributes["symbol"])
	}

	var payload TradeEvent
	if err := json.Unmarshal(topic.message.Data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.UID != "1" {
		t.Fatalf("payload uid = %s, want 1", payload.UID)
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

func TestGetOrderingKey(t *testing.T) {
	if got := GetOrderingKey("binance", "BTCUSDT"); got != "binance:BTCUSDT" {
		t.Fatalf("ordering key = %s, want binance:BTCUSDT", got)
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
	err error
}

func (r fakePubSubResult) Get(ctx context.Context) (string, error) {
	return "message-id", r.err
}
