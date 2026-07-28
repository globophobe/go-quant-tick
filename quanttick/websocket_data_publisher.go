package quanttick

import (
	"database/sql"
	"sync"
	"time"
)

type WebSocketDataStore struct {
	db *sql.DB
}

type WebSocketDataBufferConfig struct {
	DefaultSignificantTradeFilter Decimal
	SignificantThresholds         map[string]Decimal
}

type WebSocketDataBuffer struct {
	mu      sync.Mutex
	flushMu sync.Mutex
	buckets map[websocketDataBucketKey]WebSocketDataBucket
	store   *WebSocketDataStore
	config  WebSocketDataBufferConfig
}

type WebSocketDataBucket struct {
	Exchange               string
	APISymbol              string
	SignificantTradeFilter int64
	Timestamp              time.Time
	RawTrades              []TradeEvent
	AggregatedTrades       []TradeEvent
	FilteredTrades         []SignificantTrade
	rawTradeUIDs           map[string]struct{}
	aggregatedTradeUIDs    map[string]struct{}
}

type websocketDataBucketKey struct {
	exchange               string
	symbol                 string
	significantTradeFilter int64
	timestamp              time.Time
}

type websocketDataTradePublisher struct {
	stream Stream
	buffer *WebSocketDataBuffer
}

func NewWebSocketDataStore(db *sql.DB) *WebSocketDataStore {
	return &WebSocketDataStore{db: db}
}

func NewWebSocketDataBuffer(store *WebSocketDataStore, config WebSocketDataBufferConfig) *WebSocketDataBuffer {
	return &WebSocketDataBuffer{
		buckets: make(map[websocketDataBucketKey]WebSocketDataBucket),
		store:   store,
		config: WebSocketDataBufferConfig{
			DefaultSignificantTradeFilter: config.DefaultSignificantTradeFilter,
			SignificantThresholds:         cloneThresholds(config.SignificantThresholds),
		},
	}
}

func (b *WebSocketDataBuffer) RawPublisher() Publisher[TradeEvent] {
	return websocketDataTradePublisher{stream: RawTrades, buffer: b}
}

func (b *WebSocketDataBuffer) AggregatedPublisher() Publisher[TradeEvent] {
	return websocketDataTradePublisher{stream: AggregatedTrades, buffer: b}
}
