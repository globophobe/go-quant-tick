package exchanges

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	BinanceName                    = "binance"
	BinanceFuturesName             = "binance-futures"
	BinanceURL                     = "wss://stream.binance.com:9443/ws"
	BinanceFuturesURL              = "wss://fstream.binance.com/market/stream"
	BinanceSpotRESTURL             = "https://api.binance.com/api/v3"
	BinanceFuturesRESTURL          = "https://fapi.binance.com/fapi/v1"
	binanceSubscriptionBufferLimit = 10000
	binanceSubscriptionRequestID   = 1
	binanceRecoveryPageLimit       = 1000
	binanceRecoveryMaxPages        = 100
	binanceRecoveryMaxWeight       = 1200
	binanceRecoveryRetryDelay      = time.Second
)

var (
	_                        quanttick.Exchange = (*Binance)(nil)
	errBinanceServerShutdown                    = errors.New("binance server shutdown")
)

type Binance struct {
	Symbols             []string
	name                string
	URL                 string
	RESTURL             string
	APIKey              string
	HTTPClient          *http.Client
	stream              string
	ReconnectDelay      time.Duration
	SubscriptionTimeout time.Duration
	RecoveryMaxWeight   int

	lastIDs          map[string]int64
	recoveryThrottle *restThrottle
}

type binanceParsedTrade struct {
	event   quanttick.TradeEvent
	tradeID int64
}

type BinanceOption func(*Binance)

func NewBinance(symbols []string, options ...BinanceOption) *Binance {
	exchange := &Binance{
		Symbols:             append([]string(nil), symbols...),
		name:                BinanceName,
		URL:                 BinanceURL,
		RESTURL:             BinanceSpotRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		stream:              "trade",
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		RecoveryMaxWeight:   binanceRecoveryMaxWeight,
		lastIDs:             make(map[string]int64),
		recoveryThrottle:    newRESTThrottle(0),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func NewBinanceFutures(symbols []string, options ...BinanceOption) *Binance {
	exchange := &Binance{
		Symbols:             append([]string(nil), symbols...),
		name:                BinanceFuturesName,
		URL:                 BinanceFuturesURL,
		RESTURL:             BinanceFuturesRESTURL,
		HTTPClient:          defaultRecoveryHTTPClient,
		stream:              "aggTrade",
		ReconnectDelay:      time.Second,
		SubscriptionTimeout: websocketSubscriptionTimeout,
		RecoveryMaxWeight:   binanceRecoveryMaxWeight,
		lastIDs:             make(map[string]int64),
		recoveryThrottle:    newRESTThrottle(0),
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithBinanceURL(url string) BinanceOption {
	return func(exchange *Binance) {
		exchange.URL = url
	}
}

func WithBinanceFuturesRESTURL(url string) BinanceOption {
	return WithBinanceRESTURL(url)
}

func WithBinanceRESTURL(url string) BinanceOption {
	return func(exchange *Binance) {
		exchange.RESTURL = url
	}
}

func WithBinanceAPIKey(apiKey string) BinanceOption {
	return func(exchange *Binance) {
		exchange.APIKey = strings.TrimSpace(apiKey)
	}
}

func WithBinanceHTTPClient(client *http.Client) BinanceOption {
	return func(exchange *Binance) {
		exchange.HTTPClient = client
	}
}

func WithBinanceReconnectDelay(delay time.Duration) BinanceOption {
	return func(exchange *Binance) {
		exchange.ReconnectDelay = delay
	}
}

func WithBinanceSubscriptionTimeout(timeout time.Duration) BinanceOption {
	return func(exchange *Binance) {
		exchange.SubscriptionTimeout = timeout
	}
}

func WithBinanceRecoveryMaxWeight(maxWeight int) BinanceOption {
	return func(exchange *Binance) {
		exchange.RecoveryMaxWeight = maxWeight
	}
}

func (b *Binance) Name() string {
	return b.name
}

func (b *Binance) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(trades)
		defer close(errs)
		backoff := newReconnectBackoff(b.ReconnectDelay)

		for {
			startedAt := time.Now()
			if err := b.run(ctx, trades, errs); err != nil {
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, errBinanceServerShutdown) {
					continue
				}
				sendError(ctx, errs, err)
			}

			if err := sleepContext(ctx, backoff.Next(time.Since(startedAt))); err != nil {
				return
			}
		}
	}()

	return trades, errs
}

func (b *Binance) SubscriptionMessages() []map[string]any {
	params := make([]string, 0, len(b.Symbols))
	for _, symbol := range b.Symbols {
		params = append(params, strings.ToLower(symbol)+"@"+b.stream)
	}

	return []map[string]any{
		{
			"method": "SUBSCRIBE",
			"params": params,
			"id":     binanceSubscriptionRequestID,
		},
	}
}

func (b *Binance) run(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	errs chan<- error,
) error {
	conn, _, err := websocket.Dial(ctx, b.URL, nil)
	if err != nil {
		return fmt.Errorf("dial binance websocket: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	defer conn.CloseNow()

	for _, message := range b.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal binance subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send binance subscription: %w", err)
		}
	}

	sequenceIDs := cloneBinanceLastIDs(b.lastIDs)
	buffered, err := b.awaitSubscription(ctx, conn, sequenceIDs)
	if err != nil {
		return err
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	streamSequenceIDs := cloneBinanceLastIDs(sequenceIDs)
	stream, streamErr := b.startTradeReader(streamCtx, conn, streamSequenceIDs)
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, reconnectRecoveryTimeout)
	recovered, recoveryErr := b.recoverTrades(recoveryCtx)
	cancelRecovery()
	for _, parsed := range recovered {
		if err := b.emitParsedTrade(ctx, trades, sequenceIDs, parsed); err != nil {
			return err
		}
	}
	if recoveryErr != nil {
		sendError(ctx, errs, recoveryErr)
	}
	for _, parsed := range buffered {
		if err := b.emitParsedTrade(ctx, trades, sequenceIDs, parsed); err != nil {
			cancelStream()
			return err
		}
	}
	for parsed := range stream {
		if err := b.emitParsedTrade(ctx, trades, sequenceIDs, parsed); err != nil {
			cancelStream()
			return err
		}
	}
	cancelStream()
	err = <-streamErr
	if isNormalWebSocketClose(err) {
		return nil
	}
	if err == nil {
		return nil
	}
	return err
}

func (b *Binance) startTradeReader(
	ctx context.Context,
	conn *websocket.Conn,
	sequenceIDs map[string]int64,
) (<-chan binanceParsedTrade, <-chan error) {
	trades := make(chan binanceParsedTrade, binanceSubscriptionBufferLimit)
	errs := make(chan error, 1)
	go func() {
		defer close(trades)
		defer close(errs)
		for {
			messageType, data, err := conn.Read(ctx)
			if err != nil {
				errs <- fmt.Errorf("read binance websocket: %w", err)
				return
			}
			if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
				continue
			}

			isAck, err := parseBinanceSubscriptionResponse(data)
			if err != nil {
				errs <- err
				return
			}
			if isAck {
				continue
			}

			parsed, ok, err := b.parseTradeMessage(data, time.Now().UTC(), sequenceIDs)
			if err != nil {
				errs <- err
				return
			}
			if !ok {
				continue
			}
			select {
			case trades <- parsed:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			default:
				errs <- fmt.Errorf(
					"binance websocket trade buffer exceeded %d events",
					binanceSubscriptionBufferLimit,
				)
				_ = conn.CloseNow()
				return
			}
		}
	}()
	return trades, errs
}

func (b *Binance) emitParsedTrade(
	ctx context.Context,
	trades chan<- quanttick.TradeEvent,
	sequenceIDs map[string]int64,
	parsed binanceParsedTrade,
) error {
	previousID, hadPreviousID := b.lastIDs[parsed.event.Symbol]
	if hadPreviousID && parsed.tradeID <= previousID {
		return nil
	}
	parsed.event.IsSequential = hadPreviousID && parsed.tradeID == previousID+1
	if err := sendTrade(ctx, trades, parsed.event); err != nil {
		return err
	}
	b.lastIDs[parsed.event.Symbol] = parsed.tradeID
	sequenceIDs[parsed.event.Symbol] = parsed.tradeID
	return nil
}

func (b *Binance) awaitSubscription(
	ctx context.Context,
	conn *websocket.Conn,
	sequenceIDs map[string]int64,
) ([]binanceParsedTrade, error) {
	ackCtx, cancel := context.WithTimeout(ctx, b.SubscriptionTimeout)
	defer cancel()

	buffered := make([]binanceParsedTrade, 0)
	for {
		messageType, data, err := conn.Read(ackCtx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ackCtx.Err() != nil {
				return nil, fmt.Errorf("binance subscription acknowledgement timed out after %s", b.SubscriptionTimeout)
			}
			return nil, fmt.Errorf("read binance subscription acknowledgement: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		isAck, err := parseBinanceSubscriptionResponse(data)
		if err != nil {
			return nil, err
		}
		if isAck {
			return buffered, nil
		}

		parsed, ok, err := b.parseTradeMessage(data, time.Now().UTC(), sequenceIDs)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if len(buffered) >= binanceSubscriptionBufferLimit {
			return nil, fmt.Errorf("binance subscription trade buffer exceeded %d events", binanceSubscriptionBufferLimit)
		}
		buffered = append(buffered, parsed)
	}
}

func cloneBinanceLastIDs(lastIDs map[string]int64) map[string]int64 {
	cloned := make(map[string]int64, len(lastIDs))
	for symbol, tradeID := range lastIDs {
		cloned[symbol] = tradeID
	}
	return cloned
}
