package exchanges

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

const (
	HyperliquidName = "hyperliquid"
	HyperliquidURL  = "wss://api.hyperliquid.xyz/ws"
)

var _ quanttick.Exchange = (*Hyperliquid)(nil)

type Hyperliquid struct {
	Symbols        []string
	URL            string
	ReconnectDelay time.Duration
}

type HyperliquidOption func(*Hyperliquid)

func NewHyperliquid(symbols []string, options ...HyperliquidOption) *Hyperliquid {
	exchange := &Hyperliquid{
		Symbols:        append([]string(nil), symbols...),
		URL:            HyperliquidURL,
		ReconnectDelay: time.Second,
	}
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

func WithHyperliquidURL(url string) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.URL = url
	}
}

func WithHyperliquidReconnectDelay(delay time.Duration) HyperliquidOption {
	return func(exchange *Hyperliquid) {
		exchange.ReconnectDelay = delay
	}
}

func (h *Hyperliquid) Name() string {
	return HyperliquidName
}

func (h *Hyperliquid) Trades(ctx context.Context) (<-chan quanttick.TradeEvent, <-chan error) {
	trades := make(chan quanttick.TradeEvent)
	errs := make(chan error, 1)

	go func() {
		defer close(trades)
		defer close(errs)

		for {
			if err := h.run(ctx, trades); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				sendError(ctx, errs, err)
			}

			if err := sleepContext(ctx, h.ReconnectDelay); err != nil {
				return
			}
		}
	}()

	return trades, errs
}

func (h *Hyperliquid) SubscriptionMessages() []map[string]any {
	messages := make([]map[string]any, 0, len(h.Symbols))
	for _, symbol := range h.Symbols {
		messages = append(messages, map[string]any{
			"method": "subscribe",
			"subscription": map[string]any{
				"type": "trades",
				"coin": symbol,
			},
		})
	}
	return messages
}

func (h *Hyperliquid) ParseTradeMessage(data []byte, receivedAt time.Time) ([]quanttick.TradeEvent, error) {
	var envelope hyperliquidEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("parse hyperliquid message: %w", err)
	}
	if envelope.Channel != "trades" {
		return nil, nil
	}

	var rawTrades []hyperliquidTrade
	if err := json.Unmarshal(envelope.Data, &rawTrades); err != nil {
		return nil, fmt.Errorf("parse hyperliquid trades: %w", err)
	}

	trades := make([]quanttick.TradeEvent, 0, len(rawTrades))
	for _, rawTrade := range rawTrades {
		price, err := quanttick.ParseDecimal(rawTrade.Price)
		if err != nil {
			return nil, fmt.Errorf("parse hyperliquid price: %w", err)
		}
		notional, err := quanttick.ParseDecimal(rawTrade.Size)
		if err != nil {
			return nil, fmt.Errorf("parse hyperliquid size: %w", err)
		}

		side := strings.ToLower(rawTrade.Side)
		tickRule := -1
		if side == "b" || side == "buy" {
			tickRule = 1
		}

		trades = append(trades, quanttick.NewTradeEvent(quanttick.TradeEventInput{
			Exchange:   HyperliquidName,
			UID:        strconv.FormatInt(rawTrade.Time, 10) + ":" + rawTrade.Coin + ":" + strconv.FormatInt(rawTrade.TradeID, 10),
			Symbol:     rawTrade.Coin,
			Timestamp:  time.UnixMilli(rawTrade.Time).UTC(),
			ReceivedAt: receivedAt,
			Price:      price,
			Notional:   notional,
			TickRule:   tickRule,
		}))
	}

	return trades, nil
}

func (h *Hyperliquid) run(ctx context.Context, trades chan<- quanttick.TradeEvent) error {
	conn, _, err := websocket.Dial(ctx, h.URL, nil)
	if err != nil {
		return fmt.Errorf("dial hyperliquid websocket: %w", err)
	}
	defer conn.CloseNow()

	for _, message := range h.SubscriptionMessages() {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal hyperliquid subscription: %w", err)
		}
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			return fmt.Errorf("send hyperliquid subscription: %w", err)
		}
	}

	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read hyperliquid websocket: %w", err)
		}
		if messageType != websocket.MessageText && messageType != websocket.MessageBinary {
			continue
		}

		parsedTrades, err := h.ParseTradeMessage(data, time.Now().UTC())
		if err != nil {
			return err
		}
		for _, trade := range parsedTrades {
			select {
			case trades <- trade:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

type hyperliquidEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type hyperliquidTrade struct {
	Coin    string `json:"coin"`
	Side    string `json:"side"`
	Price   string `json:"px"`
	Size    string `json:"sz"`
	Time    int64  `json:"time"`
	TradeID int64  `json:"tid"`
}
