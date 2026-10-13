package exchanges

import "time"

const (
	BinanceUSName    = "binance-us"
	BinanceUSURL     = "wss://stream.binance.us:9443/ws"
	BinanceUSRESTURL = "https://api.binance.us/api/v3"
	AsterName        = "aster"
	AsterURL         = "wss://fstream.asterdex.com/ws"
	AsterRESTURL     = "https://fapi.asterdex.com/fapi/v3"
)

func NewBinanceUS(symbols []string, options ...BinanceOption) *Binance {
	exchange := NewBinance(symbols)
	exchange.name = BinanceUSName
	exchange.URL = BinanceUSURL
	exchange.RESTURL = BinanceUSRESTURL
	exchange.RecoveryMaxWeight = 500
	exchange.recoveryThrottle = newRESTThrottle(3 * time.Second)
	for _, option := range options {
		option(exchange)
	}
	return exchange
}

// Aster's trade stream and historicalTrades share individual trade IDs.
func NewAster(symbols []string, options ...BinanceOption) *Binance {
	exchange := NewBinance(symbols)
	exchange.name = AsterName
	exchange.URL = AsterURL
	exchange.RESTURL = AsterRESTURL
	exchange.recoveryThrottle = newRESTThrottle(time.Second / 2)
	for _, option := range options {
		option(exchange)
	}
	return exchange
}
