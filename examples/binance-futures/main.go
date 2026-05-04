package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BINANCE_FUTURES_SYMBOLS", exchanges.BinanceFuturesName, []string{"BTCUSDT"})
	example.Run(exchanges.NewBinanceFutures(symbols.Symbols), symbols.Thresholds)
}
