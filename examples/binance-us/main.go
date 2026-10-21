package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BINANCE_US_SYMBOLS", exchanges.BinanceUSName, []string{"BTCUSDT"})
	example.Run(exchanges.NewBinanceUS(symbols.Symbols), symbols.Thresholds)
}
