package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BINANCE_SYMBOLS", exchanges.BinanceName, []string{"BTCUSDT"})
	example.Run(exchanges.NewBinance(symbols.Symbols), symbols.Thresholds)
}
