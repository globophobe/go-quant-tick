package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BYBIT_SYMBOLS", exchanges.BybitName, []string{"BTCUSDT"})
	example.Run(exchanges.NewBybit(symbols.Symbols), symbols.Thresholds)
}
