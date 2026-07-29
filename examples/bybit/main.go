package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BYBIT_LINEAR_SYMBOLS", exchanges.BybitLinearName, []string{"BTCUSDT"})
	example.Run(exchanges.NewBybitLinear(symbols.Symbols), symbols.Thresholds)
}
