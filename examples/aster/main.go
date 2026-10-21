package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("ASTER_SYMBOLS", exchanges.AsterName, []string{"BTCUSDT"})
	example.Run(exchanges.NewAster(symbols.Symbols), symbols.Thresholds)
}
