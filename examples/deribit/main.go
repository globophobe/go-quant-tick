package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("DERIBIT_SYMBOLS", exchanges.DeribitName, []string{"BTC-PERPETUAL"})
	example.Run(exchanges.NewDeribit(symbols.Symbols), symbols.Thresholds)
}
