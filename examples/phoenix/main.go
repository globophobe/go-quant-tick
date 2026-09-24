package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("PHOENIX_SYMBOLS", exchanges.PhoenixName, []string{"BTC"})
	example.Run(exchanges.NewPhoenix(symbols.Symbols), symbols.Thresholds)
}
