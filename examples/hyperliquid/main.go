package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("HYPERLIQUID_SYMBOLS", exchanges.HyperliquidName, []string{"BTC"})
	example.Run(exchanges.NewHyperliquid(symbols.Symbols), symbols.Thresholds)
}
