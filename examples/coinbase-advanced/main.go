package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("COINBASE_ADVANCED_SYMBOLS", exchanges.CoinbaseAdvancedName, []string{"BTC-PERP-INTX"})
	example.Run(exchanges.NewCoinbaseAdvanced(symbols.Symbols), symbols.Thresholds)
}
