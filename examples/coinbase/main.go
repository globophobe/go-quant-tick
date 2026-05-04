package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("COINBASE_SYMBOLS", exchanges.CoinbaseName, []string{"BTC-USD"})
	example.Run(exchanges.NewCoinbase(symbols.Symbols), symbols.Thresholds)
}
