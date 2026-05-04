package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BITFINEX_SYMBOLS", exchanges.BitfinexName, []string{"tBTCF0:USTF0"})
	example.Run(exchanges.NewBitfinex(symbols.Symbols), symbols.Thresholds)
}
