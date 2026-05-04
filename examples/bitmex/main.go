package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BITMEX_SYMBOLS", exchanges.BitmexName, []string{"XBTUSD"})
	example.Run(exchanges.NewBitmex(symbols.Symbols), symbols.Thresholds)
}
