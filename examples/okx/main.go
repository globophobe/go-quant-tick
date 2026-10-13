package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("OKX_SYMBOLS", exchanges.OKXName, []string{"BTC-USDT", "BTC-USDT-SWAP"})
	example.Run(exchanges.NewOKX(symbols.Symbols), symbols.Thresholds)
}
