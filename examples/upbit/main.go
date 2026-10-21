package main

import (
	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("UPBIT_SYMBOLS", exchanges.UpbitName, []string{"KRW-BTC=1000000"})
	example.Run(exchanges.NewUpbit(symbols.Symbols), symbols.Thresholds)
}
