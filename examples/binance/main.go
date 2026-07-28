package main

import (
	"os"

	"github.com/globophobe/go-quant-tick/examples/internal/example"
	"github.com/globophobe/go-quant-tick/quanttick/exchanges"
)

func main() {
	symbols := example.SymbolsEnv("BINANCE_SYMBOLS", exchanges.BinanceName, []string{"BTCUSDT"})
	example.Run(
		exchanges.NewBinance(
			symbols.Symbols,
			exchanges.WithBinanceAPIKey(os.Getenv("BINANCE_API_KEY")),
		),
		symbols.Thresholds,
	)
}
