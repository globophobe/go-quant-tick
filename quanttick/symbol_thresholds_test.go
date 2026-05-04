package quanttick

import "testing"

func TestParseSymbolThresholds(t *testing.T) {
	config, err := ParseSymbolThresholds("binance", []string{"BTCUSDT:50000", " ETHUSDT "})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Symbols) != 2 {
		t.Fatalf("symbols = %#v, want 2 symbols", config.Symbols)
	}
	if config.Symbols[0] != "BTCUSDT" || config.Symbols[1] != "ETHUSDT" {
		t.Fatalf("symbols = %#v, want BTCUSDT and ETHUSDT", config.Symbols)
	}

	threshold, ok := config.Thresholds[ExchangeSymbolKey("binance", "BTCUSDT")]
	if !ok {
		t.Fatal("expected BTCUSDT threshold")
	}
	assertDecimal(t, threshold, "50000")
	if _, ok := config.Thresholds[ExchangeSymbolKey("binance", "ETHUSDT")]; ok {
		t.Fatal("ETHUSDT should use the default threshold")
	}
}

func TestParseSymbolThresholdsRejectsInvalidThreshold(t *testing.T) {
	if _, err := ParseSymbolThresholds("binance", []string{"BTCUSDT:not-a-number"}); err == nil {
		t.Fatal("expected invalid threshold error")
	}
}
