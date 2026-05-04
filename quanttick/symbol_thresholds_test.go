package quanttick

import "testing"

func TestParseSymbolThresholds(t *testing.T) {
	config, err := ParseSymbolThresholds("binance", []string{"BTCUSDT:50000", "ETHUSDT=25000", " SOLUSDT "})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Symbols) != 3 {
		t.Fatalf("symbols = %#v, want 3 symbols", config.Symbols)
	}
	if config.Symbols[0] != "BTCUSDT" || config.Symbols[1] != "ETHUSDT" || config.Symbols[2] != "SOLUSDT" {
		t.Fatalf("symbols = %#v, want BTCUSDT, ETHUSDT, and SOLUSDT", config.Symbols)
	}

	threshold, ok := config.Thresholds[ExchangeSymbolKey("binance", "BTCUSDT")]
	if !ok {
		t.Fatal("expected BTCUSDT threshold")
	}
	assertDecimal(t, threshold, "50000")

	threshold, ok = config.Thresholds[ExchangeSymbolKey("binance", "ETHUSDT")]
	if !ok {
		t.Fatal("expected ETHUSDT threshold")
	}
	assertDecimal(t, threshold, "25000")
	if _, ok := config.Thresholds[ExchangeSymbolKey("binance", "SOLUSDT")]; ok {
		t.Fatal("SOLUSDT should use the default threshold")
	}
}

func TestParseSymbolThresholdsAllowsColonSymbols(t *testing.T) {
	config, err := ParseSymbolThresholds(
		"bitfinex",
		[]string{"tBTCF0:USTF0", "tETHF0:USTF0=25000"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Symbols) != 2 {
		t.Fatalf("symbols = %#v, want 2 symbols", config.Symbols)
	}
	if config.Symbols[0] != "tBTCF0:USTF0" || config.Symbols[1] != "tETHF0:USTF0" {
		t.Fatalf("symbols = %#v, want derivative symbols", config.Symbols)
	}

	threshold, ok := config.Thresholds[ExchangeSymbolKey("bitfinex", "tETHF0:USTF0")]
	if !ok {
		t.Fatal("expected tETHF0:USTF0 threshold")
	}
	assertDecimal(t, threshold, "25000")
	if _, ok := config.Thresholds[ExchangeSymbolKey("bitfinex", "tBTCF0:USTF0")]; ok {
		t.Fatal("tBTCF0:USTF0 should use the default threshold")
	}
}

func TestParseSymbolThresholdsRejectsInvalidThreshold(t *testing.T) {
	if _, err := ParseSymbolThresholds("binance", []string{"BTCUSDT=not-a-number"}); err == nil {
		t.Fatal("expected invalid threshold error")
	}
}

func TestParseSymbolThresholdsRejectsEmptyThreshold(t *testing.T) {
	if _, err := ParseSymbolThresholds("binance", []string{"BTCUSDT:"}); err == nil {
		t.Fatal("expected empty threshold error")
	}
}
