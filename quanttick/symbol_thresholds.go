package quanttick

import (
	"fmt"
	"strings"
)

type SymbolThresholds struct {
	Symbols    []string
	Thresholds map[string]Decimal
}

func ParseSymbolThresholds(exchange string, values []string) (SymbolThresholds, error) {
	config := SymbolThresholds{
		Symbols:    make([]string, 0, len(values)),
		Thresholds: make(map[string]Decimal),
	}

	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		symbol, threshold, hasThreshold, err := parseSymbolThreshold(value)
		if err != nil {
			return SymbolThresholds{}, err
		}
		if symbol == "" {
			return SymbolThresholds{}, fmt.Errorf("symbol is required in %q", value)
		}

		config.Symbols = append(config.Symbols, symbol)
		if !hasThreshold {
			continue
		}

		config.Thresholds[ExchangeSymbolKey(exchange, symbol)] = threshold
	}

	return config, nil
}

func parseSymbolThreshold(value string) (string, Decimal, bool, error) {
	if symbol, thresholdValue, ok := strings.Cut(value, "="); ok {
		return parseExplicitSymbolThreshold(value, symbol, thresholdValue)
	}

	index := strings.LastIndex(value, ":")
	if index < 0 {
		return strings.TrimSpace(value), Decimal{}, false, nil
	}
	thresholdValue := strings.TrimSpace(value[index+1:])
	if thresholdValue == "" {
		return "", Decimal{}, false, fmt.Errorf("threshold is required in %q", value)
	}
	if _, err := ParseDecimal(thresholdValue); err == nil {
		return "", Decimal{}, false, fmt.Errorf("threshold override in %q must use SYMBOL=THRESHOLD", value)
	}
	return strings.TrimSpace(value), Decimal{}, false, nil
}

func parseExplicitSymbolThreshold(value string, symbol string, thresholdValue string) (string, Decimal, bool, error) {
	symbol = strings.TrimSpace(symbol)
	thresholdValue = strings.TrimSpace(thresholdValue)
	if thresholdValue == "" {
		return "", Decimal{}, false, fmt.Errorf("threshold is required in %q", value)
	}

	threshold, err := ParseDecimal(thresholdValue)
	if err != nil {
		return "", Decimal{}, false, fmt.Errorf("parse threshold for %s: %w", symbol, err)
	}
	return symbol, threshold, true, nil
}

func ExchangeSymbolKey(exchange string, symbol string) string {
	return exchange + ":" + symbol
}
