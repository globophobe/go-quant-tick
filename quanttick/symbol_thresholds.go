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

		symbol := value
		thresholdValue := ""
		hasThreshold := false
		if left, right, ok := strings.Cut(value, ":"); ok {
			symbol = strings.TrimSpace(left)
			thresholdValue = strings.TrimSpace(right)
			hasThreshold = true
		}
		if symbol == "" {
			return SymbolThresholds{}, fmt.Errorf("symbol is required in %q", value)
		}

		config.Symbols = append(config.Symbols, symbol)
		if !hasThreshold {
			continue
		}

		threshold, err := ParseDecimal(thresholdValue)
		if err != nil {
			return SymbolThresholds{}, fmt.Errorf("parse threshold for %s: %w", symbol, err)
		}
		config.Thresholds[ExchangeSymbolKey(exchange, symbol)] = threshold
	}

	return config, nil
}

func ExchangeSymbolKey(exchange string, symbol string) string {
	return exchange + ":" + symbol
}
