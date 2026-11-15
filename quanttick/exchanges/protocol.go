package exchanges

import (
	"bytes"
	"encoding/json"
	"fmt"

	quanttick "github.com/globophobe/go-quant-tick/quanttick"
)

func parseRawDecimal(raw json.RawMessage) (quanttick.Decimal, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return quanttick.Decimal{}, fmt.Errorf("missing decimal value")
	}
	if bytes.Equal(raw, []byte("null")) {
		return quanttick.Decimal{}, fmt.Errorf("null decimal value")
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return quanttick.Decimal{}, err
		}
		return quanttick.ParseDecimal(value)
	}
	return quanttick.ParseDecimal(string(raw))
}
