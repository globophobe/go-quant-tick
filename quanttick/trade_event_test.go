package quanttick

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTradeEventJSONEncodesDecimalsAsStrings(t *testing.T) {
	trade := testTrade(
		"1",
		time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC),
		withPrice("101.25"),
		withNotional("2"),
	)

	data, err := json.Marshal(trade)
	if err != nil {
		t.Fatal(err)
	}
	payload := string(data)

	for _, want := range []string{
		`"price":"101.25"`,
		`"notional":"2"`,
		`"volume":"202.5"`,
	} {
		if !strings.Contains(payload, want) {
			t.Fatalf("payload %s does not contain %s", payload, want)
		}
	}
}
