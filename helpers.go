package pchr

import (
	"encoding/json"
	"strconv"
)

func decode(raw json.RawMessage, v any) error {
	return json.Unmarshal(raw, v)
}

// paymentSeqNum extracts the numeric counter from ids like "pay-000042".
func paymentSeqNum(id PaymentID) int64 {
	s := string(id)
	if len(s) > 4 && s[:4] == "pay-" {
		if n, err := strconv.ParseInt(s[4:], 10, 64); err == nil {
			return n
		}
	}
	return 0
}
