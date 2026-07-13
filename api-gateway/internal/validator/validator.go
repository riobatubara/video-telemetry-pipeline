package validator

import (
	"encoding/json"
	"errors"
)

// TelemetryPayload maps strictly to incoming client SDK data schema
type Payload struct {
	TsClient int64  `json:"tsclient"`
	TsServer int64  `json:"tsserver"`
	Sessid   string `json:"sessid"`
	Event    string `json:"event"`
	Value    string `json:"value"`
}

// ValidatePayload checks structural integrity and prevents dirty data execution leaks
func ValidatePayload(body []byte) ([]Payload, error) {
	var payloads []Payload

	// Attempt raw layout verification
	if err := json.Unmarshal(body, &payloads); err != nil {
		return nil, err
	}

	if len(payloads) == 0 {
		return nil, errors.New("empty telemetry collection payloads received")
	}

	// Validate vital session attributes inside elements
	for _, payload := range payloads {
		if payload.TsClient <= 0 {
			return nil, errors.New("malformed record payload missing or invalid tsclient timestamp")
		}
		if payload.Sessid == "" {
			return nil, errors.New("malformed record payload missing unique sessid token")
		}
		if payload.Event == "" {
			return nil, errors.New("malformed record payload missing trigger event identifier")
		}
		if payload.Value == "" {
			return nil, errors.New("malformed record payload missing event value")
		}
	}

	return payloads, nil
}
