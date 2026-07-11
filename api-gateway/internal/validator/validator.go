package validator

import (
	"encoding/json"
	"errors"
)

// TelemetryPayload maps strictly to incoming client SDK data schema
type TelemetryPayload struct {
	TsClient int64  `json:"tsclient"`
	TsServer int64  `json:"tsserver"`
	Sessid   string `json:"sessid"`
	Event    string `json:"event"`
	Value    string `json:"value"`
}

// ValidatePayload checks structural integrity and prevents dirty data execution leaks
func ValidatePayload(body []byte) ([]TelemetryPayload, error) {
	var TelemetryPayload []TelemetryPayload

	// Attempt raw layout verification
	if err := json.Unmarshal(body, &TelemetryPayload); err != nil {
		return nil, err
	}

	if len(TelemetryPayload) == 0 {
		return nil, errors.New("empty telemetry collection payload received")
	}

	// Validate vital session attributes inside elements
	for _, payload := range TelemetryPayload {
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

	return TelemetryPayload, nil
}
