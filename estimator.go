package main

import (
	"encoding/json"
	"fmt"
)

// headerConst is a constant estimate for the HTTP request line and headers
// that wrap the OTLP payload.
const headerConst = 140

// OtelValue and OtelAttribute are helper structs to parse the verbose
// OTLP JSON attribute format `[{"key": "k", "value": {"type": "v"}}]`.
type OtelValue struct {
	StringValue *string  `json:"stringValue"`
	IntValue    *string  `json:"intValue"` // OTLP JSON encodes int64 as a string
	BoolValue   *bool    `json:"boolValue"`
	DoubleValue *float64 `json:"doubleValue"`
}

type OtelAttribute struct {
	Key   string    `json:"key"`
	Value OtelValue `json:"value"`
}

// getCompactAttributeSize implements the high-performance calculation method.
// It parses the verbose attribute array and manually calculates the byte size
// of the equivalent compact JSON map, avoiding expensive re-marshaling.
func getCompactAttributeSize(rawAttrs json.RawMessage) int {
	if len(rawAttrs) <= 2 { // It's just "[]" or empty
		return 0
	}

	var verboseAttrs []OtelAttribute
	if err := json.Unmarshal(rawAttrs, &verboseAttrs); err != nil {
		// If parsing fails, we can't determine the compact size.
		return 0
	}

	if len(verboseAttrs) == 0 {
		return 0
	}

	totalBytes := 0
	for _, attr := range verboseAttrs {
		// Add size for the key, e.g., "service.name":
		totalBytes += len(attr.Key) + 3 // len(key) + 2 quotes + 1 colon

		// Add size for the value
		if attr.Value.StringValue != nil {
			totalBytes += len(*attr.Value.StringValue) + 2 // len(value) + 2 quotes
		} else if attr.Value.IntValue != nil {
			totalBytes += len(*attr.Value.IntValue) // int is sent as string in OTLP/JSON
		} else if attr.Value.BoolValue != nil {
			if *attr.Value.BoolValue {
				totalBytes += 4 // "true"
			} else {
				totalBytes += 5 // "false"
			}
		} else if attr.Value.DoubleValue != nil {
			// fmt.Sprint is a reasonable and fast approximation for float size.
			totalBytes += len(fmt.Sprint(*attr.Value.DoubleValue))
		}

		// Add 1 for the comma separating each key-value pair
		totalBytes += 1
	}

	// The loop adds one extra comma at the end. The final size of the
	// contents of a map doesn't have a trailing comma.
	// e.g. {"k1":"v1","k2":"v2"} -> contents are `"k1":"v1","k2":"v2"`
	return totalBytes - 1
}

// EstimateHydratedSize now uses the high-performance key/value transformation
// logic for maximum accuracy and speed.
func EstimateHydratedSize(bodyBytes []byte) (raw int64, factor float64, adj int64, rows int) {
	raw = int64(len(bodyBytes))

	type scopeSpans struct {
		Scope struct {
			Attributes json.RawMessage `json:"attributes"`
		} `json:"scope"`
		Spans []json.RawMessage `json:"spans"`
		Logs  []json.RawMessage `json:"logs"`
	}
	var env struct {
		ResourceSpans []struct {
			Resource struct {
				Attributes json.RawMessage `json:"attributes"`
			} `json:"resource"`
			ScopeSpans []scopeSpans `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}

	if err := json.Unmarshal(bodyBytes, &env); err != nil {
		factor = 1.0
		adj = raw + headerConst
		return
	}

	var dupBytes int64
	bodiesOnlySize := raw

	for _, rs := range env.ResourceSpans {
		resAttrBlockSize := len(rs.Resource.Attributes)
		// NEW: Calculate the size of the *transformed*, compact resource attributes.
		resAttrContentSize := getCompactAttributeSize(rs.Resource.Attributes)

		for _, ss := range rs.ScopeSpans {
			scopeAttrBlockSize := len(ss.Scope.Attributes)
			// NEW: Calculate the size of the *transformed*, compact scope attributes.
			scopeAttrContentSize := getCompactAttributeSize(ss.Scope.Attributes)

			rowCount := len(ss.Spans) + len(ss.Logs)
			rows += rowCount

			if rowCount > 0 {
				// Subtract the one-time cost of the original, verbose attribute blocks.
				bodiesOnlySize -= int64(resAttrBlockSize + scopeAttrBlockSize)

				// Calculate the full cost of hydrating EVERY row with the new, compact attributes.
				perRowOverhead := int64(resAttrContentSize + scopeAttrContentSize)
				dupBytes += perRowOverhead * int64(rowCount)
			}
		}
	}

	adj = bodiesOnlySize + dupBytes + headerConst
	if raw > 0 {
		factor = float64(adj) / float64(raw)
	} else {
		factor = 1.0
	}

	return
}

