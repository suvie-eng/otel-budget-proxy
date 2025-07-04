package main

import (
	"encoding/json"
)

// wrapperOverhead is a constant estimate for the extra fields (like timestamps,
// trace IDs, etc.) that wrap each individual span or log record when it's
// stored in the backend. This is used to more accurately estimate the final,
// hydrated data size.
const wrapperOverhead = 70
const headerConst = 140

// EstimateHydratedSize inspects a raw, uncompressed OTLP/JSON payload and
// returns an estimate of its final size after ingestion and hydration in a
// system like HyperDX.
//
// It returns:
//  * raw:    The size of the original uncompressed JSON payload in bytes.
//  * factor: The calculated inflation factor (hydrated size / raw size).
//  * adj:    The final adjusted size in bytes to be debited from the budget.
//  * rows:   The total number of spans, logs, and metrics found, for debugging.
//
// The function walks through the OTLP structure (ResourceSpans -> ScopeSpans)
// and for each span, log, or metric record, it adds the size of the shared
// resource and scope attributes, plus a constant overhead. This accounts for
// data duplication that occurs during ingestion.
func EstimateHydratedSize(bodyBytes []byte) (raw int64, factor float64, adj int64, rows int) {
	raw = int64(len(bodyBytes))

	// Define lightweight envelope structures to parse only what's needed for estimation.
	// This avoids unmarshalling the entire, potentially large, payload into memory.
	type scopeSpans struct {
		Scope struct {
			Attributes json.RawMessage `json:"attributes"`
		} `json:"scope"`
		Spans   []json.RawMessage `json:"spans"`
		Logs    []json.RawMessage `json:"logs"`
		Metrics []json.RawMessage `json:"metrics"` // Correctly include metrics in parsing.
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
		// If the payload is malformed or not valid JSON, we can't inspect it.
		// Fall back to a simple billing model: raw size plus a standard header allowance.
		factor = 1.0
		adj = raw + headerConst // 200 is a constant for headers.
		return
	}

	// Use int64 for dupBytes to prevent integer overflow on 32-bit systems
	// or with extremely large payloads (> 2GiB).
	var dupBytes int64
	for _, rs := range env.ResourceSpans {
		resBytes := len(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			scopeBytes := len(ss.Scope.Attributes)

			// Calculate the overhead that will be duplicated for each row in this scope.
			// This must be int64 to prevent overflow during multiplication.
			rowOverhead := int64(resBytes + scopeBytes + wrapperOverhead)

			// Count all types of signals (spans, logs, and metrics).
			rowCount := len(ss.Spans) + len(ss.Logs) + len(ss.Metrics)
			rows += rowCount

			// Add the duplicated overhead for all rows in this scope to the total.
			// Cast rowCount to int64 for safe multiplication.
			dupBytes += rowOverhead * int64(rowCount)
		}
	}

	// The final adjusted size is the original raw size plus the calculated
	// duplicated bytes and the constant header estimate.
	adj = raw + dupBytes + headerConst
	if raw > 0 {
		factor = float64(adj) / float64(raw)
	} else {
		// Avoid division by zero if the payload was empty.
		factor = 1.0
	}
	return
}

