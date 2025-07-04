package main

import (
	"encoding/json"
)

// headerConst is a constant estimate for the HTTP request line and headers
// that wrap the OTLP payload.
const headerConst = 140

// EstimateHydratedSize implements the "start-from-zero" calculation to provide
// a highly accurate estimate of the final ingested data size.
//
// It works by:
// 1. Starting with the raw uncompressed payload size.
// 2. For each (resource, scope) group, it subtracts the size of the shared
//    attribute blocks to isolate the size of the "pure" log/span bodies.
// 3. It then calculates the total size of these attributes when duplicated
//    across every single log/span.
// 4. The final adjusted size is the sum of the pure bodies, the duplicated
//    attributes, and a constant for HTTP headers.
func EstimateHydratedSize(bodyBytes []byte) (raw int64, factor float64, adj int64, rows int) {
	raw = int64(len(bodyBytes))

	// Define lightweight envelope structures to parse only what's needed.
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
		// If parsing fails, fall back to a simple model: raw size + header constant.
		factor = 1.0
		adj = raw + headerConst
		return
	}

	var dupBytes int64
	// Start with the full payload size. We will subtract the shared attribute
	// blocks to isolate the size of the individual log/span bodies.
	bodiesOnlySize := raw

	for _, rs := range env.ResourceSpans {
		// Get the full size of the attribute blocks as they appear in the JSON.
		resAttrBlockSize := len(rs.Resource.Attributes)
		// The actual attributes to be duplicated don't include the outer '{}'.
		resAttrContentSize := resAttrBlockSize - 2
		if resAttrContentSize < 0 {
			resAttrContentSize = 0
		}

		for _, ss := range rs.ScopeSpans {
			scopeAttrBlockSize := len(ss.Scope.Attributes)
			scopeAttrContentSize := scopeAttrBlockSize - 2
			if scopeAttrContentSize < 0 {
				scopeAttrContentSize = 0
			}

			rowCount := len(ss.Spans) + len(ss.Logs)
			rows += rowCount

			if rowCount > 0 {
				// 1. Subtract the one-time cost of the attribute blocks from the total.
				// This leaves `bodiesOnlySize` holding (mostly) the size of the pure log/span bodies.
				bodiesOnlySize -= int64(resAttrBlockSize + scopeAttrBlockSize)

				// 2. Calculate the full cost of hydrating EVERY row in this group with attributes.
				perRowOverhead := int64(resAttrContentSize + scopeAttrContentSize)
				dupBytes += perRowOverhead * int64(rowCount)
			}
		}
	}

	// 3. The final debit is the size of the pure bodies + all duplicated attributes + headers.
	adj = bodiesOnlySize + dupBytes + headerConst

	if raw > 0 {
		factor = float64(adj) / float64(raw)
	} else {
		factor = 1.0
	}

	return
}

