package main

import (
	"encoding/json"
)

type EstimationResult struct {
	RawBytes        int64   // Original OTLP JSON size
	AdjustedBytes   int64   // Combined estimated size (spans + logs)
	ExpansionFactor float64
	SpanCount       int
	LogCount        int
}

func EstimateAll(bodyBytes []byte) EstimationResult {
	rawSize := int64(len(bodyBytes))

	var root struct {
		ResourceSpans []json.RawMessage `json:"resourceSpans"`
	}
	if err := json.Unmarshal(bodyBytes, &root); err != nil {
		return EstimationResult{
			RawBytes:        rawSize,
			AdjustedBytes:   rawSize,
			ExpansionFactor: 1.0,
		}
	}

	var totalAdjusted int64
	var totalSpans, totalLogs int

	for _, rs := range root.ResourceSpans {
		spanSize, spanCount := EstimateSpans(rs)
		_, _, logSize, logCount := EstimateLogs(rs)

		totalAdjusted += spanSize + logSize
		totalSpans += spanCount
		totalLogs += logCount
	}

	factor := 1.0
	if rawSize > 0 {
		factor = float64(totalAdjusted) / float64(rawSize)
	}

	return EstimationResult{
		RawBytes:        rawSize,
		AdjustedBytes:   totalAdjusted,
		ExpansionFactor: factor,
		SpanCount:       totalSpans,
		LogCount:        totalLogs,
	}
}

