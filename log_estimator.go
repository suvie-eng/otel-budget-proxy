package main

import (
	"encoding/json"
)

func EstimateLogs(bodyBytes []byte) (raw int64, factor float64, adj int64, rows int) {
	raw = int64(len(bodyBytes))

	// Use generic decoding for logs
	type scopeLogs struct {
		LogRecords []json.RawMessage `json:"logRecords"`
	}
	type resourceLogs struct {
		ScopeLogs []scopeLogs `json:"scopeLogs"`
	}
	var env struct {
		ResourceLogs []resourceLogs `json:"resourceLogs"`
	}

	if err := json.Unmarshal(bodyBytes, &env); err != nil {
		factor = 1.0
		adj = raw
		return
	}

	// We'll conservatively assume the logs transmit mostly as-is + some protocol overhead
	// and maybe one shared process/service context. No API key or tag bloat inferred from logs
	var totalLogBytes int64
	for _, rl := range env.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, logEntry := range sl.LogRecords {
				totalLogBytes += int64(len(logEntry))
				raws := json.RawMessage(logEntry)
				_ = raws // no parsing yet, just size
				rows++
			}
		}
	}

	adj = totalLogBytes
	if raw > 0 {
		factor = float64(adj) / float64(raw)
	} else {
		factor = 1.0
	}
	return
}

