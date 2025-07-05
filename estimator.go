package main

import (
	"encoding/json"
	"strconv"
)

type OtelValue struct {
	StringValue *string  `json:"stringValue"`
	IntValue    *string  `json:"intValue"`
	BoolValue   *bool    `json:"boolValue"`
	DoubleValue *float64 `json:"doubleValue"`
}

type OtelAttribute struct {
	Key   string    `json:"key"`
	Value OtelValue `json:"value"`
}

func getCompactAttributeSize(rawAttrs json.RawMessage) int {
	if len(rawAttrs) <= 2 {
		return 0
	}

	var verboseAttrs []OtelAttribute
	if err := json.Unmarshal(rawAttrs, &verboseAttrs); err != nil {
		return 0
	}

	if len(verboseAttrs) == 0 {
		return 0
	}

	totalBytes := 2 // for enclosing braces {}
	for i, attr := range verboseAttrs {
		totalBytes += len(attr.Key) + 3 // "key":

		if attr.Value.StringValue != nil {
			totalBytes += len(*attr.Value.StringValue) + 2
		} else if attr.Value.IntValue != nil {
			totalBytes += len(*attr.Value.IntValue)
		} else if attr.Value.BoolValue != nil {
			if *attr.Value.BoolValue {
				totalBytes += 4 // true
			} else {
				totalBytes += 5 // false
			}
		} else if attr.Value.DoubleValue != nil {
			totalBytes += len(strconv.FormatFloat(*attr.Value.DoubleValue, 'f', -1, 64))
		}

		if i < len(verboseAttrs)-1 {
			totalBytes += 1 // comma
		}
	}
	return totalBytes
}

func EstimateHydratedSize(bodyBytes []byte) (raw int64, factor float64, adj int64, rows int) {
	raw = int64(len(bodyBytes))

	type scopeSpans struct {
		Scope struct {
			Name       string          `json:"name"`
			Version    string          `json:"version"`
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
		adj = raw
		return
	}

	var dupBytes int64
	bodiesOnlySize := raw

	for _, rs := range env.ResourceSpans {
		resAttrBlockSize := len(rs.Resource.Attributes)
		resAttrContentSize := getCompactAttributeSize(rs.Resource.Attributes)

		for _, ss := range rs.ScopeSpans {
			scopeAttrBlockSize := len(ss.Scope.Attributes)
			scopeAttrContentSize := getCompactAttributeSize(ss.Scope.Attributes)

			rowCount := len(ss.Spans) + len(ss.Logs)
			rows += rowCount

			if rowCount > 0 {
				bodiesOnlySize -= int64(resAttrBlockSize + scopeAttrBlockSize)
				perRowOverhead := int64(resAttrContentSize + scopeAttrContentSize)
				dupBytes += perRowOverhead * int64(rowCount)
			}
		}
	}

	staticOverhead := int64(55) // For __HDX_API_KEY
	adj = bodiesOnlySize + dupBytes + staticOverhead

	if raw > 0 {
		factor = float64(adj) / float64(raw)
	} else {
		factor = 1.0
	}
	return
}

