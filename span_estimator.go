package main

import (
	"encoding/json"
	"strconv"
	"time"
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

type ResourceSpan struct {
	Resource struct {
		Attributes json.RawMessage `json:"attributes"`
	} `json:"resource"`
	ScopeSpans []ScopeSpan `json:"scopeSpans"`
}

type ScopeSpan struct {
	Scope struct {
		Name    string          `json:"name"`
		Version string          `json:"version"`
		Attrs   json.RawMessage `json:"attributes"`
	} `json:"scope"`
	Spans []struct {
		TraceID    string          `json:"traceId"`
		SpanID     string          `json:"spanId"`
		Name       string          `json:"name"`
		Kind       int             `json:"kind"`
		Attributes json.RawMessage `json:"attributes"`
	} `json:"spans"`
}

func EstimateSpans(raw json.RawMessage) (int64, int) {
	var rs ResourceSpan
	if err := json.Unmarshal(raw, &rs); err != nil {
		return 0, 0
	}

	resAttrs := parseAttributes(rs.Resource.Attributes)

	total := int64(0)
	count := 0

	for _, ss := range rs.ScopeSpans {
		scopeAttrs := parseAttributes(ss.Scope.Attrs)

		for _, span := range ss.Spans {
			count++
			spanAttrs := parseAttributes(span.Attributes)

			// Pull JaegerTags
			jaegerTag := map[string]string{
				"otel.library.name":    ss.Scope.Name,
				"otel.library.version": ss.Scope.Version,
				"span.kind":            kindToString(span.Kind),
			}
			for _, k := range []string{"deployment.environment.name", "net.peer.name", "net.peer.port"} {
				if v, ok := spanAttrs[k]; ok {
					jaegerTag[k] = v
					delete(spanAttrs, k)
				}
			}

			jaegerBytes, _ := json.Marshal(struct {
				TraceID       string            `json:"traceID"`
				SpanID        string            `json:"spanID"`
				OperationName string            `json:"operationName"`
				Process       struct {
					ServiceName string            `json:"serviceName"`
					Tag         map[string]string `json:"tag"`
					Tags        []any             `json:"tags"`
				} `json:"process"`
				Tags      map[string]string `json:"tags"`
				JaegerTag map[string]string `json:"JaegerTag"`
				StartTime int64             `json:"startTime"`
				StartMs   int64             `json:"startTimeMillis"`
				Timestamp int64             `json:"timestamp"`
				Duration  int64             `json:"duration"`
				Type      string            `json:"type"`
				Logs      []any             `json:"logs"`
				References []any            `json:"references"`
			}{
				TraceID:       span.TraceID,
				SpanID:        span.SpanID,
				OperationName: span.Name,
				Process: struct {
					ServiceName string            `json:"serviceName"`
					Tag         map[string]string `json:"tag"`
					Tags        []any             `json:"tags"`
				}{
					ServiceName: resAttrs["service.name"],
					Tag:         mergeMaps(resAttrs, scopeAttrs),
					Tags:        []any{},
				},
				Tags:      spanAttrs,
				JaegerTag: jaegerTag,
				StartTime: time.Now().UnixMilli() * int64(time.Millisecond),
				StartMs:   time.Now().UnixMilli(),
				Timestamp: time.Now().UnixMilli(),
				Duration:  500,
				Type:      "jaegerSpan",
				Logs:      []any{},
				References: []any{},
			})

			total += int64(len(jaegerBytes)) + 55 // Static bytes for __HDX_API_KEY header
		}
	}
	return total, count
}

func parseAttributes(raw json.RawMessage) map[string]string {
	out := map[string]string{}
	if len(raw) <= 2 {
		return out
	}

	var attrs []OtelAttribute
	if err := json.Unmarshal(raw, &attrs); err != nil {
		return out
	}

	for _, attr := range attrs {
		if attr.Value.StringValue != nil {
			out[attr.Key] = *attr.Value.StringValue
		} else if attr.Value.IntValue != nil {
			out[attr.Key] = *attr.Value.IntValue
		} else if attr.Value.BoolValue != nil {
			out[attr.Key] = strconv.FormatBool(*attr.Value.BoolValue)
		} else if attr.Value.DoubleValue != nil {
			out[attr.Key] = strconv.FormatFloat(*attr.Value.DoubleValue, 'f', -1, 64)
		}
	}
	return out
}

func mergeMaps(m1, m2 map[string]string) map[string]string {
	out := make(map[string]string, len(m1)+len(m2))
	for k, v := range m1 {
		out[k] = v
	}
	for k, v := range m2 {
		out[k] = v
	}
	return out
}

func marshalTagMap(m map[string]string) []byte {
	b, _ := json.Marshal(m)
	return b
}

func kindToString(kind int) string {
	switch kind {
	case 1:
		return "internal"
	case 2:
		return "server"
	case 3:
		return "client"
	case 4:
		return "producer"
	case 5:
		return "consumer"
	default:
		return "unspecified"
	}
}

