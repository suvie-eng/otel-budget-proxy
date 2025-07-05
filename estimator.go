package main

import (
	"encoding/json"
	"fmt"
	"os"
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

type JaegerSpan struct {
	TraceID       string            `json:"traceID"`
	SpanID        string            `json:"spanID"`
	OperationName string            `json:"operationName"`
	Process       JaegerProcess     `json:"process"`
	Tags          map[string]string `json:"tags"`
	JaegerTag     map[string]string `json:"JaegerTag"`
	StartTime     int64             `json:"startTime"`
	StartTimeMs   int64             `json:"startTimeMillis"`
	Timestamp     int64             `json:"timestamp"`
	Duration      int64             `json:"duration"`
	Type          string            `json:"type"`
	Logs          []any             `json:"logs"`
	References    []any             `json:"references"`
}

type JaegerProcess struct {
	ServiceName string            `json:"serviceName"`
	Tag         map[string]string `json:"tag"`
	Tags        []any             `json:"tags"`
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

type ResourceSpans struct {
	Resource struct {
		Attributes json.RawMessage `json:"attributes"`
	} `json:"resource"`
	ScopeSpans []ScopeSpan `json:"scopeSpans"`
}

func EstimateHydratedSize(bodyBytes []byte) (int64, float64, int64, int) {
	var env struct {
		ResourceSpans []ResourceSpans `json:"resourceSpans"`
	}
	if err := json.Unmarshal(bodyBytes, &env); err != nil {
		return int64(len(bodyBytes)), 1.0, int64(len(bodyBytes)), 0
	}

	var allSpans []JaegerSpan

	for _, rs := range env.ResourceSpans {
		resAttrs := parseAttributes(rs.Resource.Attributes)
		serviceName := resAttrs["service.name"]
		delete(resAttrs, "service.name")

		for _, ss := range rs.ScopeSpans {
			scopeAttrs := parseAttributes(ss.Scope.Attrs)

			for _, span := range ss.Spans {
				spanAttrs := parseAttributes(span.Attributes)

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
				jaegerTag["__HDX_API_KEY"] = "d3f19c25-c4c6-40de-968a-a2a8407eec70"

				now := time.Now().UnixMilli()
				start := now
				duration := int64(500)

				allSpans = append(allSpans, JaegerSpan{
					TraceID:       span.TraceID,
					SpanID:        span.SpanID,
					OperationName: span.Name,
					Tags:          spanAttrs,
					JaegerTag:     jaegerTag,
					Process: JaegerProcess{
						ServiceName: serviceName,
						Tag:         mergeMaps(resAttrs, scopeAttrs),
						Tags:        []any{},
					},
					StartTime:   start * int64(time.Millisecond),
					StartTimeMs: start,
					Timestamp:   start,
					Duration:    duration,
					Type:        "jaegerSpan",
					Logs:        []any{},
					References:  []any{},
				})
			}
		}
	}

	var total int64
	for _, s := range allSpans {
		b, _ := json.Marshal(s)
		total += int64(len(b))
	}

	raw := int64(len(bodyBytes))
	factor := float64(total) / float64(raw)
	return raw, factor, total, len(allSpans)
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
		return "unknown"
	}
}

func parseAttributes(raw json.RawMessage) map[string]string {
	out := map[string]string{}
	if len(raw) <= 2 {
		return out
	}

	var verboseAttrs []OtelAttribute
	if err := json.Unmarshal(raw, &verboseAttrs); err != nil {
		return out
	}

	for _, attr := range verboseAttrs {
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

