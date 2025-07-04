package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// -----------------------------------------------------------------------------
// Debug / log-level plumbing
// -----------------------------------------------------------------------------

var debugEnabled bool

func debugf(format string, args ...interface{}) {
	if debugEnabled {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// -----------------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------------

var (
	upstreamURL        *url.URL
	authToken          string
	budgetBytes        int64
	budgetWindowType   string
	failOpenSampleRate float64

	rdb               *redis.Client
	client            *http.Client
	checkBudgetScript *redis.Script
	ctx               = context.Background()

	// Concurrency-safe random number generator for fail-open logic.
	rng      *rand.Rand
	rngMutex sync.Mutex
)

// -----------------------------------------------------------------------------
// Lua script: atomic budget check
// -----------------------------------------------------------------------------

const checkBudgetLua = `
local key = KEYS[1]
local debit_amount = tonumber(ARGV[1])
local budget = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])

-- Atomically check for key existence and set with initial value and TTL if it doesn't exist.
-- This prevents a race condition where multiple requests could set the key simultaneously.
if redis.call("EXISTS", key) == 0 then
    -- Set initial value to 0 with the specified TTL in milliseconds.
    -- The 'NX' option ensures this only happens if the key does not exist.
    redis.call("SET", key, 0, "PX", ttl_ms, "NX")
end

local current_usage = redis.call("INCRBY", key, debit_amount)

if current_usage > budget then
  -- If over budget, revert the increment and return 0 (denied).
  redis.call("DECRBY", key, debit_amount)
  return 0
end

-- Return 1 (allowed).
return 1
`

// -----------------------------------------------------------------------------
// init()
// -----------------------------------------------------------------------------

func init() {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug", "trace":
		debugEnabled = true
		log.Println("Log level: DEBUG")
	default:
		debugEnabled = false
	}

	ingestURLStr := os.Getenv("OTEL_INGEST_URL")
	if ingestURLStr == "" {
		log.Fatal("FATAL: OTEL_INGEST_URL not set")
	}
	var err error
	upstreamURL, err = url.Parse(ingestURLStr)
	if err != nil {
		log.Fatalf("FATAL: invalid OTEL_INGEST_URL: %v", err)
	}

	authToken = os.Getenv("OTEL_INGEST_TOKEN")
	if authToken == "" {
		log.Fatal("FATAL: OTEL_INGEST_TOKEN not set")
	}

	mbStr := os.Getenv("MAX_MEGABYTES_PER_WINDOW")
	if mbStr == "" {
		log.Fatal("FATAL: MAX_MEGABYTES_PER_WINDOW not set")
	}
	mb, err := strconv.ParseInt(mbStr, 10, 64)
	if err != nil {
		log.Fatalf("FATAL: invalid MAX_MEGABYTES_PER_WINDOW: %v", err)
	}
	budgetBytes = mb * 1000 * 1000 // decimal MB

	budgetWindowType = strings.ToLower(os.Getenv("BUDGET_WINDOW_TYPE"))
	if budgetWindowType != "hourly" && budgetWindowType != "daily" {
		budgetWindowType = "hourly"
		log.Println("Defaulting BUDGET_WINDOW_TYPE to 'hourly'")
	}

	if v := os.Getenv("FAIL_OPEN_SAMPLE_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			failOpenSampleRate = f
		}
	}

	redisURLStr := os.Getenv("REDIS_URL")
	if redisURLStr == "" {
		log.Fatal("FATAL: REDIS_URL not set")
	}
	opt, err := redis.ParseURL(redisURLStr)
	if err != nil {
		log.Fatalf("FATAL: invalid REDIS_URL: %v", err)
	}
	rdb = redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("FATAL: cannot connect to Redis: %v", err)
	}

	checkBudgetScript = redis.NewScript(checkBudgetLua)

	client = &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Initialize the concurrency-safe random number generator.
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))

	log.Printf("Proxy configured. Budget: %d bytes/%s. Upstream: %s", budgetBytes, budgetWindowType, upstreamURL.Host)
}

// -----------------------------------------------------------------------------
// main()
// -----------------------------------------------------------------------------

func main() {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:              ":4318",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	mux.HandleFunc("/_healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/v1/traces", handleRequest)
	mux.HandleFunc("/v1/logs", handleRequest)
	mux.HandleFunc("/v1/metrics", handleMetricsPassthrough)

	go func() {
		log.Println("Proxy listening on :4318")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("Shutdown signal received, gracefully shutting down...")
	ctxShut, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctxShut); err != nil {
		log.Fatalf("Server shutdown failed: %+v", err)
	}
	log.Println("Server exited properly")
}

// -----------------------------------------------------------------------------
// request handlers
// -----------------------------------------------------------------------------

func handleMetricsPassthrough(w http.ResponseWriter, r *http.Request) {
	upstreamStatus, err := forwardRequest(r, r.Body, r.ContentLength)
	if err != nil {
		log.Printf("ERROR: failed to forward metrics request: %v", err)
		http.Error(w, "Failed to forward request", http.StatusBadGateway)
		return
	}
	w.WriteHeader(upstreamStatus)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// 1. Content-Type Validation
	contentType := r.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "application/json") {
		http.Error(w, "Unsupported Content-Type: must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	if len(bodyBytes) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// 2. Large Body Guard: Skip estimator for very large payloads to prevent OOM.
	const maxBodyForEstimate = 15 * 1024 * 1024 // 15 MiB
	var adjSize int64
	if len(bodyBytes) > maxBodyForEstimate {
		log.Printf("WARN: Large body (%d bytes), skipping estimator. Billing raw size.", len(bodyBytes))
		// Fallback to billing raw compressed size + headers
		adjSize = int64(len(bodyBytes)) + 200
	} else {
		var jsonBytes []byte
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(bytes.NewReader(bodyBytes))
			if err != nil {
				http.Error(w, "failed to create gzip reader", http.StatusBadRequest)
				return
			}
			jsonBytes, err = io.ReadAll(zr)
			zr.Close() // Close the reader as soon as we are done with it.
			if err != nil {
				http.Error(w, "failed to decompress gzip body", http.StatusBadRequest)
				return
			}
		} else {
			jsonBytes = bodyBytes
		}
		// Estimate hydrated size from the uncompressed JSON.
		_, _, adjSize, _ = EstimateHydratedSize(jsonBytes)
	}

	// --- optimistic budget check ---
	key := "otel:budget:" + getWindowKey()
	ttl := getWindowTTL().Milliseconds()
	redisCheckPassed := false

	res, err := checkBudgetScript.Run(ctx, rdb, []string{key}, adjSize, budgetBytes, ttl).Result()
	if err != nil {
		// 3. Concurrency-Safe Fail-Open Logic
		rngMutex.Lock()
		shouldFailOpen := rng.Float64() < failOpenSampleRate
		rngMutex.Unlock()

		if failOpenSampleRate > 0 && shouldFailOpen {
			log.Printf("WARN: Redis unavailable, failing open for request. Error: %v", err)
			// Fallthrough to forward the request without budget check.
		} else {
			log.Printf("ERROR: Redis budget check failed: %v", err)
			http.Error(w, "error checking budget", http.StatusServiceUnavailable)
			return
		}
	} else {
		if allowed, _ := res.(int64); allowed == 1 {
			redisCheckPassed = true
		} else {
			http.Error(w, "Budget exceeded", http.StatusTooManyRequests)
			return
		}
	}

	// --- forward original (potentially compressed) request ---
	status, fwdErr := forwardRequest(r, bytes.NewReader(bodyBytes), int64(len(bodyBytes)))
	if fwdErr != nil || status >= 300 {
		if redisCheckPassed {
			_ = rdb.DecrBy(ctx, key, adjSize)
			debugf("refunded %d from %s due to forwarding error", adjSize, key)
		}

		if fwdErr != nil {
			http.Error(w, "failed to forward request", http.StatusBadGateway)
		} else {
			w.WriteHeader(status)
		}
		return
	}

	w.WriteHeader(status)
}

// -----------------------------------------------------------------------------
// helper fns
// -----------------------------------------------------------------------------

func getWindowKey() string {
	now := time.Now().UTC()
	if budgetWindowType == "daily" {
		return now.Format("2006-01-02")
	}
	return now.Format("2006-01-02T15") // Hourly key
}

func getWindowTTL() time.Duration {
	if budgetWindowType == "daily" {
		return 24*time.Hour + 5*time.Minute
	}
	return time.Hour + 5*time.Minute
}

func forwardRequest(orig *http.Request, body io.Reader, size int64) (int, error) {
	dest := upstreamURL.ResolveReference(orig.URL)
	req, err := http.NewRequestWithContext(orig.Context(), orig.Method, dest.String(), body)
	if err != nil {
		return 0, err
	}

	req.Header = orig.Header.Clone()
	// 4. Correct Host Header Handling: Let net/http set the Host from the request URL.
	req.Host = ""
	req.Header.Set("Authorization", authToken)

	if size > 0 {
		req.ContentLength = size
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		log.Printf("Upstream returned status %d: %s", resp.StatusCode, string(b))
	}

	return resp.StatusCode, nil
}

