package main

import (
	"bytes"
	"compress/gzip" // Import the gzip package
	"context"
	"crypto/tls"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// -----------------------------------------------------------------------------
// Debug / log-level plumbing ---------------------------------------------------
// -----------------------------------------------------------------------------

var debugEnabled bool

func debugf(format string, args ...interface{}) {
	if debugEnabled {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// -----------------------------------------------------------------------------
// Globals ---------------------------------------------------------------------
// -----------------------------------------------------------------------------

var (
	upstreamURL      *url.URL
	authToken        string
	budgetBytes      int64
	budgetWindowType string
	failOpenSampleRate float64

	rdb               *redis.Client
	client            *http.Client
	checkBudgetScript *redis.Script
	ctx               = context.Background()
)

// -----------------------------------------------------------------------------
// Lua script: atomic budget check ---------------------------------------------
// -----------------------------------------------------------------------------

const checkBudgetLua = `
local current_usage = redis.call("INCRBY", KEYS[1], ARGV[1])
local budget = tonumber(ARGV[2])

if current_usage > budget then
  redis.call("DECRBY", KEYS[1], ARGV[1])
  return 0
end

if redis.call("PTTL", KEYS[1]) < 0 then
  redis.call("PEXPIRE", KEYS[1], ARGV[3])
end

return 1
`

// -----------------------------------------------------------------------------
// init() -----------------------------------------------------------------------
// -----------------------------------------------------------------------------

func init() {
	rand.Seed(time.Now().UnixNano())

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
	redisURL, err := url.Parse(redisURLStr)
	if err != nil {
		log.Fatalf("FATAL: invalid REDIS_URL: %v", err)
	}
	var tlsCfg *tls.Config
	if redisURL.Scheme == "rediss" {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	pwd, _ := redisURL.User.Password()
	rdb = redis.NewClient(&redis.Options{
		Addr:      redisURL.Host,
		Username:  redisURL.User.Username(),
		Password:  pwd,
		TLSConfig: tlsCfg,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("FATAL: cannot connect Redis: %v", err)
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

	log.Printf("Proxy configured. Budget: %d bytes/%s. Upstream: %s", budgetBytes, budgetWindowType, upstreamURL.Host)
}

// -----------------------------------------------------------------------------
// main() -----------------------------------------------------------------------
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

	// --- Handler Routing ---
	// Budgeting is applied to logs and traces.
	mux.HandleFunc("/v1/traces", handleRequest)
	mux.HandleFunc("/v1/logs", handleRequest)
	// Metrics are passed through without budgeting.
	mux.HandleFunc("/v1/metrics", handleMetricsPassthrough)

	go func() {
		log.Println("Proxy listening on :4318")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	// graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctxShut, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server.Shutdown(ctxShut)
}

// handleMetricsPassthrough forwards requests without any budget check.
func handleMetricsPassthrough(w http.ResponseWriter, r *http.Request) {
	// This logic can be simplified as we don't need the body for any checks here.
	if upstreamStatus, err := forwardRequest(r, r.Body, r.ContentLength); err != nil {
		http.Error(w, "Failed to forward request", http.StatusInternalServerError)
	} else {
		w.WriteHeader(upstreamStatus)
	}
}

// -----------------------------------------------------------------------------
// request handler (for logs and traces) ---------------------------------------
// -----------------------------------------------------------------------------

func handleRequest(w http.ResponseWriter, r *http.Request) {
	// Enforce a max request size to prevent a bad actor from sending a massive
	// payload that could exhaust memory when buffered. 8 MiB is a reasonable limit.
	// This will automatically respond with a 413 "Request Entity Too Large" if exceeded.
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20) // 8 MiB limit

	defer r.Body.Close()

	// --- 1. Buffer the entire request body into memory ---
	// This is a trade-off: it increases memory usage in exchange for perfect
	// budget accuracy. The proxy must hold the entire compressed request
	// in memory to both calculate its uncompressed size and forward the
	// original compressed body.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		// If the error is from MaxBytesReader, a 413 response has already been sent.
		// Otherwise, it's a different read error.
		log.Printf("read body: %v", err)
		// No need to write an error header if one was already sent by MaxBytesReader.
		// We can check for this, but for simplicity, we'll just log and return.
		return
	}

	requestSize := int64(len(bodyBytes))
	if requestSize == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// --- 2. Determine the size for budgeting (uncompressed size) ---
	sizeForBudgeting := requestSize
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf("invalid gzip: %v", err)
			http.Error(w, "bad gzip", http.StatusBadRequest)
			return
		}
		defer zr.Close()
		if n, err := io.Copy(io.Discard, zr); err == nil {
			sizeForBudgeting = n
		} else {
			log.Printf("gun-zip copy: %v", err)
			http.Error(w, "decompress", http.StatusInternalServerError)
			return
		}
	}

	rawSize := sizeForBudgeting

	const (
		rMax = 2.6
		s    = 60_000.0
	)
	factor   := 1 + (rMax-1)*(1-math.Exp(-float64(rawSize)/s))
	adjSize  := int64(float64(rawSize) * factor)   // proper float→int

	debugf("budget bytes: raw=%d  factor=%.3f  adjusted=%d",
       rawSize, factor, adjSize)

	sizeForBudgeting = adjSize   // now use this for the Redis check

	// --- 3. Optimistic budget check (INCRBY inside Lua) ---
	key := "otel:budget:" + getWindowKey()
	ttl := getWindowTTL().Milliseconds()

	// Use the uncompressed size for the budget check
	res, err := checkBudgetScript.Run(
		ctx, rdb, []string{key}, sizeForBudgeting, budgetBytes, ttl,
	).Result()
	if err != nil {
		log.Printf("Redis err: %v", err)
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}
	allowed, _ := res.(int64)

	if debugEnabled {
		usage, _ := rdb.Get(ctx, key).Int64()
		debugf("post-check key=%s usage=%d size=%d budget=%d allowed=%v",
			key, usage, sizeForBudgeting, budgetBytes, allowed == 1)
	}

	if allowed == 0 {
		http.Error(w, "Budget exceeded", http.StatusTooManyRequests)
		return
	}

	// --- 4. Forward the original, compressed request ---
	// Create a new reader from the buffered bytes.
	bodyReader := bytes.NewReader(bodyBytes)
	status, fwdErr := forwardRequest(r, bodyReader, requestSize)

	// --- 5. If upstream failed → refund budget and surface error ---
	// The refund must use the same size that was debited.
	if fwdErr != nil || status >= 300 {
		if derr := rdb.DecrBy(ctx, key, sizeForBudgeting).Err(); derr != nil {
			log.Printf("refund failed: %v (key=%s size=%d)", derr, key, sizeForBudgeting)
		}
		if fwdErr != nil {
			http.Error(w, "forward error", http.StatusBadGateway)
		} else {
			w.WriteHeader(status) // bubble 4xx/5xx so caller sees it
		}
		return
	}

	// --- 6. Success path ---
	w.WriteHeader(status) // usually 202 Accepted
}


// -----------------------------------------------------------------------------
// helper fns -------------------------------------------------------------------
// -----------------------------------------------------------------------------

func getWindowKey() string {
	now := time.Now().UTC()
	if budgetWindowType == "daily" {
		return now.Format("2006-01-02")
	}
	return now.Format("2006-01-02T15")
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
	req.Header.Del("Authorization")
	req.Header.Set("Authorization", strings.TrimSpace(authToken))
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
		log.Printf("upstream %d: %s", resp.StatusCode, string(b))
	}

	return resp.StatusCode, nil
}
