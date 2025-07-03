package main

import (
    "bytes"
    "context"
    "crypto/tls"
    "io"
    "log"
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
    upstreamURL        *url.URL
    authToken          string
    budgetBytes        int64
    budgetWindowType   string
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

    mux.HandleFunc("/v1/traces", handleRequest)
    mux.HandleFunc("/v1/logs", handleRequest)
    mux.HandleFunc("/v1/metrics", handleRequest)

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

// -----------------------------------------------------------------------------
// request handler --------------------------------------------------------------
// -----------------------------------------------------------------------------

func handleRequest(w http.ResponseWriter, r *http.Request) {
    defer r.Body.Close()

    requestSize := r.ContentLength
    var bodyBytes []byte
    var err error

    if requestSize <= 0 {
        bodyBytes, err = io.ReadAll(r.Body)
        if err != nil {
            log.Printf("read body: %v", err)
            http.Error(w, "read error", http.StatusInternalServerError)
            return
        }
        requestSize = int64(len(bodyBytes))
    }

    if requestSize == 0 {
        w.WriteHeader(http.StatusAccepted)
        return
    }

    // budget key + TTL
    key := "otel:budget:" + getWindowKey()
    ttl := getWindowTTL().Milliseconds()

    // atomic budget check
    res, err := checkBudgetScript.Run(ctx, rdb, []string{key}, requestSize, budgetBytes, ttl).Result()
    if err != nil {
        log.Printf("Redis err: %v", err)
        http.Error(w, "Redis", http.StatusServiceUnavailable)
        return
    }
    allowed, _ := res.(int64)
    // fetch updated usage for debug visibility
    usage, _ := rdb.Get(ctx, key).Int64()
    debugf("post-check key=%s usage=%d size=%d budget=%d allowed=%v", key, usage, requestSize, budgetBytes, allowed == 1)
    if allowed == 0 {
        // emitDropMetric(r, int(requestSize))
        http.Error(w, "Budget exceeded", http.StatusTooManyRequests)
        return
    }

    // prepare body reader
    var bodyReader io.Reader
    if len(bodyBytes) > 0 {
        bodyReader = bytes.NewReader(bodyBytes)
    } else {
        bodyReader = r.Body
    }

    status, err := forwardRequest(r, bodyReader, requestSize)
    if err != nil {
        http.Error(w, "forward", http.StatusBadGateway)
        return
    }
    w.WriteHeader(status)
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

// -----------------------------------------------------------------------------
// metrics ---------------------------------------------------------------------
// -----------------------------------------------------------------------------

// func emitDropMetric(orig *http.Request, dropped int) {
//     metric := map[string]interface{}{
//         "name":  "otel_proxy.ingest_budget.dropped_bytes",
//         "unit":  "By",
//         "value": dropped,
//         "timestamp": time.Now().UTC().Format(time.RFC3339Nano),
//         "attributes": map[string]string{"reason": "budget_exceeded"},
//     }
//     payload := map[string]interface{}{
//         "resourceMetrics": []interface{}{map[string]interface{}{
//             "scopeMetrics": []interface{}{map[string]interface{}{
//                 "metrics": []interface{}{metric},
//             }},
//         }},
//     }
//     data, _ := json.Marshal(payload)
//
//     // forward metric asynchronously (fire and forget)
//     go func() {
//         req, _ := http.NewRequestWithContext(orig.Context(), "POST", "/v1/metrics", bytes.NewReader(data))
//         req.Header.Set("Content-Type", "application/json")
//         forwardRequest(orig, req.Body, int64(len(data)))
//     }()
// }
//
