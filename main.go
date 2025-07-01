package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
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

// HourlySchedule represents a single hour's budget configuration
type HourlySchedule struct {
	Hour             int     `json:"hour"`
	MegabytesPerHour int64   `json:"megabytes_per_hour"`
	TotalPercent     float64 `json:"total_percent"`
}

// ScheduleConfig represents the full schedule configuration
type ScheduleConfig struct {
	Schedule []HourlySchedule `json:"schedule"`
}

var (
	upstreamURL        *url.URL
	authToken          string
	budgetBytes        int64
	budgetWindowType   string
	failOpenSampleRate float64

	// Schedule-specific variables
	scheduleConfig     *ScheduleConfig
	dailyTotalBytes    int64
	hourlyBudgetBytes  [24]int64 // Pre-calculated budget for each hour

	rdb               *redis.Client
	client            *http.Client
	checkBudgetScript *redis.Script
	ctx               = context.Background()
)

// This Lua script is the core of the atomic budget check. It has been hardened
// to handle rare TTL race conditions after a Redis failover.
const checkBudgetLua = `
local current_usage = redis.call("INCRBY", KEYS[1], ARGV[1])
local budget = tonumber(ARGV[2])

if current_usage > budget then
  -- Budget exceeded, so refund the increment and return 0 (deny).
  redis.call("DECRBY", KEYS[1], ARGV[1])
  return 0
end

-- If the key's TTL is not set (e.g., after a failover), set it.
if redis.call("PTTL", KEYS[1]) < 0 then
  redis.call("PEXPIRE", KEYS[1], ARGV[3])
end

return 1
`

func init() {
	// Seed the random number generator for non-deterministic sampling.
	rand.Seed(time.Now().UnixNano())

	// --- Configuration Loading ---
	ingestURLStr := os.Getenv("OTEL_INGEST_URL")
	if ingestURLStr == "" {
		log.Fatal("FATAL: OTEL_INGEST_URL environment variable is not set.")
	}
	var err error
	upstreamURL, err = url.Parse(ingestURLStr)
	if err != nil {
		log.Fatalf("FATAL: Invalid OTEL_INGEST_URL: %v", err)
	}

	authToken = os.Getenv("OTEL_INGEST_TOKEN")
	if authToken == "" {
		log.Fatal("FATAL: OTEL_INGEST_TOKEN environment variable is not set.")
	}

	// Read budget in Megabytes for user-friendliness.
	budgetMegabytesStr := os.Getenv("MAX_MEGABYTES_PER_WINDOW")
	if budgetMegabytesStr == "" {
		log.Fatal("FATAL: MAX_MEGABYTES_PER_WINDOW environment variable is not set.")
	}
	budgetMegabytes, err := strconv.ParseInt(budgetMegabytesStr, 10, 64)
	if err != nil {
		log.Fatalf("FATAL: Invalid MAX_MEGABYTES_PER_WINDOW: %v", err)
	}
	// Convert megabytes to bytes for internal calculations.
	budgetBytes = budgetMegabytes * 1000 * 1000

	budgetWindowType = strings.ToLower(os.Getenv("BUDGET_WINDOW_TYPE"))
	if budgetWindowType != "hourly" && budgetWindowType != "daily" && budgetWindowType != "schedule" {
		budgetWindowType = "hourly"
	}

	// --- Schedule Configuration ---
	if budgetWindowType == "schedule" {
		if err := loadScheduleConfig(); err != nil {
			log.Fatalf("FATAL: Failed to load schedule configuration: %v", err)
		}
		if err := validateAndCalculateSchedule(); err != nil {
			log.Fatalf("FATAL: Schedule validation failed: %v", err)
		}
	}

	// --- Failure Strategy Configuration ---
	failSampleRateStr := os.Getenv("FAIL_OPEN_SAMPLE_RATE")
	if failSampleRateStr != "" {
		rate, err := strconv.ParseFloat(failSampleRateStr, 64)
		if err != nil || rate < 0 || rate > 1 {
			log.Printf("WARN: Invalid FAIL_OPEN_SAMPLE_RATE '%s'. Must be float between 0.0 and 1.0. Defaulting to 0.0 (fail closed).", failSampleRateStr)
			failOpenSampleRate = 0.0
		} else {
			failOpenSampleRate = rate
			log.Printf("Redis failure mode: fail open with sample rate %f", failOpenSampleRate)
		}
	} else {
		failOpenSampleRate = 0.0 // Default to fail closed
		log.Println("Redis failure mode: fail closed.")
	}

	// --- Redis Client Initialization ---
	redisURLStr := os.Getenv("REDIS_URL")
	if redisURLStr == "" {
		log.Fatal("FATAL: REDIS_URL environment variable is not set.")
	}

	parsedRedisURL, err := url.Parse(redisURLStr)
	if err != nil {
		log.Fatalf("FATAL: Invalid REDIS_URL: %v", err)
	}

	var tlsConfig *tls.Config
	if parsedRedisURL.Scheme == "rediss" {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	password, _ := parsedRedisURL.User.Password()
	rdb = redis.NewClient(&redis.Options{
		Addr:      parsedRedisURL.Host,
		Username:  parsedRedisURL.User.Username(),
		Password:  password,
		TLSConfig: tlsConfig,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("FATAL: Could not connect to Redis: %v", err)
	}
	log.Println("Successfully connected to Redis.")

	checkBudgetScript = redis.NewScript(checkBudgetLua)

	// --- HTTP Client Initialization ---
	client = &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 90 * time.Second,
		},
	}

	if budgetWindowType == "schedule" {
		log.Printf("Proxy configured with schedule budget. Upstream: %s", upstreamURL.Host)
	} else {
		log.Printf("Proxy configured. Budget: %d bytes per %s. Upstream: %s", budgetBytes, budgetWindowType, upstreamURL.Host)
	}
}

// loadScheduleConfig loads the schedule configuration from schedule.json
func loadScheduleConfig() error {
	data, err := os.ReadFile("schedule.json")
	if err != nil {
		return err
	}

	scheduleConfig = &ScheduleConfig{}
	if err := json.Unmarshal(data, scheduleConfig); err != nil {
		return err
	}

	if len(scheduleConfig.Schedule) != 24 {
		return fmt.Errorf("schedule must contain exactly 24 hours, got %d", len(scheduleConfig.Schedule))
	}

	// Verify all hours 0-23 are present
	hourMap := make(map[int]bool)
	for _, h := range scheduleConfig.Schedule {
		if h.Hour < 0 || h.Hour > 23 {
			return fmt.Errorf("invalid hour %d, must be 0-23", h.Hour)
		}
		if hourMap[h.Hour] {
			return fmt.Errorf("duplicate hour %d in schedule", h.Hour)
		}
		hourMap[h.Hour] = true
	}

	log.Println("Successfully loaded schedule configuration.")
	return nil
}

// validateAndCalculateSchedule validates the schedule and pre-calculates hourly budgets
func validateAndCalculateSchedule() error {
	// Calculate total percentage
	totalPercent := 0.0
	for _, h := range scheduleConfig.Schedule {
		totalPercent += h.TotalPercent
	}

	// Validate percentage range
	if totalPercent < 99.9 || totalPercent > 100.0 {
		return fmt.Errorf("total percentage %.2f%% is outside valid range (99.9%% - 100.0%%)", totalPercent)
	}

	log.Printf("Schedule validation passed. Total percentage: %.2f%%", totalPercent)

	// Check for MAX_TOTAL_BYTES_PER_DAY environment variable
	dailyBytesStr := os.Getenv("MAX_TOTAL_BYTES_PER_DAY")
	if dailyBytesStr != "" {
		dailyMegabytes, err := strconv.ParseInt(dailyBytesStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid MAX_TOTAL_BYTES_PER_DAY: %v", err)
		}
		dailyTotalBytes = dailyMegabytes * 1000 * 1000

		// Calculate hourly budgets based on percentages
		for _, h := range scheduleConfig.Schedule {
			percentage := h.TotalPercent / 100.0
			hourlyBudgetBytes[h.Hour] = int64(math.Round(float64(dailyTotalBytes) * percentage))
		}

		log.Printf("Using MAX_TOTAL_BYTES_PER_DAY: %d MB, budgets calculated from percentages", dailyMegabytes)
	} else {
		// Use the static megabytes_per_hour values
		for _, h := range scheduleConfig.Schedule {
			hourlyBudgetBytes[h.Hour] = h.MegabytesPerHour * 1000 * 1000
		}

		log.Println("Using static megabytes_per_hour values from schedule")
	}

	// Log the calculated hourly budgets
	for i := 0; i < 24; i++ {
		log.Printf("Hour %02d: %d MB budget", i, hourlyBudgetBytes[i]/(1000*1000))
	}

	return nil
}

func main() {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:              ":4318",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	mux.HandleFunc("/_healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/v1/traces", handleRequest)
	mux.HandleFunc("/v1/logs", handleRequest)
	mux.HandleFunc("/v1/metrics", handleRequest)

	go func() {
		log.Println("Starting proxy server on :4318...")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v\n", server.Addr, err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
	log.Println("Server exited properly")
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	requestSize := r.ContentLength
	var bodyReader io.Reader = r.Body

	if requestSize <= 0 {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("ERROR: Failed to read request body: %v", err)
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		requestSize = int64(len(bodyBytes))
		bodyReader = bytes.NewReader(bodyBytes)
	}

	if requestSize == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	budgetKey := "otel:budget:" + getWindowKey()
	windowTTLMillis := getWindowTTL().Milliseconds()
	currentBudget := getCurrentBudget()

	res, err := checkBudgetScript.Run(ctx, rdb, []string{budgetKey}, requestSize, currentBudget, windowTTLMillis).Result()
	if err != nil {
		log.Printf("CRITICAL: Redis script failed: %v. Executing fail-over strategy.", err)
		if failOpenSampleRate > 0 && rand.Float64() < failOpenSampleRate {
			log.Printf("Failing open with sample rate %f. Forwarding request.", failOpenSampleRate)
			if _, err := forwardRequest(r, bodyReader, requestSize); err != nil {
				http.Error(w, "Failed to forward request", http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusAccepted)
			}
		} else {
			log.Println("Failing closed. Dropping request.")
			http.Error(w, "Rate limit backend unavailable; request dropped.", http.StatusServiceUnavailable)
		}
		return
	}

	if isAllowed, ok := res.(int64); !ok || isAllowed == 0 {
		log.Printf("WARN: Budget exceeded. Dropping %d bytes.", requestSize)
		emitDropMetric(r, int(requestSize))
		http.Error(w, "Data budget exceeded", http.StatusTooManyRequests)
		return
	}

	if upstreamStatus, err := forwardRequest(r, bodyReader, requestSize); err != nil {
		http.Error(w, "Failed to forward request", http.StatusInternalServerError)
	} else {
		w.WriteHeader(upstreamStatus)
	}
}

// getCurrentBudget returns the budget for the current window
func getCurrentBudget() int64 {
	if budgetWindowType == "schedule" {
		hour := time.Now().UTC().Hour()
		return hourlyBudgetBytes[hour]
	}
	return budgetBytes
}

func getWindowKey() string {
	now := time.Now().UTC()
	if budgetWindowType == "daily" {
		return now.Format("2006-01-02")
	}
	// For both "hourly" and "schedule" types, use hourly keys
	return now.Format("2006-01-02T15")
}

func getWindowTTL() time.Duration {
	if budgetWindowType == "daily" {
		return 24*time.Hour + 5*time.Minute
	}
	// For both "hourly" and "schedule" types, use hourly TTL
	return time.Hour + 5*time.Minute
}

func forwardRequest(originalReq *http.Request, body io.Reader, requestSize int64) (int, error) {
	destURL := upstreamURL.ResolveReference(originalReq.URL)

	req, err := http.NewRequestWithContext(originalReq.Context(), originalReq.Method, destURL.String(), body)
	if err != nil {
		log.Printf("ERROR: Failed to create upstream request: %v", err)
		return 0, err
	}

	req.Header = originalReq.Header.Clone()
	req.Header.Del("Authorization")
	req.Header.Set("Authorization", strings.TrimSpace(authToken))

	if requestSize > 0 {
		req.ContentLength = requestSize
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("ERROR: Failed to forward request to upstream: %v", err)
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("WARN: Upstream responded with status %d for %s: %s", resp.StatusCode, destURL.Path, string(respBody))
	}

	return resp.StatusCode, nil
}

func emitDropMetric(originalReq *http.Request, bytesDropped int) {
	metric := map[string]interface{}{
		"name": "otel_proxy.ingest_budget.dropped_bytes", "unit": "By", "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "value": bytesDropped,
		"attributes": map[string]string{"reason": "budget_exceeded"},
	}
	payload := map[string]interface{}{
		"resourceMetrics": []map[string]interface{}{{"scopeMetrics": []map[string]interface{}{{"metrics": []map[string]interface{}{metric}}}}},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("ERROR: Failed to marshal drop metric: %v", err)
		return
	}

	metricReq, err := http.NewRequestWithContext(originalReq.Context(), "POST", "/v1/metrics", nil)
	if err != nil {
		log.Printf("ERROR: Could not create metric request: %v", err)
		return
	}
	metricReq.Header.Set("Content-Type", "application/json")

	forwardRequest(metricReq, bytes.NewReader(data), int64(len(data)))
}

