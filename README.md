# OTEL Budget Proxy

A lightweight, high-performance reverse proxy for OpenTelemetry (OTLP) traffic that enforces a fixed data budget. It is designed to be a simple, reliable safeguard to prevent runaway observability costs from unexpected spikes in telemetry data.

The proxy sits between your services (or OTLP collectors) and your observability backend (e.g., HyperDX, Honeycomb, etc.).

### Features

-   **Atomic Budget Enforcement:** Uses a Redis Lua script to atomically track usage, preventing race conditions and ensuring your budget is never exceeded.
-   **Configurable Time Windows:** Enforce budgets on an `hourly` or `daily` basis.
-   **Resilient & Safe by Default:** Defaults to a "fail-closed" strategy. If the Redis backend is unavailable, it drops traffic to protect your budget.
-   **Configurable Failure Mode:** Can be configured to "fail-open" with a specific sample rate during a Redis outage, maintaining partial observability.
-   **Production Ready:** Includes `/healthz` and `/metrics` (Prometheus) endpoints for robust monitoring and orchestration.
-   **Graceful Shutdown:** Handles `SIGINT` and `SIGTERM` for zero-downtime deployments in environments like Kubernetes.
-   **Transparent:** Correctly propagates upstream paths, query parameters, and status codes.

### Use Cases

-   Set a hard cap on observability spending with vendors like HyperDX, SigNoz, Honeycomb, etc.
-   Protect against bugs or deployment issues that cause a flood of telemetry data (e.g., worker fan-outs, infinite loops).
-   Enforce a simple, global budget without the complexity of head-based or tail-based sampling.

### Configuration

The proxy is configured entirely through environment variables.

| Variable                  | Description                                                                                             | Example                                             |
| ------------------------- | ------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| `OTEL_INGEST_URL`         | Full URL of the upstream OTLP endpoint.                                                                 | `https://in-us.hyperdx.io/v1/traces`                |
| `OTEL_INGEST_TOKEN`       | The secret authorization token for the upstream endpoint.                                               | `abc-123-def-456`                                   |
| `REDIS_ADDR`              | Full Redis URL for budget tracking. Use `rediss://` for TLS.                                            | `redis://localhost:6379` or `rediss://:pass@host:port` |
| `BUDGET_WINDOW_TYPE`      | The window for the budget. Can be `hourly` or `daily`.                                                  | `daily`                                             |
| `MAX_BYTES_PER_WINDOW`    | Max allowed bytes per window in plain integers.                                                         | `1073741824` (for 1 GiB)                            |
| `FAIL_OPEN_SAMPLE_RATE`   | **(Optional)** A float between `0.0` and `1.0`. If set, the proxy will forward this percentage of traffic if Redis is down. **Defaults to `0.0` (fail-closed).** | `0.05` (for 5% sampling)                            |

### Monitoring

The proxy exposes two standard monitoring endpoints:

-   **Health Checks:** `GET /_healthz`
    -   Returns `HTTP 200 OK` when the server is running. Used for liveness/readiness probes.
-   **Metrics:** `GET /metrics`
    -   Exposes detailed Go runtime metrics in Prometheus format. Useful for monitoring memory usage, goroutines, and GC performance.

### Example (Docker)

```bash
# This example runs the proxy with a 100MB hourly budget.
# It will fail closed if it cannot reach Redis.

docker run -p 4318:4318 \
  -e OTEL_INGEST_URL="[https://in-us.hyperdx.io/v1/traces](https://in-us.hyperdx.io/v1/traces)" \
  -e OTEL_INGEST_TOKEN="your-secret-token" \
  -e REDIS_ADDR="redis://your-redis-host:6379" \
  -e BUDGET_WINDOW_TYPE="hourly" \
  -e MAX_BYTES_PER_WINDOW="100000000" \
  suvie-eng/otel-budget-proxy
