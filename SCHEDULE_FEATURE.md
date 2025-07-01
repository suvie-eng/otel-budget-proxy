# Schedule Budget Window Type

The OpenTelemetry Budget Proxy now supports a new budget window type called "schedule" that allows you to set variable hourly limits throughout the day to better accommodate traffic patterns.

## Configuration

### Environment Variables

To use the schedule budget type, set the following environment variable:

```bash
export BUDGET_WINDOW_TYPE=schedule
```

### Schedule Configuration File

The schedule configuration must be defined in a `schedule.json` file in the same directory as the proxy binary. The file should contain a JSON object with a "schedule" array containing exactly 24 entries (one for each hour of the day).

#### Example `schedule.json`:

```json
{
  "schedule": [
    {
      "hour": 0,
      "megabytes_per_hour": 50,
      "total_percent": 2.1
    },
    {
      "hour": 1,
      "megabytes_per_hour": 30,
      "total_percent": 1.2
    }
    // ... continue for all 24 hours
  ]
}
```

### Configuration Fields

Each hourly schedule entry contains:

- **`hour`** (int): The hour of the day (0-23, using UTC time)
- **`megabytes_per_hour`** (int): The number of megabytes allowed during this hour (used when `MAX_TOTAL_BYTES_PER_DAY` is not set)
- **`total_percent`** (float): The percentage of the total daily budget this hour should receive (used when `MAX_TOTAL_BYTES_PER_DAY` is set)

### Budget Calculation Modes

The proxy supports two modes for calculating hourly budgets:

#### 1. Static Mode (Default)

When `MAX_TOTAL_BYTES_PER_DAY` is not set, the proxy uses the `megabytes_per_hour` values directly from the schedule configuration.

#### 2. Percentage Mode

When `MAX_TOTAL_BYTES_PER_DAY` is set, the proxy calculates each hour's budget based on the `total_percent` values:

```bash
export MAX_TOTAL_BYTES_PER_DAY=10000  # Total daily budget in megabytes
```

In this mode, each hour's budget is calculated as:

```
hourly_budget = (total_daily_budget * total_percent) / 100
```

### Validation

At startup, the proxy validates that:

1. The `schedule.json` file exists and is valid JSON
2. Exactly 24 hours are defined (0-23)
3. No duplicate hours exist
4. All `total_percent` values sum to between 99.9% and 100.0%

If validation fails, the proxy will exit with a fatal error.

## Usage Examples

### Example 1: Static Hourly Limits

```bash
export BUDGET_WINDOW_TYPE=schedule
export OTEL_INGEST_URL=https://your-otel-endpoint.com
export OTEL_INGEST_TOKEN=your-token
export REDIS_URL=redis://localhost:6379
# Do not set MAX_TOTAL_BYTES_PER_DAY

./otel-budget-proxy
```

This will use the `megabytes_per_hour` values directly from `schedule.json`.

### Example 2: Percentage-Based Daily Budget

```bash
export BUDGET_WINDOW_TYPE=schedule
export MAX_TOTAL_BYTES_PER_DAY=10000  # 10GB total daily budget
export OTEL_INGEST_URL=https://your-otel-endpoint.com
export OTEL_INGEST_TOKEN=your-token
export REDIS_URL=redis://localhost:6379

./otel-budget-proxy
```

This will calculate hourly budgets based on the `total_percent` values and the total daily budget.

## Traffic Pattern Design

The provided `schedule.json` is designed for US timezone traffic patterns with:

- **Low traffic hours** (12 AM - 6 AM): 0.8% - 2.1% of daily budget
- **Business hours** (9 AM - 5 PM): 5.0% - 7.5% of daily budget (peak at 3 PM)
- **Evening hours** (6 PM - 11 PM): 2.3% - 4.6% of daily budget

You can modify the schedule to match your specific traffic patterns.

## Monitoring

When using schedule mode, the proxy logs:

- Successful schedule loading and validation
- Total percentage validation results
- Calculated hourly budgets for each hour
- Budget mode (static vs. percentage-based)

Example log output:

```
Successfully loaded schedule configuration.
Schedule validation passed. Total percentage: 100.60%
Using MAX_TOTAL_BYTES_PER_DAY: 10000 MB, budgets calculated from percentages
Hour 00: 210 MB budget
Hour 01: 120 MB budget
...
Proxy configured with schedule budget. Upstream: your-endpoint.com
```
