# Stock Market ETL Pipeline on AWS

![tests](https://github.com/ella-qianai/stock-pipeline/actions/workflows/tests.yml/badge.svg)

English | [中文](README.zh-CN.md)

A fully automated, scheduled ETL pipeline: it calls a third-party REST API daily, stages the raw response in S3, validates and transforms it, and loads it into a PostgreSQL data warehouse — with retry logic, per-record failure isolation, and a standalone data-quality check on top.

5 stocks, one API provider across three endpoints. The failure modes it's built to handle are listed below.

---

## Architecture

```
EventBridge (daily 5PM ET, cron)
        ↓
Lambda ① — Ingest (lambda_function.py)
  GET request to the Twelve Data REST API (JSON, API-key auth)
  Retries transient failures with exponential backoff
  Distinguishes "rate-limited" vs. "auth failed" vs. "invalid request"
  Stores raw JSON to S3 Bronze layer, unmodified
        ↓
S3 Upload Event (automatic trigger)
        ↓
Lambda ② — Transform & Load (lambda_transform.py)
  Reads raw JSON from S3
  Validates schema (required fields) and sanity (price/volume ranges)
  Skips and logs a bad record instead of failing the whole batch
  Upserts into RDS PostgreSQL (idempotent — safe to re-run)
        ↓
RDS PostgreSQL
  dim_stocks        — company reference data (sector, market cap, PE, dividend yield)
  fact_stock_prices — daily price records
        ↓
data_quality_check.py — run after the daily load
  Freshness: did every tracked symbol load today?
  Completeness: any symbol gone unexpectedly sparse this week?
  Sanity: does anything in the table itself look wrong?

EventBridge (weekly, cron) ─────────────────────────────────┐
        ↓                                                    │
Lambda ③ — Company Overview Refresh (lambda_overview.py)     │
  Two GET requests per symbol to different Twelve Data       │
  endpoints — /profile (name, sector) and /statistics        │
  (market cap, PE, dividend yield) — same auth, same client  │
  Stages both raw responses to S3, validates, upserts        │
  dim_stocks. Weekly, not daily — sector/market cap/PE don't │
  move day to day, so this doesn't spend API calls refreshing│
  data that's still accurate a week later                    │
────────────────────────────────────────────────────────────┘

Supporting services:
  twelvedata_client.py — shared auth, retry policy, and the
                          "rate-limited vs. auth-failed vs.
                          invalid-request" taxonomy both Lambda
                          ① and ③ use
  Secrets Manager  — API key and DB credentials, never in code or env vars
  CloudWatch       — logs and failure alerts
  GitHub Actions   — pytest runs on every push
```

---

## AWS Services Used

| Service | Role |
|---------|------|
| **Lambda** | Serverless compute for ingest, transform, and the quality check |
| **S3** | Raw data storage (Bronze layer) |
| **RDS PostgreSQL** | Structured storage for analytical queries |
| **EventBridge** | Daily scheduled trigger (cron) |
| **Secrets Manager** | Encrypted storage for the API key and DB credentials |
| **CloudWatch** | Log collection and failure alerting |
| **GitHub Actions** | CI — runs the test suite on every push |

---

## Reliability & data quality

The things most likely to actually break a recurring third-party data pull, and how each is handled here:

| Failure mode | Handling |
|---|---|
| Third-party API times out / connection drops | `urllib3.Retry` with exponential backoff (2s → 4s → 8s) on 5xx and connection errors — deliberately excludes 429 (see next row), only on the request itself |
| API rate-limits the key (HTTP 429) | Raised as `ApiRateLimitError` and *not* retried automatically — retrying a rate-limited request immediately just spends more of the same limited budget. Tracked separately from other failures in the run summary so it reads as "try again next scheduled run," not "something is broken" |
| The key itself is invalid, expired, or doesn't cover an endpoint (HTTP 401/403) | Raised as `ApiAuthenticationError` — distinct from a per-symbol problem on purpose, since every remaining symbol in the loop would fail identically. `lambda_handler` breaks out of the loop on this instead of logging the same failure five times |
| API rejects the request itself: bad/delisted symbol, malformed params (HTTP 400/404) | Raised as `ApiInvalidRequestError` — terminal for this one symbol, but not for the run; the loop continues to the next symbol. Also covers the rarer case of a 200 response whose body still says `"status": "error"` (documented Twelve Data behavior, checked for explicitly rather than assumed to only ever come with a 4xx) |
| API silently renames or drops a field | `validate_price_data()` / `validate_overview_data()` check for every required field by name before touching it, so a schema change fails loudly with the missing field named, not a bare `KeyError` |
| API returns technically-valid but nonsensical values | Sanity checks on top of schema checks: prices > 0, high ≥ low, volume ≥ 0, market cap ≥ 0 |
| A numeric field is legitimately absent for a business reason, not a bug | `/statistics` simply omits the `dividends_and_splits` block for a stock that's never paid a dividend — `_dig()` walks the nested structure and returns `None` instead of raising, so "no dividend" becomes a real `NULL`, not a crash |
| One bad record in a batch of five | Each record is validated and committed independently (`process_record`) — a bad file for one symbol is logged and skipped, the other four still load. The original version wrapped the whole batch in one transaction, so a single bad record rolled back everything |
| Pipeline re-runs on the same day (retry, backfill) | `ON CONFLICT ... DO UPDATE` upsert — idempotent by design, for both `fact_stock_prices` and `dim_stocks` |
| Silent degradation over time (a symbol quietly stops loading) | `data_quality_check.py` runs after the load and checks freshness (today's row exists per symbol), completeness (no symbol unexpectedly sparse over the last 7 days), and sanity (no bad values in the table itself) |

## Data Model

```sql
CREATE TABLE dim_stocks (
    symbol          VARCHAR(10) PRIMARY KEY,
    company_name    VARCHAR(100),
    sector          VARCHAR(50),
    market_cap      BIGINT,
    pe_ratio        DECIMAL(10,2),
    dividend_yield  DECIMAL(10,4),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE fact_stock_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(10) REFERENCES dim_stocks(symbol),
    price_date      DATE,
    open_price      DECIMAL(10,2),
    high_price      DECIMAL(10,2),
    low_price       DECIMAL(10,2),
    close_price     DECIMAL(10,2),
    volume          BIGINT,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(symbol, price_date)
);
```

Follows **Medallion Architecture**:
- **Bronze layer**: raw JSON in S3, partitioned by date (`bronze/stock_prices/YYYY-MM-DD/`), kept as-is so a transform bug can be fixed and replayed without re-calling the API
- **Gold layer**: cleaned, validated, structured data in RDS for SQL querying

---

## Testing

Core logic (validation rules, the quality checks) is written as plain functions with no AWS dependency at the call site, so it's unit-tested without needing real AWS credentials or a live database:

```bash
pip install -r requirements.txt
pytest -v
```

29 tests cover: the Twelve Data response taxonomy (rate-limited/429, auth-failed/401/403, invalid-request/400/404, and the 200-with-error-body edge case, all against a faked HTTP response), schema-drift detection for both price and overview records (missing/renamed fields), sanity-check edge cases (negative price, high < low, negative market cap, non-numeric values), the missing-dividend-block-becomes-`NULL` case, and the three data-quality checks — all against fake HTTP responses / DB cursors, no real API key, AWS credentials, or network access needed. CI runs this on every push via GitHub Actions.

---

## Setup (to deploy for real)

### Prerequisites
- AWS account with CLI configured
- Python 3.12+
- Twelve Data API key (free Basic plan — 800 requests/day, 8/minute; sign up at [twelvedata.com/pricing](https://twelvedata.com/pricing), no card required). `/time_series`, `/profile`, and `/statistics` were all verified reachable on the free plan for a test symbol before this pipeline was built against them — some providers gate fundamentals data to paid tiers, but Twelve Data's free plan covers all three endpoints this pipeline uses.

### Steps

1. **RDS** — create a PostgreSQL `db.t3.micro` instance (Free Tier) with public access enabled. Note the endpoint.

2. **Secrets Manager** — store two secrets:
   - `stock-pipeline/db-credentials` with host, port, username, password, database
   - `stock-pipeline/twelvedata-api-key` with the API key

3. **S3** — create a bucket for raw data storage.

4. **Lambda ①** — deploy `lambda_function.py` + `twelvedata_client.py` + `symbols.py` (Python 3.12, 60s timeout). Set environment variable `S3_BUCKET`. Attach an execution role with permissions for S3, Secrets Manager, and CloudWatch Logs.

5. **Lambda ②** — package `lambda_transform.py` with `psycopg2-binary` (Linux build required for Lambda). Deploy with the same execution role.

6. **S3 trigger** — configure S3 to invoke Lambda ② on `ObjectCreated` events under the `bronze/stock_prices/` prefix.

7. **EventBridge (daily)** — create a cron rule (`cron(0 21 * * ? *)`) to invoke Lambda ① daily at 5PM ET.

8. **Lambda ③** — deploy `lambda_overview.py` + `twelvedata_client.py` + `symbols.py` with `psycopg2-binary`. Same execution role as Lambda ②, plus S3 write access. On its own **weekly** EventBridge cron rule — company reference data doesn't need a daily refresh.

9. **Lambda ④ (optional)** — deploy `data_quality_check.py` on its own EventBridge schedule shortly after Lambda ①/②, with a CloudWatch alarm on non-200 responses.

10. **CloudWatch** — create metric alarms on `Errors` for all Lambda functions, with SNS email notification on failure.
