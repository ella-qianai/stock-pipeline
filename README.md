# Stock Market ETL Pipeline on AWS

![tests](https://github.com/ella-qianai/stock-pipeline/actions/workflows/tests.yml/badge.svg)

A fully automated, scheduled ETL pipeline: it calls a third-party REST API daily, stages the raw response in S3, validates and transforms it, and loads it into a PostgreSQL data warehouse — with retry logic, per-record failure isolation, and a standalone data-quality check on top.

It's a small pipeline (5 stocks, one API), but every piece is built the way a larger one would need to be: nothing silently swallows a bad record, nothing assumes the upstream API will always behave, and every failure mode has a specific, readable log line instead of a stack trace.

---

## Architecture

```
EventBridge (daily 5PM ET, cron)
        ↓
Lambda ① — Ingest
  GET request to the Alpha Vantage REST API (JSON, API-key auth)
  Retries transient failures with exponential backoff
  Distinguishes "rate-limited" from "no data" from "bad response"
  Stores raw JSON to S3 Bronze layer, unmodified
        ↓
S3 Upload Event (automatic trigger)
        ↓
Lambda ② — Transform & Load
  Reads raw JSON from S3
  Validates schema (required fields) and sanity (price/volume ranges)
  Skips and logs a bad record instead of failing the whole batch
  Upserts into RDS PostgreSQL (idempotent — safe to re-run)
        ↓
RDS PostgreSQL
  dim_stocks        — company reference data
  fact_stock_prices — daily price records
        ↓
data_quality_check.py — run after the daily load
  Freshness: did every tracked symbol load today?
  Completeness: any symbol gone unexpectedly sparse this week?
  Sanity: does anything in the table itself look wrong?

Supporting services:
  Secrets Manager  — API key and DB credentials, never in code or env vars
  CloudWatch       — logs and failure alerts
  GitHub Actions   — pytest runs on every push
```

---

## Why this exists

This started as a way to practice an end-to-end ETL build, but the design choices below are aimed at a specific, recurring data-engineering problem: **keeping a pipeline that depends on someone else's API honest when that API changes underneath you, rate-limits you, or just sends something slightly malformed.** That's the part that tends to matter more in practice than the happy path.

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
| Third-party API times out / connection drops | `urllib3.Retry` with exponential backoff (2s → 4s → 8s), only on the request itself |
| API rate-limits the key | Alpha Vantage returns HTTP 200 with a `"Note"` field instead of an error — this is checked for explicitly (`ApiRateLimitError`) and logged separately from a real failure, so it's never confused with "the symbol doesn't exist" |
| API silently renames or drops a field | `validate_price_data()` checks for every required field by name before touching it, so a schema change fails loudly with the missing field named, not a bare `KeyError` |
| API returns technically-valid but nonsensical values | Sanity checks on top of schema checks: prices > 0, high ≥ low, volume ≥ 0 |
| One bad record in a batch of five | Each record is validated and committed independently (`process_record`) — a bad file for one symbol is logged and skipped, the other four still load. The original version wrapped the whole batch in one transaction, so a single bad record rolled back everything |
| Pipeline re-runs on the same day (retry, backfill) | `ON CONFLICT ... DO UPDATE` upsert — idempotent by design |
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

14 tests cover: schema-drift detection (missing/renamed fields), sanity-check edge cases (negative price, high < low, non-numeric values), and the three data-quality checks against a fake DB cursor. CI runs this on every push via GitHub Actions.

---

## Setup (to deploy for real)

### Prerequisites
- AWS account with CLI configured
- Python 3.12+
- Alpha Vantage API key (free tier — 25 requests/day)

### Steps

1. **RDS** — create a PostgreSQL `db.t3.micro` instance (Free Tier) with public access enabled. Note the endpoint.

2. **Secrets Manager** — store two secrets:
   - `stock-pipeline/db-credentials` with host, port, username, password, database
   - `stock-pipeline/alphavantage-api-key` with the API key

3. **S3** — create a bucket for raw data storage.

4. **Lambda ①** — deploy `lambda_function.py` (Python 3.12, 60s timeout). Set environment variable `S3_BUCKET`. Attach an execution role with permissions for S3, Secrets Manager, and CloudWatch Logs.

5. **Lambda ②** — package `lambda_transform.py` with `psycopg2-binary` (Linux build required for Lambda). Deploy with the same execution role.

6. **S3 trigger** — configure S3 to invoke Lambda ② on `ObjectCreated` events under the `bronze/` prefix.

7. **EventBridge** — create a cron rule (`cron(0 21 * * ? *)`) to invoke Lambda ① daily at 5PM ET.

8. **Lambda ③ (optional)** — deploy `data_quality_check.py` on its own EventBridge schedule shortly after Lambda ①/②, with a CloudWatch alarm on non-200 responses.

9. **CloudWatch** — create metric alarms on `Errors` for all Lambda functions, with SNS email notification on failure.

---

## Design Decisions

**Why two Lambda functions instead of one?**
Separating ingest from transform means each function has a single responsibility. A schema change in the API only requires updating Lambda ②, without touching the ingestion logic. Each can be tested and debugged independently.

**Why store raw data in S3 before loading to RDS?**
The S3 Bronze layer acts as a replayable source of truth. If the transform logic has a bug, raw data is preserved and can be reprocessed without re-calling the API — important for staying within the free tier's 25 requests/day limit.

**Why Secrets Manager instead of environment variables?**
Environment variables are visible in the Lambda console and can appear in logs. Secrets Manager provides encryption at rest, access auditing, and is standard practice for SOC II compliance in fintech environments.

**Why `ON CONFLICT` in the upsert query?**
Ensures idempotency — running the pipeline multiple times on the same day won't create duplicate records.

**Why validate at the transform step instead of trusting the API?**
Free third-party APIs change without much notice. Checking the shape and sanity of the data on the way in means a schema change gets caught here, with a specific error message, instead of surfacing as a wrong number in a downstream report weeks later.

---

## Tracked Stocks

AAPL · MSFT · GOOGL · AMZN · NVDA

---

## Cost

Fits within AWS Free Tier for 12 months. RDS `db.t3.micro` begins billing after the free tier expires — delete the instance when not in use.
