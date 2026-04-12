# Stock Market ETL Pipeline on AWS

A fully automated data pipeline that ingests daily stock price data from the Alpha Vantage API, stores raw data in S3, transforms it, and loads it into a PostgreSQL database on AWS RDS.

---

## Architecture

```
EventBridge (daily 5PM ET)
        ↓
Lambda ① — Ingest
  Fetches stock prices from Alpha Vantage API
  Stores raw JSON to S3 Bronze layer
        ↓
S3 Upload Event (automatic trigger)
        ↓
Lambda ② — Transform & Load
  Reads raw JSON from S3
  Cleans and transforms data
  Loads into RDS PostgreSQL
        ↓
RDS PostgreSQL
  dim_stocks        — company reference data
  fact_stock_prices — daily price records

Supporting services:
  Secrets Manager  — API keys and DB credentials
  CloudWatch       — logs and failure alerts
```

---

## AWS Services Used

| Service | Role |
|---------|------|
| **Lambda** | Serverless compute for ingest and transform |
| **S3** | Raw data storage (Bronze layer) |
| **RDS PostgreSQL** | Structured storage for analytical queries |
| **EventBridge** | Daily scheduled trigger |
| **Secrets Manager** | Encrypted storage for credentials |
| **CloudWatch** | Log collection and failure alerting |

---

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
- **Bronze layer**: raw JSON in S3, partitioned by date (`bronze/stock_prices/YYYY-MM-DD/`)
- **Gold layer**: cleaned, structured data in RDS for SQL querying

---

## Setup

### Prerequisites
- AWS account with CLI configured
- Python 3.12+
- Alpha Vantage API key (free tier)

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

8. **CloudWatch** — create metric alarms on `Errors` for both Lambda functions, with SNS email notification on failure.

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

---

## Tracked Stocks

AAPL · MSFT · GOOGL · AMZN · NVDA

---

## Cost

Fits within AWS Free Tier for 12 months. RDS `db.t3.micro` begins billing after the free tier expires — delete the instance when not in use.
