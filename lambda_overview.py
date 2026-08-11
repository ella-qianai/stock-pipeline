"""Weekly company-overview refresh: populates dim_stocks.

This is a second, deliberately different integration against the same
provider as lambda_function.py: two different endpoints (`/profile` for
company identity, `/statistics` for valuation metrics), a different
refresh cadence, and its own validation rules. Company sector/market cap/
PE ratio don't change meaningfully day to day, so refreshing this weekly
instead of daily is a design choice, not a limitation — no reason to spend
10 of the free tier's 800 daily requests (2 calls x 5 symbols) on data
that's still accurate a week later.

Structured as fetch -> validate -> upsert in a single Lambda rather than
split across an S3 hop like the price pipeline: at this volume (5 symbols,
weekly) the extra moving part isn't buying anything, but each raw response
is still staged to S3 first for the same reason as the price data — so a
validation bug can be fixed and replayed without re-spending API calls.
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import psycopg2

from twelvedata_client import (
    ApiAuthenticationError,
    ApiInvalidRequestError,
    ApiRateLimitError,
    call_twelvedata,
    get_api_key,
)
from symbols import TRACKED_SYMBOLS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

S3_BUCKET = os.environ.get("S3_BUCKET")
DB_SECRET_NAME = "stock-pipeline/db-credentials"

# /profile fields this pipeline requires. Sector is intentionally not
# required — it's genuinely absent for some instrument types, and a
# missing sector isn't a schema-drift signal the way a missing name is.
REQUIRED_PROFILE_FIELDS = ["symbol", "name"]


class OverviewValidationError(Exception):
    """Raised when a single symbol's overview record fails validation, so
    it can be skipped and logged without aborting the rest of the batch —
    same per-record isolation as lambda_transform.py's DataQualityError."""


def get_db_credentials():
    secrets_client = boto3.client("secretsmanager")
    response = secrets_client.get_secret_value(SecretId=DB_SECRET_NAME)
    return json.loads(response["SecretString"])


def get_db_connection(creds):
    return psycopg2.connect(
        host=creds["host"], database=creds["database"],
        user=creds["username"], password=creds["password"],
        port=creds["port"], connect_timeout=10,
    )


def fetch_company_profile(symbol, api_key):
    """Company identity: name, sector. Reuses call_twelvedata for the
    request/auth/rate-limit handling shared with the price pipeline."""
    return call_twelvedata("profile", {"symbol": symbol, "apikey": api_key})


def fetch_company_statistics(symbol, api_key):
    """Valuation metrics: market cap, PE ratio, dividend yield. A separate
    call from /profile — Twelve Data doesn't return these on the same
    endpoint, which is why this Lambda makes two requests per symbol
    instead of one."""
    return call_twelvedata("statistics", {"symbol": symbol, "apikey": api_key})


def save_raw_overview_to_s3(bucket, symbol, profile, statistics):
    dt_now = datetime.now(tz=timezone.utc)
    prefix = f"bronze/company_overview/{dt_now.strftime('%Y-%m-%d')}"
    s3_client.put_object(
        Bucket=bucket, Key=f"{prefix}/{symbol}_profile.json",
        Body=json.dumps(profile), ContentType="application/json",
    )
    s3_client.put_object(
        Bucket=bucket, Key=f"{prefix}/{symbol}_statistics.json",
        Body=json.dumps(statistics), ContentType="application/json",
    )


def _dig(d, *keys):
    """Walk a chain of nested dict keys, returning None if any level is
    missing instead of raising. /statistics nests everything several
    levels deep (statistics -> valuations_metrics -> market_capitalization),
    and several of those sub-blocks are simply absent for symbols they
    don't apply to — e.g. dividends_and_splits is missing entirely for a
    stock that's never paid a dividend, which isn't a data-quality problem,
    just "no dividend" and should become NULL, not an error.
    """
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def validate_overview_data(symbol, profile, statistics):
    """Combine /profile and /statistics into one dim_stocks record.

    Two different strictness levels on purpose: profile.symbol/name
    missing means the API changed shape under us (raise — this is the
    schema-drift case). The numeric fields under /statistics can be
    legitimately absent for real business reasons rather than a data
    problem, so those become NULL via _dig() instead of raising.
    """
    missing = [f for f in REQUIRED_PROFILE_FIELDS if f not in profile]
    if missing:
        raise OverviewValidationError(
            f"{symbol}: profile missing fields {missing} — possible upstream schema change"
        )

    if profile["symbol"] != symbol:
        # Defends against a mismatched/cached response being attributed to
        # the wrong symbol rather than failing later as a confusing FK error.
        raise OverviewValidationError(
            f"{symbol}: profile response symbol is '{profile['symbol']}', expected '{symbol}'"
        )

    market_cap = _dig(statistics, "statistics", "valuations_metrics", "market_capitalization")
    pe_ratio = _dig(statistics, "statistics", "valuations_metrics", "trailing_pe")
    dividend_yield = _dig(
        statistics, "statistics", "dividends_and_splits", "trailing_annual_dividend_yield"
    )

    if market_cap is not None and market_cap < 0:
        raise OverviewValidationError(f"{symbol}: negative market cap ({market_cap})")

    return {
        "company_name": profile["name"],
        "sector": profile.get("sector"),
        "market_cap": int(market_cap) if market_cap is not None else None,
        "pe_ratio": pe_ratio,
        "dividend_yield": dividend_yield,
    }


def upsert_company_overview(cursor, symbol, clean):
    """Write one validated overview record (idempotent — safe to re-run)."""
    sql = """
        INSERT INTO dim_stocks
            (symbol, company_name, sector, market_cap, pe_ratio, dividend_yield)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol)
        DO UPDATE SET
            company_name   = EXCLUDED.company_name,
            sector         = EXCLUDED.sector,
            market_cap     = EXCLUDED.market_cap,
            pe_ratio       = EXCLUDED.pe_ratio,
            dividend_yield = EXCLUDED.dividend_yield
    """
    cursor.execute(sql, (
        symbol, clean["company_name"], clean["sector"],
        clean["market_cap"], clean["pe_ratio"], clean["dividend_yield"],
    ))


def lambda_handler(event, context):
    api_key = get_api_key()
    creds = get_db_credentials()
    conn = get_db_connection(creds)
    cursor = conn.cursor()

    processed, rate_limited, skipped = [], [], []

    try:
        for symbol in TRACKED_SYMBOLS:
            logger.info("Fetching overview for %s...", symbol)
            try:
                profile = fetch_company_profile(symbol, api_key)
                statistics = fetch_company_statistics(symbol, api_key)
                save_raw_overview_to_s3(S3_BUCKET, symbol, profile, statistics)
                clean = validate_overview_data(symbol, profile, statistics)
                upsert_company_overview(cursor, symbol, clean)
                conn.commit()
                processed.append(symbol)
            except ApiRateLimitError:
                conn.rollback()
                rate_limited.append(symbol)
            except ApiAuthenticationError as e:
                # Same reasoning as lambda_function.py: a bad key fails
                # identically for every remaining symbol, so stop the run
                # here instead of repeating the same error four more times.
                conn.rollback()
                logger.error("Authentication failed, aborting run: %s", e)
                break
            except (ApiInvalidRequestError, OverviewValidationError) as e:
                conn.rollback()
                skipped.append(str(e))
                logger.warning("Skipping %s: %s", symbol, e)
    finally:
        cursor.close()
        conn.close()

    logger.info(
        "Done. processed=%d rate_limited=%d skipped=%d",
        len(processed), len(rate_limited), len(skipped),
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "rate_limited": rate_limited,
            "skipped": skipped,
        }),
    }
