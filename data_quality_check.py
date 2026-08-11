"""Standalone data-quality check for fact_stock_prices.

Meant to run on a schedule shortly after the daily load (e.g. a third
EventBridge-triggered Lambda, or a cron job) and report on the pipeline's
output rather than assuming a "no errors raised" run means the data is
actually good. Three questions it answers, on purpose:

1. Freshness  — did today's load actually happen for every tracked symbol?
2. Completeness — are there unexpected gaps in the last 7 trading days?
3. Sanity     — do the values in the table still look like real prices?

Exit code is non-zero if any check fails, so it can be wired into a
CloudWatch alarm / SNS notification the same way the Lambda error alarms
already are.
"""
import json
import logging
import sys
from datetime import date, timedelta

import boto3
import psycopg2

from symbols import TRACKED_SYMBOLS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_SECRET_NAME = "stock-pipeline/db-credentials"


def get_db_credentials():
    secrets_client = boto3.client("secretsmanager")
    response = secrets_client.get_secret_value(SecretId=DB_SECRET_NAME)
    return json.loads(response["SecretString"])


def get_connection(creds):
    return psycopg2.connect(
        host=creds["host"], database=creds["database"],
        user=creds["username"], password=creds["password"],
        port=creds["port"], connect_timeout=10,
    )


def check_freshness(cursor, as_of: date):
    """Every tracked symbol should have a row for the most recent load."""
    cursor.execute(
        "SELECT symbol FROM fact_stock_prices WHERE price_date = %s",
        (as_of,),
    )
    present = {row[0] for row in cursor.fetchall()}
    missing = sorted(set(TRACKED_SYMBOLS) - present)
    return {
        "check": "freshness",
        "passed": len(missing) == 0,
        "detail": f"missing symbols for {as_of}: {missing}" if missing else f"all {len(TRACKED_SYMBOLS)} symbols present",
    }


def check_completeness(cursor, lookback_days: int = 7):
    """Flag symbols with fewer rows than expected over the last N calendar
    days — a rough proxy for 'did this symbol silently stop loading'."""
    since = date.today() - timedelta(days=lookback_days)
    cursor.execute(
        """
        SELECT symbol, COUNT(*) FROM fact_stock_prices
        WHERE price_date >= %s
        GROUP BY symbol
        """,
        (since,),
    )
    counts = dict(cursor.fetchall())
    sparse = {s: counts.get(s, 0) for s in TRACKED_SYMBOLS if counts.get(s, 0) < 3}
    return {
        "check": "completeness",
        "passed": len(sparse) == 0,
        "detail": f"symbols with <3 rows in last {lookback_days}d: {sparse}" if sparse else "no unexpectedly sparse symbols",
    }


def check_sanity(cursor):
    """Values already pass validate_price_data() on the way in, but this
    re-checks the table directly — catches issues from any write path
    that bypasses the transform Lambda (manual backfills, etc.)."""
    cursor.execute(
        """
        SELECT symbol, price_date FROM fact_stock_prices
        WHERE high_price < low_price
           OR open_price <= 0 OR close_price <= 0
           OR volume < 0
        LIMIT 10
        """
    )
    bad_rows = cursor.fetchall()
    return {
        "check": "sanity",
        "passed": len(bad_rows) == 0,
        "detail": f"found {len(bad_rows)} rows failing basic sanity checks: {bad_rows}" if bad_rows else "no sanity violations found",
    }


def run_checks(cursor, as_of: date = None) -> list:
    as_of = as_of or date.today()
    return [
        check_freshness(cursor, as_of),
        check_completeness(cursor),
        check_sanity(cursor),
    ]


def lambda_handler(event, context):
    creds = get_db_credentials()
    conn = get_connection(creds)
    try:
        cursor = conn.cursor()
        results = run_checks(cursor)
    finally:
        conn.close()

    for r in results:
        level = logger.info if r["passed"] else logger.error
        level("[%s] %s — %s", r["check"], "PASS" if r["passed"] else "FAIL", r["detail"])

    all_passed = all(r["passed"] for r in results)
    return {
        "statusCode": 200 if all_passed else 500,
        "body": json.dumps({"passed": all_passed, "checks": results}),
    }


if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(json.loads(result["body"]), indent=2))
    sys.exit(0 if result["statusCode"] == 200 else 1)
