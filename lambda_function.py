import boto3
import json
import logging
import urllib3
import os
from datetime import datetime, timezone
from urllib3.util import Retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

S3_BUCKET = os.environ.get("S3_BUCKET")
SECRET_NAME = "stock-pipeline/alphavantage-api-key"
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

# Retry policy for the Alpha Vantage call: retry on connection resets and
# 5xx responses, back off exponentially so a transient blip doesn't burn
# through the free tier's 25 requests/day limit any faster than necessary.
RETRY_POLICY = Retry(
    total=3,
    backoff_factor=2,  # 2s, 4s, 8s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
http = urllib3.PoolManager(retries=RETRY_POLICY)


def get_api_key():
    """Load the Alpha Vantage API key from Secrets Manager (not an env var,
    so it never ends up printed in a log line or the Lambda console)."""
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response["SecretString"])
    return secret["api_key"]


class ApiRateLimitError(Exception):
    """Raised when Alpha Vantage's response body indicates the key is
    throttled, so callers can distinguish 'no data because rate-limited'
    from 'no data because the symbol is bad'."""


def fetch_stock_data(symbol, api_key):
    """Fetch daily time series for one symbol from Alpha Vantage.

    Alpha Vantage does not use HTTP status codes for rate limiting — a
    throttled request still returns 200 with a "Note" or "Information"
    field instead of the expected "Time Series (Daily)" payload. That's a
    common failure mode for free third-party APIs, so it's handled
    explicitly here instead of just falling through to "no data".
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",  # last 100 trading days
        "apikey": api_key,
    }
    try:
        response = http.request("GET", url, fields=params, timeout=10.0)
    except urllib3.exceptions.HTTPError as e:
        logger.error("HTTP request failed for %s after retries: %s", symbol, e)
        return None

    if response.status != 200:
        logger.error("Unexpected status %s for %s", response.status, symbol)
        return None

    data = json.loads(response.data.decode("utf-8"))

    if "Note" in data or "Information" in data:
        # This is the API telling us we're rate-limited, not that the data
        # doesn't exist — worth a distinct log line so it's not confused
        # with a real "symbol not found" case downstream.
        logger.warning(
            "Alpha Vantage rate-limit response for %s: %s",
            symbol,
            data.get("Note") or data.get("Information"),
        )
        raise ApiRateLimitError(symbol)

    if "Time Series (Daily)" not in data:
        logger.warning("Unexpected response shape for %s: keys=%s", symbol, list(data.keys()))
        return None

    return data


def save_to_s3(data, symbol):
    """Store the raw JSON response to the S3 Bronze layer, unmodified."""
    dt_now = datetime.now(tz=timezone.utc)
    key = f"bronze/stock_prices/{dt_now.strftime('%Y-%m-%d')}/{symbol}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    logger.info("Saved %s to s3://%s/%s", symbol, S3_BUCKET, key)
    return key


def lambda_handler(event, context):
    api_key = get_api_key()
    saved_files = []
    rate_limited = []
    failed = []

    for symbol in SYMBOLS:
        logger.info("Fetching data for %s...", symbol)
        try:
            data = fetch_stock_data(symbol, api_key)
        except ApiRateLimitError:
            rate_limited.append(symbol)
            continue

        if data:
            key = save_to_s3(data, symbol)
            saved_files.append(key)
        else:
            failed.append(symbol)
            logger.warning("No usable data returned for %s", symbol)

    logger.info(
        "Done. saved=%d rate_limited=%d failed=%d",
        len(saved_files), len(rate_limited), len(failed),
    )

    if rate_limited or failed:
        # Still return 200 (partial success is normal for a 5-symbol daily
        # pull), but surface which symbols need a retry on the next run
        # instead of silently dropping them.
        logger.warning("Incomplete run — rate_limited=%s failed=%s", rate_limited, failed)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "saved_files": saved_files,
            "rate_limited": rate_limited,
            "failed": failed,
        }),
    }
