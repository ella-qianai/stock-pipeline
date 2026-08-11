import boto3
import json
import logging
import os
from datetime import datetime, timezone

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
SYMBOLS = TRACKED_SYMBOLS


def fetch_stock_data(symbol, api_key):
    """Fetch the last 100 daily bars for one symbol from Twelve Data.

    Delegates the request itself, and the rate-limit / auth / invalid-
    request distinction, to twelvedata_client. What's specific to this
    endpoint is checked here: a 200 that passes those checks can still be
    missing the "values" array in edge cases (e.g. a symbol with no trading
    history yet) that aren't one of the client-level error shapes.

    `order=desc` is requested explicitly so the most recent bar is always
    values[0] — see get_latest_price() in lambda_transform.py, which
    re-sorts defensively rather than trusting this blindly, but there's no
    reason to make it do more work than it needs to.
    """
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": "100",  # last 100 trading days
        "order": "desc",
        "apikey": api_key,
    }
    data = call_twelvedata("time_series", params)

    if "values" not in data or not data["values"]:
        logger.warning("Unexpected response shape for %s: keys=%s", symbol, list(data.keys()))
        raise ApiInvalidRequestError(f"{symbol}: no 'values' array in response")

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
    invalid = []

    for symbol in SYMBOLS:
        logger.info("Fetching data for %s...", symbol)
        try:
            data = fetch_stock_data(symbol, api_key)
        except ApiRateLimitError:
            # Transient — worth retrying on the next scheduled run.
            rate_limited.append(symbol)
            continue
        except ApiAuthenticationError as e:
            # The key itself is bad — every remaining symbol would fail
            # the exact same way, so stop the run here instead of logging
            # the same error four more times and returning a misleading
            # "4 invalid, 1 saved" that looks like a per-symbol problem.
            logger.error("Authentication failed, aborting run: %s", e)
            break
        except ApiInvalidRequestError as e:
            # Terminal for this symbol on this run — a bad/delisted symbol
            # or a malformed response won't fix itself by retrying later,
            # so it's tracked separately from rate-limiting rather than
            # lumped into the same bucket.
            logger.warning("Invalid request for %s: %s", symbol, e)
            invalid.append(symbol)
            continue

        key = save_to_s3(data, symbol)
        saved_files.append(key)

    logger.info(
        "Done. saved=%d rate_limited=%d invalid=%d",
        len(saved_files), len(rate_limited), len(invalid),
    )

    if rate_limited or invalid:
        # Still return 200 (partial success is normal for a 5-symbol daily
        # pull), but surface which symbols need attention on the next run
        # instead of silently dropping them.
        logger.warning("Incomplete run — rate_limited=%s invalid=%s", rate_limited, invalid)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "saved_files": saved_files,
            "rate_limited": rate_limited,
            "invalid": invalid,
        }),
    }
