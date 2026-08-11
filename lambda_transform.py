import boto3
import json
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

DB_SECRET_NAME = "stock-pipeline/db-credentials"

# The fields Twelve Data is expected to return for each daily bar. Checked
# explicitly on every record so a silent upstream schema change (a renamed
# or missing field) shows up as a clear log line instead of a raw KeyError
# three functions away from where it actually happened.
REQUIRED_PRICE_FIELDS = ["open", "high", "low", "close", "volume"]


def get_db_credentials():
    """Load RDS connection info from Secrets Manager."""
    response = secrets_client.get_secret_value(SecretId=DB_SECRET_NAME)
    return json.loads(response["SecretString"])


def get_db_connection(creds):
    """Open an RDS PostgreSQL connection."""
    return psycopg2.connect(
        host=creds["host"],
        database=creds["database"],
        user=creds["username"],
        password=creds["password"],
        port=creds["port"],
        connect_timeout=10,
    )


def read_from_s3(bucket, key):
    """Read one raw JSON payload from the S3 Bronze layer."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def get_latest_price(values):
    """Pick the most recent trading day out of the "values" list.

    lambda_function.py requests `order=desc`, so values[0] is already the
    latest bar — but that's re-derived here with max() instead of trusted
    blindly. If the fetch-time param ever changed (or a future caller built
    this list a different way), reading the wrong day should fail loudly
    against real data, not silently pass because index 0 used to be right.
    """
    latest = max(values, key=lambda v: v["datetime"])
    return latest["datetime"], latest


class DataQualityError(Exception):
    """Raised when a single day's price record fails validation, so it can
    be skipped and logged without aborting the rest of the batch."""


def validate_price_data(symbol, price_date, price_data):
    """Schema and sanity checks run on every record before it's written.

    Two different failure modes are checked for on purpose:
    - schema drift: a field Twelve Data used to send is missing or renamed
    - bad values: fields are present but nonsensical (negative price,
      high below low), which a schema check alone would miss
    """
    missing = [f for f in REQUIRED_PRICE_FIELDS if f not in price_data]
    if missing:
        raise DataQualityError(
            f"{symbol} {price_date}: missing fields {missing} — possible upstream schema change"
        )

    try:
        open_p = float(price_data["open"])
        high_p = float(price_data["high"])
        low_p = float(price_data["low"])
        close_p = float(price_data["close"])
        volume = int(float(price_data["volume"]))
    except (TypeError, ValueError) as e:
        raise DataQualityError(f"{symbol} {price_date}: non-numeric field value ({e})")

    if min(open_p, high_p, low_p, close_p) <= 0:
        raise DataQualityError(f"{symbol} {price_date}: non-positive price found")
    if high_p < low_p:
        raise DataQualityError(f"{symbol} {price_date}: high ({high_p}) < low ({low_p})")
    if volume < 0:
        raise DataQualityError(f"{symbol} {price_date}: negative volume ({volume})")

    return {
        "open_price": open_p,
        "high_price": high_p,
        "low_price": low_p,
        "close_price": close_p,
        "volume": volume,
    }


def upsert_stock_price(cursor, symbol, price_date, clean):
    """Write one validated price record (idempotent — safe to re-run)."""
    sql = """
        INSERT INTO fact_stock_prices
            (symbol, price_date, open_price, high_price, low_price, close_price, volume)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, price_date)
        DO UPDATE SET
            open_price  = EXCLUDED.open_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume      = EXCLUDED.volume
    """
    cursor.execute(sql, (
        symbol, price_date,
        clean["open_price"], clean["high_price"], clean["low_price"],
        clean["close_price"], clean["volume"],
    ))


def process_record(cursor, bucket, key):
    """Handle one S3 object end to end: read, validate, upsert.

    Raises DataQualityError / KeyError for the caller to catch, so one bad
    file in a batch of five doesn't roll back the four good ones.
    """
    symbol = key.split("/")[-1].replace(".json", "")
    raw_data = read_from_s3(bucket, key)

    if not raw_data.get("values"):
        raise DataQualityError(f"{symbol}: no 'values' array in raw payload")

    price_date, price_data = get_latest_price(raw_data["values"])
    clean = validate_price_data(symbol, price_date, price_data)
    upsert_stock_price(cursor, symbol, price_date, clean)
    return f"{symbol} - {price_date}"


def lambda_handler(event, context):
    """Triggered by an S3 ObjectCreated event under bronze/.

    Each record in the batch is committed independently: if one symbol's
    file is malformed, it's logged and skipped, and the rest of the batch
    still lands in RDS. The old version wrapped the whole loop in a single
    transaction, so one bad record silently discarded every good one in
    the same run.
    """
    creds = get_db_credentials()
    conn = get_db_connection(creds)
    cursor = conn.cursor()
    processed = []
    skipped = []

    try:
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            try:
                result = process_record(cursor, bucket, key)
                conn.commit()
                processed.append(result)
                logger.info("Processed %s", result)
            except (DataQualityError, KeyError) as e:
                conn.rollback()
                skipped.append(str(e))
                logger.warning("Skipping %s: %s", key, e)
    finally:
        cursor.close()
        conn.close()

    logger.info("Done. processed=%d skipped=%d", len(processed), len(skipped))
    if skipped:
        logger.warning("Skipped records: %s", skipped)

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": processed, "skipped": skipped}),
    }
