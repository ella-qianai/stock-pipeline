import boto3
import json
import os
import psycopg2
from datetime import datetime, timezone

secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

DB_SECRET_NAME = "stock-pipeline/db-credentials"


def get_db_credentials():
    """from Secrets Manager load database connection information"""
    response = secrets_client.get_secret_value(SecretId=DB_SECRET_NAME)
    return json.loads(response["SecretString"])


def get_db_connection(creds):
    """establish RDS PostgreSQL connection"""
    return psycopg2.connect(
        host=creds["host"],
        database=creds["database"],
        user=creds["username"],
        password=creds["password"],
        port=creds["port"],
        connect_timeout=10
    )


def read_from_s3(bucket, key):
    """read original JSON file from S3"""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def get_latest_price(time_series):
    """extract latest day data from time series"""
    latest_date = sorted(time_series.keys())[-1]
    return latest_date, time_series[latest_date]


def upsert_stock_price(cursor, symbol, price_date, price_data):
    """write stock price data to fact_stock_prices table (update if exists, insert if not)"""
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
        symbol,
        price_date,
        float(price_data["1. open"]),
        float(price_data["2. high"]),
        float(price_data["3. low"]),
        float(price_data["4. close"]),
        int(price_data["5. volume"])
    ))


def lambda_handler(event, context):
    """
    Triggered by S3 upload event, event contains bucket name and file path
    """
    creds = get_db_credentials()
    conn = get_db_connection(creds)
    cursor = conn.cursor()
    processed = []

    try:
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            # extract stock code from file path (e.g. bronze/stock_prices/2026-04-12/AAPL.json)
            symbol = key.split("/")[-1].replace(".json", "")
            print(f"Processing {symbol} from s3://{bucket}/{key}")

            raw_data = read_from_s3(bucket, key)

            if "Time Series (Daily)" not in raw_data:
                print(f"Warning: No time series data for {symbol}, skipping")
                continue

            time_series = raw_data["Time Series (Daily)"]
            price_date, price_data = get_latest_price(time_series)

            upsert_stock_price(cursor, symbol, price_date, price_data)
            processed.append(f"{symbol} - {price_date}")
            print(f"Inserted {symbol} price for {price_date}")

        conn.commit()
        print(f"Done. Processed: {processed}")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise e

    finally:
        cursor.close()
        conn.close()

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": processed})
    }