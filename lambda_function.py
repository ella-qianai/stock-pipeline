import boto3
import json
import urllib3
import os
from datetime import datetime, timezone

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

S3_BUCKET = os.environ.get("S3_BUCKET")
SECRET_NAME = "stock-pipeline/alphavantage-api-key"
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]


def get_api_key():
    """from Secrets Manager load Alpha Vantage API Key"""
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response["SecretString"])
    return secret["api_key"]


def fetch_stock_data(symbol, api_key):
    """Use Alpha Vantage API extract daily stock prices"""
    http = urllib3.PoolManager()
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",  # 100 days
        "apikey": api_key
    }
    try:
        response = http.request("GET", url, fields=params)
        data = json.loads(response.data.decode("utf-8"))
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


def save_to_s3(data, symbol):
    """Store JSON to S3 Bronze"""
    dt_now = datetime.now(tz=timezone.utc)
    # path format：bronze/stock_prices/2026-04-11/AAPL.json
    key = f"bronze/stock_prices/{dt_now.strftime('%Y-%m-%d')}/{symbol}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    print(f"Saved {symbol} to s3://{S3_BUCKET}/{key}")
    return key


def lambda_handler(event, context):
    api_key = get_api_key()
    saved_files = []

    for symbol in SYMBOLS:
        print(f"Fetching data for {symbol}...")
        data = fetch_stock_data(symbol, api_key)
        if data and "Time Series (Daily)" in data:
            key = save_to_s3(data, symbol)
            saved_files.append(key)
        else:
            print(f"Warning: No data returned for {symbol}")

    print(f"Done. Saved {len(saved_files)} files to S3.")
    return {
        "statusCode": 200,
        "body": json.dumps({"saved_files": saved_files})
    }