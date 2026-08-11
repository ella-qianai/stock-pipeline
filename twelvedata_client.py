"""Shared Twelve Data HTTP client: auth, retry policy, and error handling.

Twelve Data uses conventional HTTP status codes for errors (400/401/403/
404/429/5xx, with a JSON body carrying `code`/`message`/`status`), so "was
this an error" is just `response.status` — the taxonomy below exists to
turn that into something callers can act on differently depending on which
kind of failure it was, not just "it didn't work."

Free tier (Basic plan): 800 requests/day, 8 requests/minute — comfortable
headroom for tracking 5 symbols with a daily price pull, a weekly overview
refresh, retries, occasional backfills, and running the pipeline locally
without worrying about burning the day's quota.
"""
import json
import logging

import boto3
import urllib3
from urllib3.util import Retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASE_URL = "https://api.twelvedata.com"
SECRET_NAME = "stock-pipeline/twelvedata-api-key"

# Retry on connection resets and 5xx responses, back off exponentially.
# 429 (rate-limited) is NOT in this list on purpose — retrying a
# rate-limited request immediately just spends more of the same limited
# budget; that case is raised as ApiRateLimitError for the caller to decide
# what to do (skip this run, not hammer the API three more times).
RETRY_POLICY = Retry(
    total=3,
    backoff_factor=2,  # 2s, 4s, 8s
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"],
)

_http = urllib3.PoolManager(retries=RETRY_POLICY)
_secrets_client = boto3.client("secretsmanager")


class ApiRateLimitError(Exception):
    """HTTP 429 — the key has hit its per-minute or per-day request cap.
    Worth retrying on the next scheduled run, not a data problem."""


class ApiAuthenticationError(Exception):
    """HTTP 401/403 — the key itself is invalid, expired, or doesn't cover
    this endpoint (fundamentals data is gated to paid plans on some
    providers). Distinct from a per-symbol problem on purpose: this means
    every other call in the same run will fail identically, so callers
    should treat it as terminal for the whole run instead of logging the
    same failure once per symbol and continuing to loop."""


class ApiInvalidRequestError(Exception):
    """HTTP 400/404, or a 200 response whose body says `"status": "error"`
    — a bad symbol or malformed parameter. Terminal for this one request;
    retrying the same bad symbol won't fix it."""


def get_api_key():
    """Load the Twelve Data API key from Secrets Manager (not an env var,
    so it never ends up printed in a log line or the Lambda console)."""
    response = _secrets_client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response["SecretString"])["api_key"]


def call_twelvedata(path, params, timeout=10.0):
    """GET request to a Twelve Data endpoint, decoded to a dict.

    Raises ApiRateLimitError / ApiAuthenticationError / ApiInvalidRequestError
    based on the HTTP status code (and the rare 200-with-error-body case);
    raises ConnectionError for transport-level failures (already retried
    per RETRY_POLICY by the time they get here). Returns the raw parsed
    dict on success for the caller to validate further — what counts as a
    complete response differs per endpoint (time series vs. profile vs.
    statistics), so that check stays with the caller.
    """
    url = f"{BASE_URL}/{path}"
    try:
        response = _http.request("GET", url, fields=params, timeout=timeout)
    except urllib3.exceptions.HTTPError as e:
        raise ConnectionError(f"Twelve Data request failed after retries: {e}") from e

    try:
        data = json.loads(response.data.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise ConnectionError(f"Non-JSON response from Twelve Data (status {response.status}): {e}") from e

    if response.status == 429:
        raise ApiRateLimitError(data.get("message", "rate limit exceeded"))
    if response.status in (401, 403):
        raise ApiAuthenticationError(data.get("message", f"HTTP {response.status}"))
    if response.status in (400, 404):
        raise ApiInvalidRequestError(data.get("message", f"HTTP {response.status}"))
    if response.status != 200:
        raise ConnectionError(f"Unexpected status {response.status} from Twelve Data: {data}")

    if data.get("status") == "error":
        # Twelve Data's own docs note this can happen on a 200, not just
        # on a 4xx — checked for explicitly rather than assumed away.
        raise ApiInvalidRequestError(data.get("message", "unknown error"))

    return data
