import json

import pytest

import twelvedata_client
from twelvedata_client import (
    ApiAuthenticationError,
    ApiInvalidRequestError,
    ApiRateLimitError,
    call_twelvedata,
)


class FakeResponse:
    """Minimal stand-in for a urllib3 HTTPResponse: just the two attributes
    call_twelvedata reads, so these tests don't need a live network call or
    a real Twelve Data key."""

    def __init__(self, status, body: dict):
        self.status = status
        self.data = json.dumps(body).encode("utf-8")


def _patch_http(monkeypatch, response):
    monkeypatch.setattr(twelvedata_client._http, "request", lambda *a, **k: response)


def test_call_twelvedata_returns_data_on_success(monkeypatch):
    _patch_http(monkeypatch, FakeResponse(200, {"status": "ok", "values": []}))
    data = call_twelvedata("time_series", {"symbol": "AAPL"})
    assert data["status"] == "ok"


def test_call_twelvedata_raises_rate_limit_on_429(monkeypatch):
    _patch_http(monkeypatch, FakeResponse(429, {"code": 429, "message": "too many requests", "status": "error"}))
    with pytest.raises(ApiRateLimitError):
        call_twelvedata("time_series", {"symbol": "AAPL"})


def test_call_twelvedata_raises_authentication_error_on_401(monkeypatch):
    _patch_http(monkeypatch, FakeResponse(401, {"code": 401, "message": "invalid apikey", "status": "error"}))
    with pytest.raises(ApiAuthenticationError, match="invalid apikey"):
        call_twelvedata("time_series", {"symbol": "AAPL"})


def test_call_twelvedata_raises_authentication_error_on_403(monkeypatch):
    """A 403 on Twelve Data can mean "your plan doesn't cover this
    endpoint" (e.g. fundamentals data on the free tier) as well as a bad
    key outright — both are terminal-for-the-run auth problems, not a
    per-symbol issue, so both map to the same exception."""
    _patch_http(monkeypatch, FakeResponse(403, {"code": 403, "message": "plan does not include this endpoint", "status": "error"}))
    with pytest.raises(ApiAuthenticationError):
        call_twelvedata("statistics", {"symbol": "AAPL"})


def test_call_twelvedata_raises_invalid_request_on_400(monkeypatch):
    _patch_http(monkeypatch, FakeResponse(400, {"code": 400, "message": "invalid symbol", "status": "error"}))
    with pytest.raises(ApiInvalidRequestError, match="invalid symbol"):
        call_twelvedata("time_series", {"symbol": "NOTASYMBOL"})


def test_call_twelvedata_raises_invalid_request_on_404(monkeypatch):
    _patch_http(monkeypatch, FakeResponse(404, {"code": 404, "message": "not found", "status": "error"}))
    with pytest.raises(ApiInvalidRequestError):
        call_twelvedata("time_series", {"symbol": "NOTASYMBOL"})


def test_call_twelvedata_raises_invalid_request_on_200_with_error_status(monkeypatch):
    """Twelve Data's own docs note a 200 can still carry `"status":
    "error"` in the body — checked for explicitly rather than assumed to
    only ever come with a 4xx status code."""
    _patch_http(monkeypatch, FakeResponse(200, {"status": "error", "message": "no data"}))
    with pytest.raises(ApiInvalidRequestError, match="no data"):
        call_twelvedata("time_series", {"symbol": "AAPL"})


def test_call_twelvedata_raises_connection_error_on_unexpected_status(monkeypatch):
    _patch_http(monkeypatch, FakeResponse(503, {}))
    with pytest.raises(ConnectionError):
        call_twelvedata("time_series", {"symbol": "AAPL"})
