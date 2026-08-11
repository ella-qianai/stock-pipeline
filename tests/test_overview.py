import pytest

from lambda_overview import (
    OverviewValidationError,
    upsert_company_overview,
    validate_overview_data,
)

# Shapes taken from real /profile and /statistics responses (verified
# against Twelve Data's public demo key for AAPL), trimmed to the fields
# this pipeline actually reads.
GOOD_PROFILE = {
    "symbol": "AAPL",
    "name": "Apple Inc.",
    "sector": "Technology",
}

GOOD_STATISTICS = {
    "statistics": {
        "valuations_metrics": {
            "market_capitalization": 3000000000000,
            "trailing_pe": 28.5,
        },
        "dividends_and_splits": {
            "trailing_annual_dividend_yield": 0.005,
        },
    }
}


class FakeCursor:
    """Minimal stand-in for a psycopg2 cursor — same pattern as
    tests/test_quality.py's FakeCursor, just recording the last query."""

    def __init__(self):
        self.last_query = None
        self.last_params = None

    def execute(self, query, params=None):
        self.last_query = query
        self.last_params = params


def test_validate_overview_data_accepts_good_record():
    clean = validate_overview_data("AAPL", GOOD_PROFILE, GOOD_STATISTICS)
    assert clean == {
        "company_name": "Apple Inc.",
        "sector": "Technology",
        "market_cap": 3000000000000,
        "pe_ratio": 28.5,
        "dividend_yield": 0.005,
    }


def test_validate_overview_data_catches_missing_field():
    broken = {k: v for k, v in GOOD_PROFILE.items() if k != "name"}
    with pytest.raises(OverviewValidationError, match="missing fields"):
        validate_overview_data("AAPL", broken, GOOD_STATISTICS)


def test_validate_overview_data_catches_symbol_mismatch():
    """Defends against a response being attributed to the wrong symbol —
    e.g. an upstream caching bug returning stale data for a different
    ticker than the one actually requested."""
    mismatched = dict(GOOD_PROFILE, symbol="MSFT")
    with pytest.raises(OverviewValidationError, match="expected 'AAPL'"):
        validate_overview_data("AAPL", mismatched, GOOD_STATISTICS)


def test_validate_overview_data_allows_missing_sector():
    """Sector is genuinely absent for some instrument types — that's not a
    schema-drift signal the way a missing name/symbol would be."""
    no_sector = {k: v for k, v in GOOD_PROFILE.items() if k != "sector"}
    clean = validate_overview_data("AAPL", no_sector, GOOD_STATISTICS)
    assert clean["sector"] is None


def test_validate_overview_data_treats_missing_dividend_block_as_null():
    """A stock that's never paid a dividend simply has no
    "dividends_and_splits" block at all in /statistics — that's a real
    business fact, not a data-quality problem, and should become NULL
    rather than raising or crashing on a missing nested key."""
    no_dividends = {
        "statistics": {
            "valuations_metrics": GOOD_STATISTICS["statistics"]["valuations_metrics"],
        }
    }
    clean = validate_overview_data("AAPL", GOOD_PROFILE, no_dividends)
    assert clean["dividend_yield"] is None


def test_validate_overview_data_catches_negative_market_cap():
    bad_stats = {
        "statistics": {
            "valuations_metrics": {"market_capitalization": -100, "trailing_pe": 28.5},
            "dividends_and_splits": {"trailing_annual_dividend_yield": 0.005},
        }
    }
    with pytest.raises(OverviewValidationError, match="negative market cap"):
        validate_overview_data("AAPL", GOOD_PROFILE, bad_stats)


def test_upsert_company_overview_writes_expected_params():
    cursor = FakeCursor()
    clean = validate_overview_data("AAPL", GOOD_PROFILE, GOOD_STATISTICS)
    upsert_company_overview(cursor, "AAPL", clean)
    assert cursor.last_params[0] == "AAPL"
    assert "ON CONFLICT (symbol)" in cursor.last_query
