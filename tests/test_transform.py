import pytest

from lambda_transform import (
    DataQualityError,
    get_latest_price,
    validate_price_data,
)

GOOD_RECORD = {
    "1. open": "100.00",
    "2. high": "105.00",
    "3. low": "99.00",
    "4. close": "103.50",
    "5. volume": "1000000",
}


def test_get_latest_price_picks_most_recent_date():
    time_series = {
        "2026-08-05": {"4. close": "100"},
        "2026-08-07": {"4. close": "102"},
        "2026-08-06": {"4. close": "101"},
    }
    latest_date, record = get_latest_price(time_series)
    assert latest_date == "2026-08-07"
    assert record == {"4. close": "102"}


def test_validate_price_data_accepts_good_record():
    clean = validate_price_data("AAPL", "2026-08-07", GOOD_RECORD)
    assert clean == {
        "open_price": 100.00,
        "high_price": 105.00,
        "low_price": 99.00,
        "close_price": 103.50,
        "volume": 1000000,
    }


def test_validate_price_data_catches_missing_field():
    """Simulates the API silently dropping a field — should fail loudly
    with a message that names the missing field, not a bare KeyError."""
    broken = {k: v for k, v in GOOD_RECORD.items() if k != "5. volume"}
    with pytest.raises(DataQualityError, match="missing fields"):
        validate_price_data("AAPL", "2026-08-07", broken)


def test_validate_price_data_catches_renamed_field():
    """Simulates the API renaming a field (e.g. Alpha Vantage changing
    '5. volume' to '5. Volume') — this should be treated the same as a
    missing field, not silently pass through as a KeyError elsewhere."""
    renamed = dict(GOOD_RECORD)
    renamed["5. Volume"] = renamed.pop("5. volume")
    with pytest.raises(DataQualityError, match="missing fields"):
        validate_price_data("AAPL", "2026-08-07", renamed)


def test_validate_price_data_catches_high_below_low():
    bad = dict(GOOD_RECORD, **{"2. high": "50.00", "3. low": "99.00"})
    with pytest.raises(DataQualityError, match="high .* < low"):
        validate_price_data("AAPL", "2026-08-07", bad)


def test_validate_price_data_catches_non_positive_price():
    bad = dict(GOOD_RECORD, **{"1. open": "0"})
    with pytest.raises(DataQualityError, match="non-positive price"):
        validate_price_data("AAPL", "2026-08-07", bad)


def test_validate_price_data_catches_negative_volume():
    bad = dict(GOOD_RECORD, **{"5. volume": "-5"})
    with pytest.raises(DataQualityError, match="negative volume"):
        validate_price_data("AAPL", "2026-08-07", bad)


def test_validate_price_data_catches_non_numeric_value():
    bad = dict(GOOD_RECORD, **{"4. close": "N/A"})
    with pytest.raises(DataQualityError, match="non-numeric"):
        validate_price_data("AAPL", "2026-08-07", bad)
