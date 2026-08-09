from datetime import date

from data_quality_check import (
    TRACKED_SYMBOLS,
    check_completeness,
    check_freshness,
    check_sanity,
)


class FakeCursor:
    """Minimal stand-in for a psycopg2 cursor: records the last query and
    returns pre-set rows, so these checks can be tested without a real
    Postgres connection."""

    def __init__(self, rows):
        self._rows = rows
        self.last_query = None
        self.last_params = None

    def execute(self, query, params=None):
        self.last_query = query
        self.last_params = params

    def fetchall(self):
        return self._rows


def test_check_freshness_passes_when_all_symbols_present():
    today = date(2026, 8, 7)
    cursor = FakeCursor(rows=[(s,) for s in TRACKED_SYMBOLS])
    result = check_freshness(cursor, today)
    assert result["passed"] is True


def test_check_freshness_fails_when_symbol_missing():
    today = date(2026, 8, 7)
    present = TRACKED_SYMBOLS[:-1]  # drop the last symbol
    cursor = FakeCursor(rows=[(s,) for s in present])
    result = check_freshness(cursor, today)
    assert result["passed"] is False
    assert TRACKED_SYMBOLS[-1] in result["detail"]


def test_check_completeness_fails_for_sparse_symbol():
    # AAPL has 5 rows in the lookback window, MSFT only has 1 — MSFT
    # should get flagged as unexpectedly sparse.
    cursor = FakeCursor(rows=[("AAPL", 5), ("MSFT", 1)])
    result = check_completeness(cursor, lookback_days=7)
    assert result["passed"] is False
    assert "MSFT" in result["detail"]


def test_check_completeness_passes_when_all_well_covered():
    cursor = FakeCursor(rows=[(s, 5) for s in TRACKED_SYMBOLS])
    result = check_completeness(cursor, lookback_days=7)
    assert result["passed"] is True


def test_check_sanity_passes_with_no_bad_rows():
    cursor = FakeCursor(rows=[])
    result = check_sanity(cursor)
    assert result["passed"] is True


def test_check_sanity_fails_when_bad_rows_found():
    cursor = FakeCursor(rows=[("AAPL", date(2026, 8, 7))])
    result = check_sanity(cursor)
    assert result["passed"] is False
    assert "1 rows" in result["detail"]
