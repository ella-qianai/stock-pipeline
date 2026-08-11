"""Single source of truth for which symbols this pipeline tracks.

Previously duplicated as a literal list in both lambda_function.py and
data_quality_check.py — if the tracked set changed, updating only one of
the two meant the quality check would silently compare against the wrong
universe of symbols.
"""

TRACKED_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
