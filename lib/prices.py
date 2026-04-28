"""
Stock price fetching via yfinance, with aggressive on-disk caching.

Yahoo Finance's unofficial API (via yfinance) is free and rate-limited.
We minimize requests by:

  1) Caching every (ticker, date) lookup to data/price_cache.json forever.
     A historical close price never changes, so we never re-fetch.

  2) Caching "today's price" too, but with a date stamp so it gets refreshed
     once per day (i.e. any lookup of "today's price" for ticker X on a
     given calendar day is cached for that day).

  3) Caching the "ticker not found" result as well, so we don't keep
     hammering Yahoo for tickers that don't exist (delisted, foreign,
     mistyped). These can be re-checked manually by clearing the cache.

The cache key is "{TICKER}|{YYYY-MM-DD}" -> {"close": float | null}.
A null close means we tried and got nothing back; treat as unpriceable.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# yfinance is imported lazily inside fetch functions so import errors during
# scraper-only runs don't block the Step 1/2/3 scripts.


CACHE_FILENAME = "price_cache.json"

# Pause between Yahoo requests to be polite. yfinance batches internally
# but we add this between distinct ticker requests as insurance.
INTER_REQUEST_DELAY_SECONDS = 0.15


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------

def load_cache(data_dir: Path) -> dict[str, dict | None]:
    """Load the price cache from disk. Returns {} if no cache yet."""
    path = data_dir / CACHE_FILENAME
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_cache(data_dir: Path, cache: dict[str, dict | None]) -> None:
    """Persist the price cache to disk. Atomic via temp-file rename."""
    path = data_dir / CACHE_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
        f.write("\n")
    tmp.replace(path)


def _key(ticker: str, d: date) -> str:
    return f"{ticker.upper()}|{d.isoformat()}"


# ---------------------------------------------------------------------------
# Public price-lookup API
# ---------------------------------------------------------------------------

def get_close_price(
    ticker: str,
    target_date: date,
    cache: dict[str, dict | None],
    *,
    fetch_window_days: int = 5,
) -> float | None:
    """Return the close price for `ticker` on `target_date`.

    If the date is a weekend or market holiday, Yahoo won't have data for
    that exact day. We accept the closest *prior* trading day within
    fetch_window_days as a substitute (this is the standard "what would
    retail have paid?" approximation since you can't trade on a weekend).

    Returns None if the ticker is unknown to Yahoo OR no trading data is
    available within the window.

    Mutates `cache` with new lookups so callers can save it after a batch.
    """
    cache_key = _key(ticker, target_date)
    if cache_key in cache:
        cached = cache[cache_key]
        return cached.get("close") if isinstance(cached, dict) else None

    # Lazy import so other scripts that don't need yfinance don't pay
    # the import cost (and so missing yfinance doesn't break Step 1-3).
    import yfinance as yf

    # Pull a small window around the target date and find the closest
    # available trading day at or before the target.
    start = target_date - timedelta(days=fetch_window_days)
    end = target_date + timedelta(days=1)  # yfinance end is exclusive

    try:
        history = yf.Ticker(ticker).history(
            start=start.isoformat(),
            end=end.isoformat(),
            auto_adjust=True,  # adjusts for splits & dividends so returns are apples-to-apples
        )
    except Exception:
        # yfinance can throw on transient network errors. Treat as unpriceable
        # for this run; do NOT cache the failure so a later run can retry.
        return None
    finally:
        time.sleep(INTER_REQUEST_DELAY_SECONDS)

    if history is None or history.empty:
        # Permanent miss for the (ticker, date) combo within this window.
        # Cache it so we don't keep hammering Yahoo for the same dead ticker.
        cache[cache_key] = {"close": None}
        return None

    # history is a DataFrame indexed by date. We want the latest row at or
    # before target_date.
    target_ts = datetime.combine(target_date, datetime.min.time())
    eligible = history[history.index.tz_localize(None) <= target_ts]
    if eligible.empty:
        cache[cache_key] = {"close": None}
        return None

    close = float(eligible.iloc[-1]["Close"])
    cache[cache_key] = {"close": close}
    return close


def get_current_close(
    ticker: str,
    cache: dict[str, dict | None],
    today: date | None = None,
) -> float | None:
    """Return the most recent close price for `ticker`.

    Cached per calendar day so we don't re-fetch the same "today" twice.
    """
    if today is None:
        today = date.today()
    return get_close_price(ticker, today, cache, fetch_window_days=10)
