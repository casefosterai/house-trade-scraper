"""
Compute per-politician return metrics from trades.json and write returns.json.

Input:
    data/trades.json          (raw trade records from the scraper)
    data/price_cache.json     (cached stock prices, persisted between runs)

Output:
    data/returns.json         (one record per politician with full breakdown)
    data/price_cache.json     (updated with any new lookups)

Pipeline:
    1. Match every politician's trades into closed positions, open positions,
       and unmatched sales (via FIFO within position lineages).
    2. Fetch the close price on each disclosure_date and the current close
       for every ticker we need to value.
    3. Compute realized return % per closed position (using disclosure-date
       prices for both entry and exit — the "retail-replicable" anchor).
       Also compute the politician's actual realized return (using
       transaction-date prices) — both are useful, you asked for both.
    4. Compute mark-to-market return % per open position (disclosure-date
       cost basis vs. today's close).
    5. Aggregate per politician — total return %, win rate, trade counts,
       total exposure, etc. — for both closed and open buckets.
    6. Write data/returns.json with the full breakdown.

Usage:
    python scripts/compute_returns.py
    python scripts/compute_returns.py --min-trades 5     # default for leaderboard
    python scripts/compute_returns.py --no-current       # skip today's-price lookups
    python scripts/compute_returns.py --tickers AAPL,MSFT  # only process specific tickers (debug)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lib.positions import (  # noqa: E402
    ClosedPosition,
    MatchResult,
    OpenPosition,
    UnmatchedSale,
    match_trades,
    trade_size_usd,
)
from lib.prices import get_close_price, get_current_close, load_cache, save_cache  # noqa: E402
from lib.storage import load_trades  # noqa: E402
from lib.legislators import load_party_lookup, party_for_state_district  # noqa: E402

OUTPUT_FILENAME = "returns.json"

# Default leaderboard threshold. Politicians with fewer CLOSED positions
# (not raw trade count) than this are excluded from the leaderboard. The
# rationale: a return % computed from 1-2 closed trades is statistical
# noise. Requiring N closed positions ensures the rank means something.
# Open-only politicians (lots of open positions, nothing closed yet) get
# a "watchlist" ranking instead — see open-positions logic below.
DEFAULT_MIN_CLOSED_FOR_LEADERBOARD = 5


def main() -> None:
    args = _parse_args()
    data_dir = PROJECT_ROOT / "data"

    print("Loading trades...")
    trades = load_trades(data_dir)
    if not trades:
        print("No trades.json found. Run scripts/fetch_trades.py first.")
        return
    print(f"  {len(trades):,} total trade records")

    skipped_breakdown: dict[str, int] = defaultdict(int)
    for t in trades:
        if t.get("skipped"):
            skipped_breakdown[t.get("skip_reason") or "unknown"] += 1
    if skipped_breakdown:
        print("  skipped breakdown:")
        for reason, count in sorted(skipped_breakdown.items()):
            print(f"    {count:5d}  {reason}")

    if args.tickers:
        wanted = {t.strip().upper() for t in args.tickers.split(",")}
        trades = [t for t in trades if (t.get("ticker") or "").upper() in wanted]
        print(f"  filtered to {len(trades):,} trades for tickers: {sorted(wanted)}")

    print("\nMatching positions (FIFO within lineages)...")
    matched = match_trades(trades)
    print(f"  closed positions:   {len(matched.closed):,}")
    print(f"  open positions:     {len(matched.open):,}")
    print(f"  unmatched sales:    {len(matched.unmatched_sales):,}")

    # --- Determine which (ticker, date) prices we need ---
    cache = load_cache(data_dir)
    print(f"\nPrice cache: {len(cache):,} entries on disk")

    needed_lookups = _collect_needed_lookups(matched)
    print(f"  needed lookups for this run: {len(needed_lookups):,}")

    cache_hits = sum(1 for k in needed_lookups if k in cache)
    cache_misses = len(needed_lookups) - cache_hits
    print(f"  cache hits: {cache_hits:,}, misses (will fetch): {cache_misses:,}")

    # --- Fetch prices ---
    print("\nFetching prices...")
    today = date.today()
    fetched_count = 0
    for i, key in enumerate(needed_lookups, 1):
        ticker, date_str = key.split("|")
        target_date = date.fromisoformat(date_str)
        if key in cache:
            continue
        get_close_price(ticker, target_date, cache)
        fetched_count += 1
        if fetched_count % 50 == 0:
            print(f"  ...fetched {fetched_count}/{cache_misses} new prices")
            save_cache(data_dir, cache)  # incremental save

    if not args.no_current:
        # Also fetch today's price for every ticker with an open position.
        open_tickers = sorted({op.ticker for op in matched.open})
        print(f"\nFetching current prices for {len(open_tickers):,} open-position tickers...")
        for i, ticker in enumerate(open_tickers, 1):
            get_current_close(ticker, cache, today=today)
            if i % 50 == 0:
                print(f"  ...{i}/{len(open_tickers)}")
                save_cache(data_dir, cache)

    save_cache(data_dir, cache)
    print(f"\nPrice cache: {len(cache):,} entries after run")

    # --- Compute per-position returns ---
    print("\nComputing per-position returns...")
    closed_returns = [_closed_position_return(c, cache) for c in matched.closed]
    open_returns = [_open_position_return(o, cache, today) for o in matched.open]

    # --- Aggregate per politician ---
    print("\nLoading party affiliations...")
    party_lookup = load_party_lookup(data_dir)
    print(f"  loaded {len(party_lookup)} House member party records")

    print("\nAggregating per-politician metrics...")
    politicians = _aggregate(
        trades=trades,
        closed=matched.closed,
        closed_returns=closed_returns,
        open_positions=matched.open,
        open_returns=open_returns,
        unmatched_sales=matched.unmatched_sales,
        skipped_breakdown=skipped_breakdown,
        party_lookup=party_lookup,
    )
    print(f"  {len(politicians):,} politicians with at least one trade record")

    # --- Build leaderboard ---
    # Note: "leaderboard" here means "qualifies for the closed-return ranking".
    # Politicians with only open positions still appear in the JSON; the webapp
    # can show them on their own page or in a separate "watchlist" view.
    leaderboard = [
        p for p in politicians
        if p["closed_trade_count"] >= args.min_closed
    ]
    leaderboard.sort(
        key=lambda p: p["closed_return_disclosure_pct"]
        if p["closed_return_disclosure_pct"] is not None
        else -1e9,
        reverse=True,
    )
    print(f"  {len(leaderboard):,} qualify for leaderboard (min {args.min_closed} closed positions)")

    # --- Write output ---
    output = {
        "generated_at": today.isoformat(),
        "min_closed_for_leaderboard": args.min_closed,
        "totals": {
            "trade_records": len(trades),
            "closed_positions": len(matched.closed),
            "open_positions": len(matched.open),
            "unmatched_sales": len(matched.unmatched_sales),
            "skipped": dict(skipped_breakdown),
            "politicians_total": len(politicians),
            "politicians_on_leaderboard": len(leaderboard),
        },
        "politicians": politicians,
    }
    output_path = data_dir / OUTPUT_FILENAME
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=_json_default)
        f.write("\n")
    print(f"\nWrote {output_path} ({output_path.stat().st_size:,} bytes)")


# ---------------------------------------------------------------------------
# Lookup planning
# ---------------------------------------------------------------------------

def _collect_needed_lookups(matched: MatchResult) -> list[str]:
    """Return the list of '{TICKER}|{YYYY-MM-DD}' keys we need to price."""
    needed: set[str] = set()

    for c in matched.closed:
        # Both entry and exit on disclosure dates (the retail anchor).
        needed.add(f"{c.ticker}|{c.purchase_disclosure_date.isoformat()}")
        needed.add(f"{c.ticker}|{c.sale_disclosure_date.isoformat()}")
        # Also the politician's actual transaction-date prices (for the
        # "their actual return" bonus metric).
        needed.add(f"{c.ticker}|{c.purchase_date.isoformat()}")
        needed.add(f"{c.ticker}|{c.sale_date.isoformat()}")

    for o in matched.open:
        needed.add(f"{o.ticker}|{o.purchase_disclosure_date.isoformat()}")
        needed.add(f"{o.ticker}|{o.purchase_date.isoformat()}")

    return sorted(needed)


# ---------------------------------------------------------------------------
# Per-position return calculation
# ---------------------------------------------------------------------------

def _closed_position_return(c: ClosedPosition, cache: dict) -> dict:
    """Compute return % for one closed position using both price anchors."""
    entry_disc = _cached_close(cache, c.ticker, c.purchase_disclosure_date)
    exit_disc = _cached_close(cache, c.ticker, c.sale_disclosure_date)
    entry_txn = _cached_close(cache, c.ticker, c.purchase_date)
    exit_txn = _cached_close(cache, c.ticker, c.sale_date)

    return {
        "ticker": c.ticker,
        "asset_description": c.asset_description,
        "owner": c.owner,
        "subholding_of": c.subholding_of,
        "purchase_trade_id": c.purchase_trade_id,
        "sale_trade_id": c.sale_trade_id,
        "purchase_date": c.purchase_date.isoformat(),
        "purchase_disclosure_date": c.purchase_disclosure_date.isoformat(),
        "sale_date": c.sale_date.isoformat(),
        "sale_disclosure_date": c.sale_disclosure_date.isoformat(),
        "cost_basis_usd": c.cost_basis_usd,
        "closed_via": c.closed_via,
        "return_disclosure_pct": _pct_change(entry_disc, exit_disc),
        "return_transaction_pct": _pct_change(entry_txn, exit_txn),
        "priceable": entry_disc is not None and exit_disc is not None,
    }


def _open_position_return(o: OpenPosition, cache: dict, today: date) -> dict:
    """Compute mark-to-market return % for one open position."""
    entry_disc = _cached_close(cache, o.ticker, o.purchase_disclosure_date)
    entry_txn = _cached_close(cache, o.ticker, o.purchase_date)
    current = _cached_close(cache, o.ticker, today)

    return {
        "ticker": o.ticker,
        "asset_description": o.asset_description,
        "owner": o.owner,
        "subholding_of": o.subholding_of,
        "purchase_trade_id": o.purchase_trade_id,
        "purchase_date": o.purchase_date.isoformat(),
        "purchase_disclosure_date": o.purchase_disclosure_date.isoformat(),
        "cost_basis_usd": o.cost_basis_usd,
        "mtm_return_disclosure_pct": _pct_change(entry_disc, current),
        "mtm_return_transaction_pct": _pct_change(entry_txn, current),
        "priceable": entry_disc is not None and current is not None,
    }


def _cached_close(cache: dict, ticker: str, d: date) -> float | None:
    entry = cache.get(f"{ticker}|{d.isoformat()}")
    if not isinstance(entry, dict):
        return None
    return entry.get("close")


def _pct_change(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start == 0:
        return None
    return round((end - start) / start * 100, 2)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _aggregate(
    *,
    trades: list[dict],
    closed: list[ClosedPosition],
    closed_returns: list[dict],
    open_positions: list[OpenPosition],
    open_returns: list[dict],
    unmatched_sales: list[UnmatchedSale],
    skipped_breakdown: dict[str, int],
    party_lookup: dict[tuple[str, str], str],
) -> list[dict]:
    """Produce one record per politician with all metrics."""
    # Build politician metadata from raw trades (so politicians with only
    # skipped trades still appear in the output).
    metadata: dict[str, dict] = {}
    for t in trades:
        pid = t["politician_id"]
        if pid not in metadata:
            metadata[pid] = {
                "politician_id": pid,
                "politician_name": t["politician_name"],
                "chamber": t["chamber"],
                "state_district": t["state_district"],
                "party": party_for_state_district(t["state_district"], party_lookup),
            }

    # Index the per-position results back to politicians.
    closed_by_pid: dict[str, list[dict]] = defaultdict(list)
    for c, ret in zip(closed, closed_returns):
        closed_by_pid[c.politician_id].append({**ret, "_cost_basis": c.cost_basis_usd})

    open_by_pid: dict[str, list[dict]] = defaultdict(list)
    for o, ret in zip(open_positions, open_returns):
        open_by_pid[o.politician_id].append({**ret, "_cost_basis": o.cost_basis_usd})

    unmatched_by_pid: dict[str, list[dict]] = defaultdict(list)
    for u in unmatched_sales:
        unmatched_by_pid[u.politician_id].append({
            "ticker": u.ticker,
            "asset_description": u.asset_description,
            "owner": u.owner,
            "subholding_of": u.subholding_of,
            "sale_trade_id": u.sale_trade_id,
            "sale_date": u.sale_date.isoformat(),
            "sale_disclosure_date": u.sale_disclosure_date.isoformat(),
            "proceeds_usd": u.proceeds_usd,
        })

    # Excluded-by-skip-reason counts per politician (for transparency).
    excluded_by_pid: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for t in trades:
        if t.get("skipped"):
            excluded_by_pid[t["politician_id"]][t.get("skip_reason") or "unknown"] += 1

    # Untracked-ticker counts per politician (couldn't price).
    untracked_by_pid: dict[str, set[str]] = defaultdict(set)
    for c, ret in zip(closed, closed_returns):
        if not ret["priceable"]:
            untracked_by_pid[c.politician_id].add(c.ticker)
    for o, ret in zip(open_positions, open_returns):
        if not ret["priceable"]:
            untracked_by_pid[o.politician_id].add(o.ticker)

    # Build the per-politician record.
    politicians: list[dict] = []
    for pid, meta in metadata.items():
        c_list = closed_by_pid.get(pid, [])
        o_list = open_by_pid.get(pid, [])
        u_list = unmatched_by_pid.get(pid, [])

        # ---- Closed metrics (priceable only) ----
        c_priceable = [c for c in c_list if c["priceable"] and c["return_disclosure_pct"] is not None]
        closed_count = len(c_list)
        closed_priceable_count = len(c_priceable)

        if c_priceable:
            total_cost = sum(c["_cost_basis"] for c in c_priceable)
            # Dollar-weighted return: weight each return by its cost basis.
            wt_disc = sum(c["return_disclosure_pct"] * c["_cost_basis"] for c in c_priceable) / total_cost
            wt_txn = sum(
                (c["return_transaction_pct"] or 0) * c["_cost_basis"]
                for c in c_priceable
                if c["return_transaction_pct"] is not None
            ) / total_cost if any(c["return_transaction_pct"] is not None for c in c_priceable) else None
            wins = sum(1 for c in c_priceable if c["return_disclosure_pct"] > 0)
            win_rate = round(wins / len(c_priceable) * 100, 1)
            avg_return = round(sum(c["return_disclosure_pct"] for c in c_priceable) / len(c_priceable), 2)
            closed_total_cost = round(total_cost, 0)
        else:
            wt_disc = None
            wt_txn = None
            win_rate = None
            avg_return = None
            closed_total_cost = 0

        # ---- Open metrics (priceable only) ----
        o_priceable = [o for o in o_list if o["priceable"] and o["mtm_return_disclosure_pct"] is not None]
        open_count = len(o_list)
        open_priceable_count = len(o_priceable)

        if o_priceable:
            total_open_cost = sum(o["_cost_basis"] for o in o_priceable)
            mtm_wt = sum(o["mtm_return_disclosure_pct"] * o["_cost_basis"] for o in o_priceable) / total_open_cost
            open_total_cost = round(total_open_cost, 0)
        else:
            mtm_wt = None
            open_total_cost = 0

        politicians.append({
            **meta,

            # Headline metrics
            "closed_return_disclosure_pct":
                round(wt_disc, 2) if wt_disc is not None else None,
            "closed_return_transaction_pct":
                round(wt_txn, 2) if wt_txn is not None else None,
            "closed_win_rate_pct": win_rate,
            "closed_avg_return_pct": avg_return,
            "closed_trade_count": closed_count,
            "closed_priceable_count": closed_priceable_count,
            "closed_total_cost_basis_usd": closed_total_cost,

            "open_mtm_return_disclosure_pct":
                round(mtm_wt, 2) if mtm_wt is not None else None,
            "open_position_count": open_count,
            "open_priceable_count": open_priceable_count,
            "open_total_cost_basis_usd": open_total_cost,

            # Transparency
            "unmatched_sale_count": len(u_list),
            "untracked_tickers": sorted(untracked_by_pid.get(pid, [])),
            "excluded_by_reason": dict(excluded_by_pid.get(pid, {})),

            # Drill-down (used by per-politician detail page)
            "closed_positions": [
                {k: v for k, v in c.items() if not k.startswith("_")} for c in c_list
            ],
            "open_positions": [
                {k: v for k, v in o.items() if not k.startswith("_")} for o in o_list
            ],
            "unmatched_sales": u_list,
        })

    # Sort by politician name as a stable default ordering
    politicians.sort(key=lambda p: p["politician_name"])
    return politicians


# ---------------------------------------------------------------------------
# Argument parsing & misc
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute return metrics from trades.json")
    p.add_argument("--min-closed", type=int, default=DEFAULT_MIN_CLOSED_FOR_LEADERBOARD,
                   help="Minimum closed positions to qualify for the leaderboard")
    p.add_argument("--no-current", action="store_true",
                   help="Skip today's-price lookups (only compute closed positions)")
    p.add_argument("--tickers", type=str, default=None,
                   help="Comma-separated ticker list to limit processing (debug)")
    return p.parse_args()


def _json_default(obj: Any) -> Any:
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


if __name__ == "__main__":
    main()
