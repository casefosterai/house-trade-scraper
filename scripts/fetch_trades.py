"""
Entry point: keep the local trades.json up to date with the House Clerk's index.

Default behavior:
  - Fetch the current year's index.
  - For every PTR not already in seen_filings.json, download the PDF and parse it.
  - Merge new trades into trades.json (deduped by trade_id).
  - Mark each filing as seen (or as failed, with reason).

Usage:
    python scripts/fetch_trades.py                # current year, all unseen filings
    python scripts/fetch_trades.py --year 2025    # specific year
    python scripts/fetch_trades.py --limit 10     # cap number of filings (testing)
    python scripts/fetch_trades.py --retry-failed # also retry previously-failed filings
    python scripts/fetch_trades.py --force        # reprocess even seen filings
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.house import (  # noqa: E402
    FilingIndexEntry,
    _http_session,
    download_ptr_pdf,
    fetch_ptr_index,
)
from lib.pdf_parser import ParsedTrade, parse_ptr_pdf  # noqa: E402
from lib.normalize import display_name, politician_id  # noqa: E402
from lib.storage import (  # noqa: E402
    load_failed_filings,
    load_seen_filings,
    load_trades,
    mark_failed,
    mark_seen,
    merge_trades,
    save_failed_filings,
    save_seen_filings,
    save_trades,
)

# Be polite between PDF downloads.
DOWNLOAD_DELAY_SECONDS = 0.3


def main() -> None:
    args = _parse_args()

    data_dir = PROJECT_ROOT / "data"
    pdfs_dir = data_dir / "raw" / "pdfs"

    # --- Load current state ---
    print("Loading existing state...")
    existing_trades = load_trades(data_dir)
    seen = load_seen_filings(data_dir)
    failed = load_failed_filings(data_dir)
    print(f"  {len(existing_trades):,} trades on disk")
    print(f"  {len(seen):,} filings previously processed")
    print(f"  {len(failed):,} filings previously failed")

    # --- Fetch the index ---
    print()
    ptrs = fetch_ptr_index(args.year, data_dir)
    if not ptrs:
        print("No PTRs in index. Nothing to do.")
        return

    # --- Decide what to process ---
    targets = _select_targets(ptrs, seen, failed, args)
    if not targets:
        print("\nNothing new to process. trades.json is up to date.")
        return

    # Sort newest-first so the most useful data lands first if we get interrupted.
    targets.sort(key=lambda e: e.filing_date or date.min, reverse=True)
    if args.limit:
        targets = targets[: args.limit]

    print(f"\nProcessing {len(targets):,} filings...")
    print("(PDFs cached in data/raw/pdfs/ — re-runs of the same DocID skip download.)\n")

    session = _http_session()
    new_trade_records: list[dict] = []
    succeeded = 0
    failures: list[tuple[FilingIndexEntry, str]] = []

    for i, entry in enumerate(targets, start=1):
        prefix = f"[{i}/{len(targets)}]"
        date_str = entry.filing_date.isoformat() if entry.filing_date else "          "
        print(f"{prefix} {date_str}  {entry.display_name}  (DocID {entry.doc_id})")

        # Download
        try:
            pdf_existed = (pdfs_dir / f"{entry.doc_id}.pdf").exists()
            pdf_path = download_ptr_pdf(entry, pdfs_dir, session=session)
            if not pdf_existed:
                time.sleep(DOWNLOAD_DELAY_SECONDS)
        except Exception as e:
            reason = f"download error: {e}"
            print(f"   {reason}")
            failures.append((entry, reason))
            mark_failed(failed, entry.doc_id, reason)
            continue

        # Parse
        result = parse_ptr_pdf(pdf_path, doc_id=entry.doc_id)
        if result.parse_failed:
            reason = f"parse failed: {result.failure_reason}"
            print(f"   {reason}")
            failures.append((entry, reason))
            mark_failed(failed, entry.doc_id, reason)
            continue

        # Build records and report
        records = [_build_trade_record(entry, t, idx) for idx, t in enumerate(result.trades)]
        new_trade_records.extend(records)
        mark_seen(seen, entry.doc_id)
        # If this filing previously failed and now succeeds, drop it from failed.
        failed.pop(entry.doc_id, None)
        succeeded += 1

        if records:
            print(f"   parsed {len(records)} trade(s)")
        else:
            # Successful parse but no trade rows — unusual but valid (e.g. an
            # empty PTR filed by mistake). We still mark it seen so we don't retry.
            print("   parsed OK with 0 trade rows")

    # --- Persist ---
    print("\nSaving...")
    if new_trade_records:
        merged = merge_trades(existing_trades, new_trade_records)
        save_trades(data_dir, merged)
        print(f"  trades.json: {len(merged):,} total ({len(new_trade_records):,} new this run)")
    else:
        print("  trades.json: no changes")

    save_seen_filings(data_dir, seen)
    save_failed_filings(data_dir, failed)
    print(f"  seen_filings.json: {len(seen):,} entries")
    print(f"  failed_filings.json: {len(failed):,} entries")

    # --- Summary ---
    print(
        f"\nDone. {succeeded:,} succeeded, "
        f"{len(failures):,} failed, "
        f"{len(new_trade_records):,} trades extracted."
    )
    if failures:
        print("\nFailures this run:")
        for entry, reason in failures:
            print(f"  - {entry.display_name} (DocID {entry.doc_id}): {reason}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape House PTR filings.")
    parser.add_argument(
        "--year", type=int, default=date.today().year,
        help="Filing year to fetch (default: current year)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap number of filings processed (default: all unseen)",
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="Also retry filings in failed_filings.json",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Reprocess every PTR including ones in seen_filings.json",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _select_targets(
    ptrs: list[FilingIndexEntry],
    seen: dict[str, str],
    failed: dict[str, dict],
    args: argparse.Namespace,
) -> list[FilingIndexEntry]:
    """Decide which filings to process based on the flags + state files."""
    if args.force:
        return ptrs

    targets: list[FilingIndexEntry] = []
    for entry in ptrs:
        if entry.doc_id in seen:
            continue
        if entry.doc_id in failed and not args.retry_failed:
            continue
        targets.append(entry)
    return targets


def _build_trade_record(
    entry: FilingIndexEntry,
    trade: ParsedTrade,
    idx_within_filing: int,
) -> dict:
    """Combine index metadata + parsed trade into the JSON record we store.

    `idx_within_filing` is the trade's 0-based position in the filing,
    used to make trade_id unique across rows in a multi-trade PTR.
    """
    name = display_name(entry.first_name, entry.last_name)
    pid = politician_id(entry.first_name, entry.last_name, entry.state_district)

    return {
        # Politician / filing context
        "politician_id": pid,
        "politician_name": name,
        "chamber": "house",
        "state_district": entry.state_district,
        "doc_id": entry.doc_id,
        "source_url": entry.pdf_url,

        # Trade data
        "owner": trade.owner,
        "asset_description": trade.asset_description,
        "ticker": trade.ticker,
        "asset_type": trade.asset_type,
        "transaction_type": trade.transaction_type,
        "transaction_date": trade.transaction_date.isoformat() if trade.transaction_date else None,
        "disclosure_date": trade.disclosure_date.isoformat() if trade.disclosure_date else None,
        "amount_min": trade.amount_min,
        "amount_max": trade.amount_max,
        "filing_status": trade.filing_status,
        "description": trade.description,
        "comments": trade.comments,
        "subholding_of": trade.subholding_of,

        # Stable id for dedup. DocID + index = unique within all of House.
        "trade_id": f"{entry.doc_id}-{idx_within_filing}",
    }


if __name__ == "__main__":
    main()
