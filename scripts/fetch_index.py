"""
Entry point: download the House filing index for the current year,
filter to PTRs, and print the most recent ones.

Run from the repo root:
    python scripts/fetch_index.py

Optional: pass a year to fetch a specific year:
    python scripts/fetch_index.py 2024
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.house import fetch_ptr_index  # noqa: E402


def main() -> None:
    if len(sys.argv) > 1:
        year = int(sys.argv[1])
    else:
        year = date.today().year

    data_dir = PROJECT_ROOT / "data"
    ptrs = fetch_ptr_index(year, data_dir)

    if not ptrs:
        print("\nNo PTRs found.")
        return

    ptrs_sorted = sorted(ptrs, key=lambda e: e.filing_date or date.min, reverse=True)
    sample = ptrs_sorted[:20]

    print(f"\nMost recent {len(sample)} PTRs:")
    for entry in sample:
        date_str = entry.filing_date.isoformat() if entry.filing_date else "          "
        print(
            f"  {date_str}  "
            f"{entry.display_name:<28}  "
            f"DocID: {entry.doc_id}  "
            f"({entry.state_district})"
        )


if __name__ == "__main__":
    main()
