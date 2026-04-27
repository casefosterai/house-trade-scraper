"""
Debug helper: dump the raw text and table structure of one cached PDF.

Usage (from repo root, with venv active):
    python scripts/debug_pdf.py 20034401

Assumes the PDF has already been downloaded to data/raw/pdfs/{doc_id}.pdf
by a prior fetch_trades.py run.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pdfplumber  # noqa: E402


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/debug_pdf.py <doc_id>")
        sys.exit(1)

    doc_id = sys.argv[1]
    pdf_path = PROJECT_ROOT / "data" / "raw" / "pdfs" / f"{doc_id}.pdf"
    if not pdf_path.exists():
        print(f"PDF not found at {pdf_path}")
        print("Run scripts/fetch_trades.py first so it's cached.")
        sys.exit(1)

    print(f"=== Debugging {pdf_path.name} ===\n")

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            print(f"\n--- PAGE {page_num} ---")

            print("\n[ extract_text() output, line by line ]")
            text = page.extract_text() or ""
            for i, line in enumerate(text.splitlines()):
                print(f"  {i:3d}: {line!r}")

            print("\n[ extract_tables() output ]")
            tables = page.extract_tables()
            if not tables:
                print("  (no tables detected)")
            for t_idx, table in enumerate(tables):
                print(f"  Table #{t_idx} ({len(table)} rows):")
                for r_idx, row in enumerate(table):
                    print(f"    row {r_idx}: {row}")


if __name__ == "__main__":
    main()
