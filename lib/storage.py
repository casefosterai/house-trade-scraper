"""
Persistent JSON storage for scraped trades.

Three files in data/:

  trades.json
    The accumulated list of all trades we've ever parsed. Append-only in
    practice (we dedupe by trade_id, so re-parsing a filing replaces its
    rows rather than duplicating them).

  seen_filings.json
    Map of doc_id -> ISO-format processed_at timestamp. Lets us skip
    filings we've already handled on subsequent runs.

  failed_filings.json
    Map of doc_id -> {reason, last_attempted_at}. So we don't keep
    re-downloading and re-trying scanned PDFs forever. Manually clear
    entries to retry (e.g. after we add OCR support).

All files are JSON for inspection — you can open them in any editor and
see what's there. We pretty-print with indent=2 so diffs in git are readable.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any


# Filenames (kept as constants so callers don't hardcode paths).
TRADES_FILENAME = "trades.json"
SEEN_FILINGS_FILENAME = "seen_filings.json"
FAILED_FILINGS_FILENAME = "failed_filings.json"


# ---------------------------------------------------------------------------
# Generic JSON load/save with safe defaults
# ---------------------------------------------------------------------------

def _load_json(path: Path, default: Any) -> Any:
    """Load a JSON file. Return `default` if the file doesn't exist."""
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, data: Any) -> None:
    """Write JSON to disk with pretty indent. Atomic-ish via temp file rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=_json_default)
        f.write("\n")  # trailing newline is conventional
    tmp_path.replace(path)


def _json_default(obj: Any) -> Any:
    """Convert non-JSON-native types (date, datetime) to ISO strings."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ---------------------------------------------------------------------------
# trades.json
# ---------------------------------------------------------------------------

def load_trades(data_dir: Path) -> list[dict]:
    """Return the list of all stored trades. Empty list if none yet."""
    return _load_json(data_dir / TRADES_FILENAME, default=[])


def save_trades(data_dir: Path, trades: list[dict]) -> None:
    """Overwrite the trades file with the given list."""
    _save_json(data_dir / TRADES_FILENAME, trades)


def merge_trades(existing: list[dict], new_trades: list[dict]) -> list[dict]:
    """Merge new_trades into existing, deduping by trade_id.

    If a trade_id appears in both, the new version wins (so re-parsing a
    filing with an improved parser replaces the old rows).

    Returned list is sorted by disclosure_date descending (newest first)
    for stable output.
    """
    by_id: dict[str, dict] = {t["trade_id"]: t for t in existing}
    for t in new_trades:
        by_id[t["trade_id"]] = t

    merged = list(by_id.values())
    # Sort by disclosure_date desc, then by trade_id for tie stability.
    # Treat None disclosure_date as the empty string so it sorts to the end.
    merged.sort(
        key=lambda t: (t.get("disclosure_date") or "", t["trade_id"]),
        reverse=True,
    )
    return merged


# ---------------------------------------------------------------------------
# seen_filings.json
# ---------------------------------------------------------------------------

def load_seen_filings(data_dir: Path) -> dict[str, str]:
    """Return {doc_id: iso_timestamp} of already-processed filings."""
    data = _load_json(data_dir / SEEN_FILINGS_FILENAME, default={})
    return data if isinstance(data, dict) else {}


def save_seen_filings(data_dir: Path, seen: dict[str, str]) -> None:
    _save_json(data_dir / SEEN_FILINGS_FILENAME, seen)


def mark_seen(seen: dict[str, str], doc_id: str) -> None:
    """Record that we've successfully processed this DocID. Mutates `seen`."""
    seen[doc_id] = datetime.now().isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# failed_filings.json
# ---------------------------------------------------------------------------

def load_failed_filings(data_dir: Path) -> dict[str, dict]:
    """Return {doc_id: {reason, last_attempted_at}}."""
    data = _load_json(data_dir / FAILED_FILINGS_FILENAME, default={})
    return data if isinstance(data, dict) else {}


def save_failed_filings(data_dir: Path, failed: dict[str, dict]) -> None:
    _save_json(data_dir / FAILED_FILINGS_FILENAME, failed)


def mark_failed(failed: dict[str, dict], doc_id: str, reason: str) -> None:
    """Record a parse/download failure. Mutates `failed`."""
    failed[doc_id] = {
        "reason": reason,
        "last_attempted_at": datetime.now().isoformat(timespec="seconds"),
    }
