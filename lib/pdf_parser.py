"""
Parse trade rows out of a House Periodic Transaction Report PDF.

The Clerk's PTR PDFs have a Transactions table with these columns:

    ID | Owner | Asset | Transaction Type | Date | Notification Date |
    Amount | Cap. Gains > $200?

Below each row, in smaller text, there are optional sub-fields:
    F: Filing Status (e.g. "New", "Amendment")
    S: Subholding (rare)
    D: Description (e.g. "dividend reinvestment")
    C: Comments (free text)
    S O: "Subholding Of" — typically the brokerage account name

Three quirks of these PDFs that drive the parser design:

  1) **Cells wrap.** A long asset name like "Cisco Systems, Inc. - Common
     Stock (CSCO) [ST]" wraps to two lines inside one table cell. Same
     with amounts like "$15,001 -\n$50,000".

  2) **pdfplumber splits some rows badly.** Sometimes a row's data lands
     in cell-by-cell columns (good); sometimes the whole row is mashed
     into the first cell with the rest as None (bad). We handle both.

  3) **Null bytes in label text.** The form's section labels render as
     'F\x00\x00\x00\x00\x00 S\x00\x00\x00\x00\x00: New' rather than
     'FILING STATUS: New' due to a font-encoding artifact. We strip
     nulls before parsing so the labels become 'F S: New' which we
     then recognize.

Limitations:
  - Scanned/handwritten PDFs (no embedded text) are flagged
    parse_failed=True and skipped.
  - The Cap. Gains column is a checkbox; we don't reliably extract it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pdfplumber


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ParsedTrade:
    """One trade row, normalized."""
    owner: str
    asset_description: str
    ticker: str | None
    asset_type: str | None
    transaction_type: str           # 'purchase', 'sale', 'exchange', 'unknown'
    transaction_date: date | None
    disclosure_date: date | None
    amount_min: int | None
    amount_max: int | None
    cap_gains_over_200: bool | None
    filing_status: str | None
    description: str | None
    comments: str | None
    subholding_of: str | None
    raw: str = ""


@dataclass
class ParsedFiling:
    doc_id: str
    trades: list[ParsedTrade]
    parse_failed: bool = False
    failure_reason: str | None = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRANSACTION_TYPE_MAP = {
    "P": "purchase",
    "S": "sale",
    "E": "exchange",
}

TICKER_PATTERN = re.compile(r"\(([A-Z][A-Z0-9\.\-]{0,9})\)")
ASSET_TYPE_PATTERN = re.compile(r"\[([A-Z]{1,4})\]")
AMOUNT_PATTERN = re.compile(
    r"\$\s*([\d,]+)\s*[-–]\s*\$\s*([\d,]+)",
    re.DOTALL,
)
AMOUNT_OPEN_TOP_PATTERN = re.compile(r"\$\s*([\d,]+)\s*\+")
DATE_PATTERN = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def parse_ptr_pdf(pdf_path: Path, doc_id: str | None = None) -> ParsedFiling:
    """Parse a House PTR PDF into a ParsedFiling."""
    if doc_id is None:
        doc_id = pdf_path.stem

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_rows: list[list[str | None]] = []
            for page in pdf.pages:
                tables = page.extract_tables() or []
                for table in tables:
                    all_rows.extend(table)
    except Exception as e:
        return ParsedFiling(
            doc_id=doc_id,
            trades=[],
            parse_failed=True,
            failure_reason=f"pdfplumber error: {e}",
        )

    if not all_rows:
        return ParsedFiling(
            doc_id=doc_id,
            trades=[],
            parse_failed=True,
            failure_reason="No tables extracted (likely scanned PDF)",
        )

    cleaned_rows: list[list[str]] = [
        [_clean_cell(cell) for cell in row] for row in all_rows
    ]

    if not any(any(cell for cell in row) for row in cleaned_rows):
        return ParsedFiling(
            doc_id=doc_id,
            trades=[],
            parse_failed=True,
            failure_reason="Tables extracted but all cells empty (likely scanned PDF)",
        )

    trades = _extract_trades_from_rows(cleaned_rows)
    return ParsedFiling(doc_id=doc_id, trades=trades)


# ---------------------------------------------------------------------------
# Cell cleaning
# ---------------------------------------------------------------------------

def _clean_cell(cell: str | None) -> str:
    if cell is None:
        return ""
    s = cell.replace("\x00", "")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in s.splitlines()]
    return "\n".join(ln for ln in lines if ln)


# ---------------------------------------------------------------------------
# Row interpretation
# ---------------------------------------------------------------------------

def _is_header_row(row: list[str]) -> bool:
    if not row:
        return False
    first = row[0].strip() if row[0] else ""
    return first == "ID" and any("Owner" in (c or "") for c in row[1:3])


def _row_is_empty(row: list[str]) -> bool:
    return not any(c.strip() for c in row)


def _row_has_split_columns(row: list[str]) -> bool:
    if len(row) < 6:
        return False
    return (
        bool(row[2].strip())
        and row[3].strip() in {"P", "S", "E"}
        and bool(DATE_PATTERN.match(row[4].strip()))
        and bool(DATE_PATTERN.match(row[5].strip()))
    )


def _row_is_blob(row: list[str]) -> bool:
    if not row:
        return False
    cell0 = row[0].strip() if row[0] else ""
    if not cell0:
        return False
    dates = DATE_PATTERN.findall(cell0)
    return len(dates) >= 2 and "$" in cell0


def _row_is_subfield_continuation(row: list[str]) -> bool:
    if not row:
        return False
    cell2 = row[2].strip() if len(row) > 2 and row[2] else ""
    if not cell2:
        cell0 = row[0].strip() if row[0] else ""
        return _looks_like_subfield(cell0) and not _row_is_blob(row)
    return _looks_like_subfield(cell2)


def _looks_like_subfield(text: str) -> bool:
    if not text:
        return False
    first_line = text.splitlines()[0].strip()
    return bool(re.match(r"^(F\s*S\s*:|F\s*:|D\s*:|C\s*:|S\s*O\s*:|S\s*:)", first_line))


def _extract_trades_from_rows(rows: list[list[str]]) -> list[ParsedTrade]:
    trades: list[ParsedTrade] = []
    i = 0
    while i < len(rows):
        row = rows[i]
        if _is_header_row(row) or _row_is_empty(row):
            i += 1
            continue
        if _row_has_split_columns(row):
            subfield_text = ""
            if i + 1 < len(rows) and _row_is_subfield_continuation(rows[i + 1]):
                next_row = rows[i + 1]
                cell2 = next_row[2].strip() if len(next_row) > 2 and next_row[2] else ""
                cell0 = next_row[0].strip() if next_row[0] else ""
                subfield_text = cell2 if cell2 else cell0
                i += 2
            else:
                i += 1
            trade = _trade_from_split_row(row, subfield_text)
            if trade:
                trades.append(trade)
            continue
        if _row_is_blob(row):
            trade = _trade_from_blob(row[0])
            if trade:
                trades.append(trade)
            i += 1
            continue
        i += 1
    return trades


# ---------------------------------------------------------------------------
# Building trades
# ---------------------------------------------------------------------------

def _trade_from_split_row(row: list[str], subfield_blob: str) -> ParsedTrade | None:
    owner = row[1].strip() if row[1] else ""
    asset_text = _flatten(row[2])
    txn_letter = row[3].strip() if row[3] else ""
    txn_date = _parse_date(row[4].strip() if row[4] else "")
    disc_date = _parse_date(row[5].strip() if row[5] else "")
    amount_text = _flatten(row[6]) if len(row) > 6 else ""
    cap_gains_cell = row[7].strip() if len(row) > 7 and row[7] else ""

    amount_min, amount_max = _parse_amount(amount_text)
    txn_type = TRANSACTION_TYPE_MAP.get(txn_letter, "unknown")
    cap_gains = _parse_cap_gains(cap_gains_cell)
    subfields = _parse_subfields(subfield_blob)

    return ParsedTrade(
        owner=owner,
        asset_description=asset_text,
        ticker=_extract_ticker(asset_text),
        asset_type=_extract_asset_type(asset_text),
        transaction_type=txn_type,
        transaction_date=txn_date,
        disclosure_date=disc_date,
        amount_min=amount_min,
        amount_max=amount_max,
        cap_gains_over_200=cap_gains,
        filing_status=subfields.get("filing_status"),
        description=subfields.get("description"),
        comments=subfields.get("comments"),
        subholding_of=subfields.get("subholding_of"),
        raw=str(row),
    )


def _trade_from_blob(blob: str) -> ParsedTrade | None:
    if not blob:
        return None

    lines = [ln.strip() for ln in blob.splitlines() if ln.strip()]
    if not lines:
        return None

    trade_line_idx: int | None = None
    for idx, ln in enumerate(lines):
        if len(DATE_PATTERN.findall(ln)) >= 2:
            trade_line_idx = idx
            break
    if trade_line_idx is None:
        return None

    trade_line = lines[trade_line_idx]
    post_lines = lines[trade_line_idx + 1:]

    dates = DATE_PATTERN.findall(trade_line)
    txn_date = _parse_date(dates[0])
    disc_date = _parse_date(dates[1])

    owner = ""
    tokens = trade_line.split()
    if tokens and tokens[0] in {"SP", "JT", "DC"}:
        owner = tokens[0]
        trade_line_minus_owner = trade_line[len(owner):].strip()
    else:
        trade_line_minus_owner = trade_line

    first_date_pos = trade_line_minus_owner.find(dates[0])
    pre_dates = trade_line_minus_owner[:first_date_pos].strip()
    txn_letter = _find_trailing_txn_letter(pre_dates)
    txn_type = TRANSACTION_TYPE_MAP.get(txn_letter or "", "unknown")

    if txn_letter:
        asset_in_trade_line = pre_dates[: pre_dates.rfind(txn_letter)].strip()
    else:
        asset_in_trade_line = pre_dates

    asset_continuations: list[str] = []
    amount_continuations: list[str] = []
    subfield_lines: list[str] = []

    amount_tail = trade_line_minus_owner[first_date_pos:]
    amount_tail = re.sub(r"^\s*\d{2}/\d{2}/\d{4}\s+\d{2}/\d{2}/\d{4}\s*", "", amount_tail)

    for ln in post_lines:
        if _looks_like_subfield(ln):
            subfield_lines.append(ln)
            continue
        has_money = "$" in ln
        has_asset_marker = "[" in ln or "(" in ln
        if has_money and not has_asset_marker:
            amount_continuations.append(ln)
        elif has_asset_marker and not has_money:
            asset_continuations.append(ln)
        elif has_asset_marker and has_money:
            asset_part, amount_part = _split_mixed_continuation(ln)
            if asset_part:
                asset_continuations.append(asset_part)
            if amount_part:
                amount_continuations.append(amount_part)
        else:
            asset_continuations.append(ln)

    asset_text = " ".join([asset_in_trade_line, *asset_continuations]).strip()
    asset_text = re.sub(r"\s+", " ", asset_text)
    amount_text = (amount_tail + " " + " ".join(amount_continuations)).strip()

    amount_min, amount_max = _parse_amount(amount_text)
    subfields = _parse_subfields("\n".join(subfield_lines))

    return ParsedTrade(
        owner=owner,
        asset_description=asset_text,
        ticker=_extract_ticker(asset_text),
        asset_type=_extract_asset_type(asset_text),
        transaction_type=txn_type,
        transaction_date=txn_date,
        disclosure_date=disc_date,
        amount_min=amount_min,
        amount_max=amount_max,
        cap_gains_over_200=None,
        filing_status=subfields.get("filing_status"),
        description=subfields.get("description"),
        comments=subfields.get("comments"),
        subholding_of=subfields.get("subholding_of"),
        raw=blob,
    )


# ---------------------------------------------------------------------------
# Field-level parsers
# ---------------------------------------------------------------------------

def _flatten(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.replace("\n", " ")).strip()


def _parse_date(s: str) -> date | None:
    if not s:
        return None
    m = DATE_PATTERN.search(s)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y").date()
    except ValueError:
        return None


def _parse_amount(text: str) -> tuple[int | None, int | None]:
    if not text:
        return None, None
    flat = re.sub(r"\s+", " ", text).strip()
    m = AMOUNT_PATTERN.search(flat)
    if m:
        lo = int(m.group(1).replace(",", ""))
        hi = int(m.group(2).replace(",", ""))
        return lo, hi
    m = AMOUNT_OPEN_TOP_PATTERN.search(flat)
    if m:
        lo = int(m.group(1).replace(",", ""))
        return lo, None
    return None, None


def _find_trailing_txn_letter(text: str) -> str | None:
    tokens = text.split()
    for token in reversed(tokens):
        clean = token.strip(",.;:")
        if clean in ("P", "S", "E"):
            return clean
        if re.fullmatch(r"[PSE]\s*\(.*\)", clean):
            return clean[0]
    return None


def _split_mixed_continuation(ln: str) -> tuple[str, str]:
    money_idx = ln.find("$")
    if money_idx <= 0:
        return ln, ""
    asset_part = ln[:money_idx].strip()
    amount_part = ln[money_idx:].strip()
    return asset_part, amount_part


def _extract_ticker(asset_text: str) -> str | None:
    if not asset_text:
        return None
    matches = TICKER_PATTERN.findall(asset_text)
    if not matches:
        return None
    for cand in reversed(matches):
        if cand not in {"INC", "LLC", "LP", "ETF"}:
            return cand
    return matches[-1]


def _extract_asset_type(asset_text: str) -> str | None:
    if not asset_text:
        return None
    m = ASSET_TYPE_PATTERN.search(asset_text)
    return m.group(1) if m else None


def _parse_cap_gains(text: str) -> bool | None:
    if not text:
        return None
    if re.search(r"\bX\b", text, re.IGNORECASE):
        return True
    return None


def _parse_subfields(blob: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not blob:
        return out

    current_key: str | None = None
    buffer: list[str] = []

    label_patterns = [
        ("filing_status", re.compile(r"^F\s*S\s*:\s*(.*)$", re.IGNORECASE)),
        ("filing_status", re.compile(r"^F\s*:\s*(.*)$", re.IGNORECASE)),
        ("subholding_of", re.compile(r"^S\s*O\s*:\s*(.*)$", re.IGNORECASE)),
        ("description",   re.compile(r"^D\s*:\s*(.*)$", re.IGNORECASE)),
        ("comments",      re.compile(r"^C\s*:\s*(.*)$", re.IGNORECASE)),
        ("subholding",    re.compile(r"^S\s*:\s*(.*)$", re.IGNORECASE)),
    ]

    def flush():
        nonlocal current_key, buffer
        if current_key and buffer:
            out[current_key] = " ".join(buffer).strip()
        buffer = []

    for raw in blob.splitlines():
        line = raw.strip()
        if not line:
            continue
        matched = False
        for key, pattern in label_patterns:
            m = pattern.match(line)
            if m:
                flush()
                current_key = key
                rest = m.group(1).strip()
                buffer = [rest] if rest else []
                matched = True
                break
        if not matched and current_key:
            buffer.append(line)

    flush()
    return out
