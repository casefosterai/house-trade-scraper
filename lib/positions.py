"""
Match buys to sales (FIFO) within position lineages and produce closed and
open positions for return calculation.

A "position lineage" is a unique combination of:
    (politician_id, ticker, owner, subholding_of)

Within a lineage, trades are walked chronologically by transaction_date.
Purchases create an open lot (FIFO queue). Sales consume the oldest lots
first. Exchanges close the entire remaining position on that date and
discard any new ticker (per the "treat exchange as a closure" rule).

Sale-without-prior-purchase: counted as an "unmatched sale" — it goes to
the politician's detail page but doesn't enter return-math aggregates.

Skipped trades (skipped=True) are filtered out before matching, EXCEPT
dividend-reinvestment trades, which are silently dropped (their shares
just blend into the existing position — see comment below).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Lot:
    """One open chunk of a position created by a purchase."""
    purchase_trade_id: str
    purchase_date: date
    purchase_disclosure_date: date
    cost_basis_usd: float          # mean of the buy's amount range
    asset_description: str         # carried for display


@dataclass
class ClosedPosition:
    """A buy lot that has been fully (or partly) closed by a later sale."""
    politician_id: str
    ticker: str
    owner: str
    subholding_of: str | None
    asset_description: str

    purchase_trade_id: str
    sale_trade_id: str

    purchase_date: date
    purchase_disclosure_date: date
    sale_date: date
    sale_disclosure_date: date

    cost_basis_usd: float
    proceeds_usd: float

    # 'merger' = closed via exchange (treat as position closing).
    # 'sale'   = closed via an actual sale row.
    closed_via: str = "sale"


@dataclass
class OpenPosition:
    """A buy lot that hasn't been closed."""
    politician_id: str
    ticker: str
    owner: str
    subholding_of: str | None
    asset_description: str

    purchase_trade_id: str
    purchase_date: date
    purchase_disclosure_date: date
    cost_basis_usd: float


@dataclass
class UnmatchedSale:
    """A sale we couldn't pair to a prior purchase in our data."""
    politician_id: str
    ticker: str
    owner: str
    subholding_of: str | None
    sale_trade_id: str
    sale_date: date
    sale_disclosure_date: date
    proceeds_usd: float
    asset_description: str


@dataclass
class MatchResult:
    closed: list[ClosedPosition] = field(default_factory=list)
    open: list[OpenPosition] = field(default_factory=list)
    unmatched_sales: list[UnmatchedSale] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Trade size sizing
# ---------------------------------------------------------------------------

def trade_size_usd(amount_min: int | None, amount_max: int | None) -> float | None:
    """Mean of the disclosure dollar range. Per user's design choice.

    Returns None if both bounds are missing. If only the lower bound exists
    (open-top "$50,000,000+" case), use that lower bound as a conservative
    estimate.
    """
    if amount_min is None and amount_max is None:
        return None
    if amount_min is not None and amount_max is not None:
        return (amount_min + amount_max) / 2.0
    return float(amount_min if amount_min is not None else amount_max)


# ---------------------------------------------------------------------------
# Lineage key
# ---------------------------------------------------------------------------

def _lineage_key(trade: dict) -> tuple:
    """Group trades into position lineages.

    A subholding_of of None or '' both mean 'no specific account', and
    we treat them as the same lineage (otherwise typos in the data would
    over-fragment positions).
    """
    return (
        trade["politician_id"],
        (trade["ticker"] or "").upper(),
        trade["owner"] or "",
        trade["subholding_of"] or "",
    )


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def match_trades(trades: list[dict]) -> MatchResult:
    """Walk every position lineage and produce closed/open/unmatched results.

    Input: the raw trade records from trades.json.
    Output: lists of ClosedPosition, OpenPosition, UnmatchedSale.
    """
    # Filter out trades that:
    #   - Are skipped for a non-DRIP reason (those are excluded entirely)
    #   - Are DRIP (silently absorbed — they don't count as new lots; an
    #     existing-position sale can still close the lineage cleanly)
    #   - Have no ticker (we can't price them, can't group lineages)
    #   - Have no transaction_date (can't order them)
    candidate_trades = [
        t for t in trades
        if t.get("ticker")
        and t.get("transaction_date")
        and not (t.get("skipped") and t.get("skip_reason") != "dividend_reinvestment")
        and t.get("skip_reason") != "dividend_reinvestment"
    ]

    # Bucket trades by lineage
    lineages: dict[tuple, list[dict]] = {}
    for t in candidate_trades:
        lineages.setdefault(_lineage_key(t), []).append(t)

    result = MatchResult()
    for lineage_trades in lineages.values():
        _process_lineage(lineage_trades, result)

    return result


def _process_lineage(lineage_trades: list[dict], result: MatchResult) -> None:
    """FIFO matching within one lineage."""
    # Sort by transaction_date, then by trade_id for tie stability.
    lineage_trades.sort(
        key=lambda t: (t["transaction_date"], t["trade_id"])
    )

    open_lots: list[Lot] = []

    for t in lineage_trades:
        txn_type = t.get("transaction_type")
        size = trade_size_usd(t.get("amount_min"), t.get("amount_max"))
        if size is None:
            # Can't measure dollars — skip silently. Rare.
            continue

        txn_date = _parse_iso_date(t["transaction_date"])
        disc_date = _parse_iso_date(t.get("disclosure_date"))
        if txn_date is None or disc_date is None:
            continue

        if txn_type == "purchase":
            open_lots.append(Lot(
                purchase_trade_id=t["trade_id"],
                purchase_date=txn_date,
                purchase_disclosure_date=disc_date,
                cost_basis_usd=size,
                asset_description=t.get("asset_description") or "",
            ))

        elif txn_type == "sale":
            # FIFO: close oldest lots first.
            remaining_proceeds = size
            while remaining_proceeds > 0 and open_lots:
                lot = open_lots[0]
                if lot.cost_basis_usd <= remaining_proceeds:
                    # This lot is fully closed.
                    result.closed.append(ClosedPosition(
                        politician_id=t["politician_id"],
                        ticker=(t["ticker"] or "").upper(),
                        owner=t.get("owner") or "",
                        subholding_of=t.get("subholding_of"),
                        asset_description=lot.asset_description or t.get("asset_description") or "",
                        purchase_trade_id=lot.purchase_trade_id,
                        sale_trade_id=t["trade_id"],
                        purchase_date=lot.purchase_date,
                        purchase_disclosure_date=lot.purchase_disclosure_date,
                        sale_date=txn_date,
                        sale_disclosure_date=disc_date,
                        cost_basis_usd=lot.cost_basis_usd,
                        proceeds_usd=lot.cost_basis_usd,  # full lot consumed
                        closed_via="sale",
                    ))
                    remaining_proceeds -= lot.cost_basis_usd
                    open_lots.pop(0)
                else:
                    # Partial close — lot's cost basis is larger than the
                    # remaining proceeds. Close the matched portion.
                    portion = remaining_proceeds
                    result.closed.append(ClosedPosition(
                        politician_id=t["politician_id"],
                        ticker=(t["ticker"] or "").upper(),
                        owner=t.get("owner") or "",
                        subholding_of=t.get("subholding_of"),
                        asset_description=lot.asset_description or t.get("asset_description") or "",
                        purchase_trade_id=lot.purchase_trade_id,
                        sale_trade_id=t["trade_id"],
                        purchase_date=lot.purchase_date,
                        purchase_disclosure_date=lot.purchase_disclosure_date,
                        sale_date=txn_date,
                        sale_disclosure_date=disc_date,
                        cost_basis_usd=portion,
                        proceeds_usd=portion,
                        closed_via="sale",
                    ))
                    lot.cost_basis_usd -= portion
                    remaining_proceeds = 0

            if remaining_proceeds > 0:
                # Sold more than we ever saw them buy — unmatched.
                result.unmatched_sales.append(UnmatchedSale(
                    politician_id=t["politician_id"],
                    ticker=(t["ticker"] or "").upper(),
                    owner=t.get("owner") or "",
                    subholding_of=t.get("subholding_of"),
                    sale_trade_id=t["trade_id"],
                    sale_date=txn_date,
                    sale_disclosure_date=disc_date,
                    proceeds_usd=remaining_proceeds,
                    asset_description=t.get("asset_description") or "",
                ))

        elif txn_type == "exchange":
            # Treat as the position closing entirely on the exchange date.
            # The new ticker (if mentioned) is ignored — we don't track it.
            for lot in open_lots:
                result.closed.append(ClosedPosition(
                    politician_id=t["politician_id"],
                    ticker=(t["ticker"] or "").upper(),
                    owner=t.get("owner") or "",
                    subholding_of=t.get("subholding_of"),
                    asset_description=lot.asset_description or t.get("asset_description") or "",
                    purchase_trade_id=lot.purchase_trade_id,
                    sale_trade_id=t["trade_id"],
                    purchase_date=lot.purchase_date,
                    purchase_disclosure_date=lot.purchase_disclosure_date,
                    sale_date=txn_date,
                    sale_disclosure_date=disc_date,
                    cost_basis_usd=lot.cost_basis_usd,
                    proceeds_usd=lot.cost_basis_usd,
                    closed_via="merger",
                ))
            open_lots.clear()

        # Other transaction types ('unknown', etc.) are ignored.

    # Whatever's left in open_lots is open positions for this lineage.
    for lot in open_lots:
        result.open.append(OpenPosition(
            politician_id=lineage_trades[0]["politician_id"],
            ticker=(lineage_trades[0]["ticker"] or "").upper(),
            owner=lineage_trades[0].get("owner") or "",
            subholding_of=lineage_trades[0].get("subholding_of"),
            asset_description=lot.asset_description,
            purchase_trade_id=lot.purchase_trade_id,
            purchase_date=lot.purchase_date,
            purchase_disclosure_date=lot.purchase_disclosure_date,
            cost_basis_usd=lot.cost_basis_usd,
        ))


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _parse_iso_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except (ValueError, TypeError):
        return None
