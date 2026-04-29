"""
Look up party affiliation for current Members of Congress.

Data source: the @unitedstates project's `legislators-current.json`,
maintained by a community of public-data contributors. It contains every
currently-serving member of the House and Senate with their state, district,
party, and other identifiers.

URL: https://unitedstates.github.io/congress-legislators/legislators-current.json

We download once, cache to data/legislators_cache.json, and never re-fetch
in the same run. The compute_returns.py script can pass cache_max_age_days
to control when to refresh; default is 7 days (the file changes whenever
someone resigns, dies, or wins a special election — rare).

Matching politicians from our data to legislators-current works on:
  - state (USPS code, like 'CA')
  - district number (1-based, '0' or '00' for at-large)

Our internal `state_district` field is formatted like 'CA11', 'OH05', 'AK00'.
We split it back into state + district to look up.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import requests


LEGISLATORS_URL = "https://unitedstates.github.io/congress-legislators/legislators-current.json"
CACHE_FILENAME = "legislators_cache.json"
DEFAULT_MAX_AGE_DAYS = 7
HTTP_TIMEOUT = 30
USER_AGENT = (
    "house-trade-scraper/0.1 "
    "(+https://github.com/casefosterai/house-trade-scraper) "
    "contact: casekfoster@gmail.com"
)


def load_party_lookup(
    data_dir: Path,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
) -> dict[tuple[str, str], str]:
    """Return a dict mapping (state, district) -> party letter ('D'/'R'/'I').

    `state` is the two-letter USPS code, e.g. 'CA'.
    `district` is the district number as a zero-padded two-character string,
        e.g. '11' for California's 11th, '00' for at-large states like Alaska.

    The returned dict only includes House members. Senate lookup would key
    differently (no district), but we don't need it yet.
    """
    cache_path = data_dir / CACHE_FILENAME

    legislators = _load_or_fetch(cache_path, max_age_days)

    lookup: dict[tuple[str, str], str] = {}
    for legislator in legislators:
        # Each legislator has a list of `terms`; the LAST one is current.
        terms = legislator.get("terms") or []
        if not terms:
            continue
        current = terms[-1]

        if current.get("type") != "rep":
            # Skip senators; we don't have Senate data yet anyway.
            continue

        state = (current.get("state") or "").upper()
        district = current.get("district")
        party = (current.get("party") or "").strip()
        if not state or district is None or not party:
            continue

        # Normalize party to a single letter.
        # legislators-current uses full names: 'Democrat', 'Republican', 'Independent'.
        party_letter = _party_letter(party)
        if not party_letter:
            continue

        district_str = f"{int(district):02d}"
        lookup[(state, district_str)] = party_letter

    return lookup


def party_for_state_district(
    state_district: str,
    lookup: dict[tuple[str, str], str],
) -> str | None:
    """Look up a party for a 'CA11'-style state_district string.

    Returns 'D' / 'R' / 'I' / None.
    """
    state, district = _split_state_district(state_district)
    if not state:
        return None
    return lookup.get((state, district))


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _load_or_fetch(cache_path: Path, max_age_days: int) -> list[dict]:
    """Return the legislators list, refreshing the cache if stale."""
    if cache_path.exists():
        age = (
            datetime.now()
            - datetime.fromtimestamp(cache_path.stat().st_mtime)
        )
        if age < timedelta(days=max_age_days):
            with cache_path.open("r", encoding="utf-8") as f:
                return json.load(f)

    # Fetch fresh.
    print(f"Fetching legislators data from {LEGISLATORS_URL}")
    response = requests.get(
        LEGISLATORS_URL,
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    data = response.json()

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"  cached {len(data)} legislators to {cache_path}")
    return data


def _party_letter(party_name: str) -> str | None:
    """Normalize a party name to a single letter."""
    name = party_name.strip().lower()
    if name.startswith("d"):
        return "D"
    if name.startswith("r"):
        return "R"
    if name.startswith("i") or name in {"independent", "libertarian", "green"}:
        return "I"
    return None


def _split_state_district(s: str) -> tuple[str, str]:
    """Split 'CA11' into ('CA', '11'). Tolerates 'CA1', 'CA01', 'AK00'."""
    if not s:
        return "", ""
    m = re.match(r"^([A-Za-z]{2})(\d+)$", s.strip())
    if not m:
        return "", ""
    state = m.group(1).upper()
    district = f"{int(m.group(2)):02d}"
    return state, district
