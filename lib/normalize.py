"""
Normalize politician names so different spellings collapse to one identifier.

The House XML index gives us names in "Last, First Middle" format. But across
chambers and over time, the same person can appear as:

  - "Pelosi, Nancy"
  - "Pelosi, Nancy P."
  - "Pelosi, Nancy Patricia"
  - "Pelosi, Hon. Nancy"

For grouping trades by politician on the leaderboard, we need a stable key
that matches all of these to the same person.

Strategy:
  - `display_name`: a clean human-readable form, "First Last" (e.g. "Nancy Pelosi").
  - `politician_id`: a slug derived from last name + first name + state-district.

We include the state-district in the ID because two different members can
share the same name. The state-district disambiguates them. If a member
changes districts, they'd get a new ID — uncommon but not impossible.

Source-data quirks we handle:
  - Honorifics like "Hon.", "Dr.", "Rep." that should not be part of a name.
  - Duplicated tokens in the XML, e.g. "Scott Scott" — happens when the
    Clerk's data entry repeats. We collapse consecutive duplicates.
"""

from __future__ import annotations

import re

# Honorifics and prefixes to strip from first/last names.
# Stored without trailing periods because we strip them when matching.
HONORIFICS = {
    "hon", "mr", "mrs", "ms", "dr", "rep", "sen", "rev", "prof",
}

# Common name suffixes — kept as part of last name when present in the
# raw last-name field, but stripped from the politician_id slug.
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def _strip_period(token: str) -> str:
    return token.rstrip(".")


def _dedupe_consecutive(tokens: list[str]) -> list[str]:
    """Collapse runs of identical consecutive tokens, case-insensitive.

    ['Scott', 'Scott', 'Franklin'] -> ['Scott', 'Franklin']
    ['Scott', 'Mary', 'Scott']     -> ['Scott', 'Mary', 'Scott']  (non-consecutive, kept)
    """
    out: list[str] = []
    for tok in tokens:
        if out and out[-1].lower() == tok.lower():
            continue
        out.append(tok)
    return out


def clean_name_part(s: str) -> str:
    """Strip honorifics, dedupe consecutive duplicates, normalize whitespace."""
    if not s:
        return ""

    # Tokenize and clean.
    tokens = s.strip().split()

    # Drop ALL leading honorifics (in case of "Hon. Dr. Smith" etc.)
    while tokens and _strip_period(tokens[0]).lower() in HONORIFICS:
        tokens = tokens[1:]

    # Also drop honorifics anywhere in the middle (rare but happens).
    tokens = [t for t in tokens if _strip_period(t).lower() not in HONORIFICS]

    # Dedupe consecutive identical tokens.
    tokens = _dedupe_consecutive(tokens)

    return " ".join(tokens).strip()


def display_name(first: str, last: str) -> str:
    """Build a human-readable 'First Last' name."""
    f = clean_name_part(first)
    l = clean_name_part(last)
    if f and l:
        return f"{f} {l}"
    return f or l or "Unknown"


def politician_id(first: str, last: str, state_district: str) -> str:
    """Build a stable slug identifier for a politician.

    Format: "{last_slug}-{first_initial}-{state_district_lower}"
    Example: ('Nancy', 'Pelosi', 'CA11') -> 'pelosi-n-ca11'

    Strips suffixes (Jr., III) from the last name component of the slug
    so "Smith" and "Smith Jr." don't fragment.
    """
    f = clean_name_part(first)
    l = clean_name_part(last)

    # Strip suffix from last name if present (e.g. "Smith Jr." -> "Smith")
    last_tokens = l.split()
    if last_tokens and _strip_period(last_tokens[-1]).lower() in SUFFIXES:
        last_tokens = last_tokens[:-1]
    last_clean = " ".join(last_tokens) if last_tokens else l

    last_slug = _slugify(last_clean)
    first_initial = (f[:1] or "x").lower()
    sd = _slugify(state_district) if state_district else "unknown"

    return f"{last_slug}-{first_initial}-{sd}"


def _slugify(s: str) -> str:
    """Lowercase, replace anything non-alphanumeric with hyphens, dedupe hyphens."""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "x"
