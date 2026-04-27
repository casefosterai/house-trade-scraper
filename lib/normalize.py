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
  - `politician_id`: a slug derived from last name + first name + state-district,
    used as the canonical key.

We include the state-district in the ID because two different members can
genuinely share the same name (e.g. multiple "Smith, John" across history).
The state-district disambiguates them. If a member changes districts, they'd
get a new ID — uncommon but not impossible. We can revisit later.
"""

from __future__ import annotations

import re

# Honorifics and prefixes to strip from first/last names.
HONORIFICS = {
    "hon.", "hon", "mr.", "mr", "mrs.", "mrs", "ms.", "ms",
    "dr.", "dr", "rep.", "rep", "sen.", "sen",
}

# Common name suffixes — kept as part of last name when present.
SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}


def clean_name_part(s: str) -> str:
    """Strip honorifics, normalize whitespace, preserve internal punctuation."""
    if not s:
        return ""
    cleaned = s.strip()

    # Drop a leading honorific if present.
    parts = cleaned.split()
    if parts and parts[0].lower() in HONORIFICS:
        parts = parts[1:]

    return " ".join(parts).strip()


def display_name(first: str, last: str) -> str:
    """Build a human-readable 'First Last' name.

    For first name with multiple words (middle names), keep them all
    so distinct people aren't collapsed. We can shorten for UI later.
    """
    f = clean_name_part(first)
    l = clean_name_part(last)
    if f and l:
        return f"{f} {l}"
    return f or l or "Unknown"


def politician_id(first: str, last: str, state_district: str) -> str:
    """Build a stable slug identifier for a politician.

    Format: "{last_slug}-{first_initial}-{state_district_lower}"
    Example: ('Nancy', 'Pelosi', 'CA11') -> 'pelosi-n-ca11'

    Why first INITIAL not full first name?
      Because the same person sometimes appears as "Nancy" and sometimes
      "Nancy P." — using just the initial avoids splitting them.

    Why include state_district?
      Two members can share a name. State+district disambiguates.

    What if the index has no state_district?
      We fall back to "unknown" to avoid producing a duplicate-prone ID.
    """
    f = clean_name_part(first)
    l = clean_name_part(last)

    # Strip suffix from last name if present (e.g. "Smith Jr." -> "Smith")
    last_tokens = l.split()
    if last_tokens and last_tokens[-1].lower().rstrip(".") in SUFFIXES:
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
