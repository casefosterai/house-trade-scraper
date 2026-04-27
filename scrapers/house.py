"""
House of Representatives financial disclosure scraper.

Source: https://disclosures-clerk.house.gov/FinancialDisclosure

The Clerk publishes a per-year ZIP file containing an XML index of every
filing made that year. The index is updated continuously as new filings
come in (typically same-day or within a few hours).

This module handles:
  - Downloading the yearly ZIP
  - Extracting and parsing the XML index
  - Filtering to Periodic Transaction Reports (PTRs)
  - Downloading individual PTR PDFs

PDF parsing (turning a PDF into trade rows) lives in lib/pdf_parser.py.
"""

from __future__ import annotations

import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import requests


INDEX_URL_TEMPLATE = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
HTTP_TIMEOUT = 30
USER_AGENT = (
    "house-trade-scraper/0.1 "
    "(+https://github.com/casefosterai/house-trade-scraper) "
    "contact: casekfoster@gmail.com"
)


@dataclass
class FilingIndexEntry:
    """One row from the XML index. Represents a filing, not a trade."""
    doc_id: str
    filing_type: str            # 'P' = PTR. Other letters are non-trade filings.
    year: int
    first_name: str
    last_name: str
    state_district: str
    filing_date: date | None    # None for non-PTR admin filings (e.g. type 'W' withdrawals).

    @property
    def display_name(self) -> str:
        """e.g. 'Pelosi, Nancy'."""
        return f"{self.last_name}, {self.first_name}"

    @property
    def pdf_url(self) -> str:
        return (
            f"https://disclosures-clerk.house.gov/public_disc/"
            f"ptr-pdfs/{self.year}/{self.doc_id}.pdf"
        )


def _http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def download_index_zip(year: int, dest_dir: Path) -> Path:
    url = INDEX_URL_TEMPLATE.format(year=year)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{year}FD.zip"

    session = _http_session()
    print(f"Downloading {url}")
    response = session.get(url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()

    dest_path.write_bytes(response.content)
    size_kb = len(response.content) / 1024
    print(f"  Saved to {dest_path} ({size_kb:.1f} KB)")
    return dest_path


def parse_index_zip(zip_path: Path) -> list[FilingIndexEntry]:
    entries: list[FilingIndexEntry] = []

    with zipfile.ZipFile(zip_path) as zf:
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xml_names:
            raise RuntimeError(f"No XML file found inside {zip_path}")
        xml_name = xml_names[0]

        with zf.open(xml_name) as xml_file:
            tree = ET.parse(xml_file)

    root = tree.getroot()

    for member_el in root.findall("Member"):
        try:
            entry = _parse_member_element(member_el)
        except Exception as e:
            print(f"  WARN: failed to parse a row: {e}")
            continue
        entries.append(entry)

    return entries


def _parse_member_element(el: ET.Element) -> FilingIndexEntry:
    """Pull fields out of one <Member> XML element."""
    def text(tag: str) -> str:
        child = el.find(tag)
        return (child.text or "").strip() if child is not None else ""

    filing_date_raw = text("FilingDate")
    if filing_date_raw:
        filing_date = datetime.strptime(filing_date_raw, "%m/%d/%Y").date()
    else:
        # Some non-PTR filings (e.g. FilingType 'W' for withdrawals) have
        # no FilingDate. We still build the entry; the PTR filter drops them.
        filing_date = None

    return FilingIndexEntry(
        doc_id=text("DocID"),
        filing_type=text("FilingType"),
        year=int(text("Year")),
        first_name=text("First"),
        last_name=text("Last"),
        state_district=text("StateDst"),
        filing_date=filing_date,
    )


def filter_ptrs(entries: list[FilingIndexEntry]) -> list[FilingIndexEntry]:
    """Keep only Periodic Transaction Reports (FilingType == 'P')."""
    return [e for e in entries if e.filing_type == "P"]


def fetch_ptr_index(year: int, data_dir: Path) -> list[FilingIndexEntry]:
    """High-level helper: download, parse, filter. Returns PTR entries."""
    raw_dir = data_dir / "raw"
    zip_path = download_index_zip(year, raw_dir)

    print(f"Parsing {zip_path}")
    all_entries = parse_index_zip(zip_path)
    print(f"  Found {len(all_entries):,} total filings in {year}")

    ptrs = filter_ptrs(all_entries)
    print(f"  Filtered to {len(ptrs):,} PTRs")

    return ptrs


def download_ptr_pdf(
    entry: FilingIndexEntry,
    pdfs_dir: Path,
    session: requests.Session | None = None,
    overwrite: bool = False,
) -> Path:
    """Download one PTR PDF to disk and return its path. Cached by default."""
    pdfs_dir.mkdir(parents=True, exist_ok=True)
    dest_path = pdfs_dir / f"{entry.doc_id}.pdf"

    if dest_path.exists() and not overwrite:
        return dest_path

    if session is None:
        session = _http_session()

    response = session.get(entry.pdf_url, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    dest_path.write_bytes(response.content)
    return dest_path
