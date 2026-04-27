# house-trade-scraper

Scrapes financial disclosure filings (Periodic Transaction Reports) from the
US House of Representatives Office of the Clerk and stores normalized trade
data in `data/trades.json`.

Part 1 of a larger project: produces structured trade data for a politician
trade-tracking webapp.

## What's in this version

Steps 1, 2, and 3:
  - Step 1: download the yearly XML index, filter to PTRs.
  - Step 2: download each PTR PDF and extract trade rows.
  - Step 3: persist trades to `data/trades.json`, dedupe across runs,
    track failed filings separately so they aren't retried every time.

## Setup

You need Python 3.10 or higher. Check with:

    python3 --version

Clone the repo and `cd` into it:

    git clone https://github.com/casefosterai/house-trade-scraper.git
    cd house-trade-scraper

Create a virtual environment for isolated dependencies:

    python3 -m venv .venv

Activate it (every new terminal):

    source .venv/bin/activate

Install dependencies:

    pip install -r requirements.txt

## Run

The main command — fetches new filings since last run and updates trades.json:

    python scripts/fetch_trades.py

Useful flags:

    python scripts/fetch_trades.py --year 2025      # specific year
    python scripts/fetch_trades.py --limit 10       # cap to N filings (testing)
    python scripts/fetch_trades.py --retry-failed   # retry previously-failed filings
    python scripts/fetch_trades.py --force          # reprocess every PTR

Other scripts:

    python scripts/fetch_index.py        # just list recent filings (no download)
    python scripts/debug_pdf.py <docid>  # inspect a cached PDF's raw content

## What gets created

Tracked in git (these are the source of truth for the downstream webapp):

    data/trades.json           Every trade we've parsed, deduped by trade_id
    data/seen_filings.json     {doc_id: timestamp} for filings already processed
    data/failed_filings.json   {doc_id: {reason, last_attempted_at}} for skips

Gitignored (raw downloads, regeneratable):

    data/raw/{year}FD.zip      The Clerk's yearly index ZIP
    data/raw/pdfs/{docid}.pdf  Cached PTR PDFs

## Trade record schema

Each entry in `trades.json` looks like:

    {
      "politician_id": "latta-r-oh05",
      "politician_name": "Robert E. Latta",
      "chamber": "house",
      "state_district": "OH05",
      "doc_id": "20034401",
      "source_url": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034401.pdf",
      "owner": "SP",
      "asset_description": "Farmers & Merchants Bancorp, Inc. (FMAO) [ST]",
      "ticker": "FMAO",
      "asset_type": "ST",
      "transaction_type": "purchase",
      "transaction_date": "2026-04-20",
      "disclosure_date": "2026-04-20",
      "amount_min": 1001,
      "amount_max": 15000,
      "filing_status": "New",
      "description": "dividend reinvestment",
      "comments": "dividend reinvestment",
      "subholding_of": null,
      "trade_id": "20034401-0"
    }

## Re-running is cheap

The first run on a new year processes every PTR — currently ~160+ filings
and a few minutes of downloading + parsing. Subsequent runs only touch
filings whose DocID isn't in `seen_filings.json`, so they're nearly
instant unless there's new disclosure activity.

If a filing fails (e.g. scanned PDF), it goes into `failed_filings.json`
and won't be retried until you pass `--retry-failed`. Manually clear an
entry from that file if you want to try it again (e.g. after improving
the parser).

## Known limitations

  - Scanned/handwritten PDFs are skipped (`failed_filings.json` records why).
  - Amounts are ranges, not exact figures — this is a STOCK Act limitation.
  - Dividend reinvestments and broker-managed trades are captured, not
    filtered. The `description` and `subholding_of` fields surface them
    so downstream consumers can decide what to do.

## Source

House Clerk's financial disclosure portal:
https://disclosures-clerk.house.gov/FinancialDisclosure

Yearly ZIP: https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{YEAR}FD.zip
PDFs:       https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{YEAR}/{DocID}.pdf

## Git identity for this repo

Set the commit identity to `casefosterai` so it doesn't inherit a different default:

    git config user.name "casefosterai"
    git config user.email "casekfoster@gmail.com"
