# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Building a benchmark dataset for detecting financial misinformation in SEC-enforced reporting. Data source: SEC Accounting and Auditing Enforcement Releases (AAERs) and Litigation Releases.

**GitHub**: https://github.com/Ruomo123/audit-misinfo-benchmar

## Environment

```bash
conda activate audit   # Python 3.11
# Dependencies: requests, beautifulsoup4, pdfplumber (+ pypdf as fallback)
```

## Common Commands

```bash
# Trial runs (run before full downloads)
python test_download.py          # Scrapes page 0, downloads 3 PDFs
python test_filter.py            # Runs classifier on already-downloaded PDFs

# Full AAER download
python download_aaers.py --from-year 2025
python download_aaers.py --from-year 2024 --from-month 6 --max-cases 200
python download_aaers.py --max-cases 50
python download_aaers.py --refresh-index   # Re-scrape index if new AAERs added

# Litigation releases (broader scope than AAERs)
python download_litrel.py --from-year 2025

# Validate downloaded PDFs (detect scanned/corrupt)
python check_pdfs.py             # Report only
python check_pdfs.py --delete    # Report and delete bad files

# Classify AAERs by fraud category
python filter_aaers.py
```

## Architecture & Data Flow

```
SEC Website → download_aaers.py  → aaer_data/   → check_pdfs.py → filter_aaers.py → aaer_filtered/
SEC Website → download_litrel.py → litrel_data/
```

**`download_aaers.py`** and **`download_litrel.py`** share the same structure: paginate the SEC index (newest-first), scrape entry metadata, download PDFs, and save JSON sidecars alongside each PDF. Both support `--from-year`, `--from-month`, `--max-cases`, `--out-dir`, `--refresh-index`. The index is cached to `*_index.json` and only re-scraped with `--refresh-index`. Files already on disk are skipped (status: `cached`). Progress is checkpointed every 200 files.

**`filter_aaers.py`** pipeline per PDF:
1. Extract text via `pdfplumber`, fall back to `pypdf`; skip if <200 chars (scanned image)
2. Run regex patterns (case-insensitive, word-boundary anchored) for 5 fraud categories
3. Capture 300-char context snippets around each match
4. Detect auditor involvement via `AUDITOR_PATTERNS` (auditor, CPA, PCAOB, GAAP, audit opinion, etc.)

**`check_pdfs.py`** should be run before filtering: flags corrupt files and those with <100 chars of extractable text, writes results to `aaer_data/bad_pdfs.json`.

## Key Data Facts

- AAER range: AAER-1 through ~AAER-4589 (~3,400 total, ~34 pages, ~100/page, as of April 2026)
- Litigation releases: ~2,380 total (~119 pages)
- SEC index URL: `https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases`
- PDF URL pattern: `https://www.sec.gov/files/litigation/admin/YYYY/34-XXXXX.pdf`
- SEC requires User-Agent with org name + email; we run at 4 req/sec (0.25s delay); 429 triggers exponential backoff (60s, 120s, 180s)
- Some older AAERs are scanned images with no extractable text — tracked in `aaer_data/bad_pdfs.json`

## Output Structure

```
aaer_data/
  aaer_index.json          # Full scraped index (cached)
  download_log.json        # Per-file status: ok | cached | failed | no_url
  AAER-XXXX.pdf            # Individual PDFs
  AAER-XXXX.json           # Metadata sidecar: respondent, date, release_no, pdf_url, size_kb

aaer_filtered/
  aaer_dataset.json        # Full structured records (full_text omitted to save space)
  aaer_dataset.csv         # Flat: aaer_num, respondent, date, auditor, categories, backend
  aaer_by_category.json    # {CATEGORY: [{aaer_num, respondent, date, snippets}, ...]}
  texts/AAER-XXXX.txt      # Full extracted text (matched AAERs only)
  extraction_failures.txt  # PDFs that failed text extraction

litrel_data/
  litrel_index.json
  download_log.json
  LR-XXXXX-{type}.pdf      # Multiple PDFs per release (complaint, judgment, etc.)
  LR-XXXXX.json            # Metadata sidecar
```

## Fraud Categories

| Category | Description |
|---|---|
| `REVENUE_TIMING` | Premature/improper revenue recognition (bill-and-hold, channel stuffing, fictitious revenue) |
| `EXPENSE_DEFERRAL` | Capitalizing costs that should be expensed (line cost capitalization, assetize) |
| `ACCOUNTING_ESTIMATE` | Manipulating depreciation, useful life, salvage values, goodwill impairment |
| `EARNINGS_SMOOTHING` | Reserve manipulation, cookie jar, big bath, income smoothing |
| `NARRATIVE_DISTORTION` | Misleading MD&A, material omissions, pro forma abuse |

## Benchmark Tasks (Research Goals)

- **Task 1**: Source of Profit Change Detection — identify misleading earnings attribution
- **Task 2**: Misleading Narrative Detection — MD&A distortion
- **Task 3**: Fraud Pattern Recognition — classify fraud type from AAER text
