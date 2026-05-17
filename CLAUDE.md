# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Building a benchmark dataset for detecting financial misinformation in SEC-enforced reporting. Data source: SEC Accounting and Auditing Enforcement Releases (AAERs) and Litigation Releases.

**GitHub**: https://github.com/Ruomo123/audit-misinfo-benchmar

## Environment

```bash
conda create -n audit python=3.11 -y
conda activate audit
pip install requests beautifulsoup4 pdfplumber pypdf openai python-dotenv
```

API keys go in a `.env` file in the project root:
```
DEEPSEEK_API_KEY=...
ANTHROPIC_API_KEY=...   # only needed for enrich_aaers.py
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

# Litigation releases (all SEC civil court actions — not just auditing; filter after download)
python download_litrel.py --from-year 2025

# Validate downloaded PDFs (detect scanned/corrupt)
python check_pdfs.py             # Report only
python check_pdfs.py --delete    # Report and delete bad files

# Classify AAERs (v1: keyword-based; v2: LLM-based, requires DEEPSEEK_API_KEY)
python filter_aaers.py
python filter_aaers_v2.py

# LLM enrichment — extracts company name, fiscal periods, mechanism (requires ANTHROPIC_API_KEY)
python enrich_aaers.py --max-cases 10   # trial run
python enrich_aaers.py --resume         # safe restart

# Select high-confidence EDGAR-traceable cases
python select_top_cases.py              # default: top 1000, conf >= 0.7
python select_top_cases.py --top-n 30 --conf 0.8

# Download matching EDGAR filings (10-K + 10-K/A)
python fetch_filings.py --dry-run       # verify CIK resolution, no download
python fetch_filings.py --limit 100     # first 100 cases only
python fetch_filings.py --resume        # skip already-fetched cases

# Assemble final benchmark records
python build_benchmark.py                          # full run; skips already-built cases
python build_benchmark.py --aaer-num 4247          # single case (skipped if already built)
python build_benchmark.py --aaer-num 4247 --force  # force re-run of one case
python build_benchmark.py --skip-xbrl             # skip XBRL lookup
python build_benchmark.py --skip-passages         # assemble skeleton records only (no DeepSeek)
python build_benchmark.py --patch-task1            # re-extract task1 attribution only for records with empty management_attribution
```

## Recommended Workflow

### Stage 1 — Collect & Classify AAERs
```bash
python test_download.py                          # verify connectivity
python download_aaers.py --from-year 2025        # download AAERs
python download_litrel.py --from-year 2025       # download litigation releases
python check_pdfs.py --delete                    # remove scanned/corrupt PDFs
python filter_aaers.py                           # v1 keyword classifier
python filter_aaers_v2.py --sample 0             # v2 LLM classifier (DEEPSEEK_API_KEY)
python select_top_cases.py                       # select top 1000, conf >= 0.7
```

### Stage 2 — Build Benchmark
```bash
python enrich_aaers.py --resume                  # LLM-enrich AAER records (ANTHROPIC_API_KEY)
python select_top_cases.py                       # select high-confidence cases (top 1000)
python fetch_filings.py --limit 100 --dry-run    # verify CIK resolution
python fetch_filings.py --limit 100              # download first 100
python fetch_filings.py --resume                 # resume; skip already-fetched
python build_benchmark.py                        # assemble benchmark records (resumable)
```

## Architecture & Data Flow

```
SEC Website → download_aaers.py  → aaer_data/        → check_pdfs.py
                                                       → filter_aaers.py    → aaer_filtered/
                                                       → filter_aaers_v2.py → aaer_filtered_v2/
                                                       → enrich_aaers.py    → aaer_filtered_v2/ (enriched)
                                                       → select_top_cases.py → selected_cases.json
                                                       → fetch_filings.py   → edgar_filings/
                                                       → build_benchmark.py → benchmark_data/
SEC Website → download_litrel.py → litrel_data/
```

**`download_aaers.py`** and **`download_litrel.py`** share the same structure: paginate the SEC index (newest-first), scrape entry metadata, download PDFs, and save JSON sidecars alongside each PDF. Both support `--from-year`, `--from-month`, `--max-cases`, `--out-dir`, `--refresh-index`. The index is cached to `*_index.json` and only re-scraped with `--refresh-index`. Files already on disk are skipped (status: `cached`). Progress is checkpointed every 200 files.

**`filter_aaers.py`** (v1, keyword-based) pipeline per PDF:
1. Extract text via `pdfplumber`, fall back to `pypdf`; skip if <200 chars (scanned image)
2. Run regex patterns (case-insensitive, word-boundary anchored) for 5 fraud categories
3. Capture 300-char context snippets around each match
4. Detect auditor involvement via `AUDITOR_PATTERNS` (auditor, CPA, PCAOB, GAAP, audit opinion, etc.)

**`filter_aaers_v2.py`** (v2, LLM-based) uses DeepSeek (`deepseek-chat`) for higher-precision classification. Requires `DEEPSEEK_API_KEY` in `.env`.

**`enrich_aaers.py`** calls Claude Haiku to extract structured metadata per AAER: `filing_entity`, `ticker`, `fiscal_periods_affected`, `primary_category`, `specific_mechanism`, `dollar_impact`, `respondent_type`, `traceable_to_financials`, `llm_confidence`. Output: `aaer_filtered_v2/aaer_dataset_v2.json`. Requires `ANTHROPIC_API_KEY`. Run `filter_aaers.py` first so texts exist in `aaer_filtered/texts/`.

**`select_top_cases.py`** filters enriched records to EDGAR-traceable issuer cases with `llm_confidence >= 0.7` (adjustable), ranks by confidence + dollar impact, outputs `selected_cases.json`. Defaults: `--top-n 1000`, `--conf 0.7`.

**`fetch_filings.py`** resolves CIK via browse-edgar company search, matches filings by form type + fiscal period, downloads original (10-K/10-Q) and restatement (10-K/A, 10-Q/A) documents. Supports `--resume` (skip already-fetched) and `--limit N` (first N cases only). CIK overrides for companies that fuzzy-match incorrectly are stored in `cik_overrides.json` (project root); entries there take priority over name lookup, and `--resume` automatically re-processes cases where the override CIK differs from the stored CIK.

**`build_benchmark.py`** assembles the final benchmark:
- Financial figures from EDGAR XBRL company facts API (concepts: revenue, ar_net, net_income, gross_profit)
- Text passages extracted by DeepSeek (`deepseek-chat`, `temperature=0`), sections capped at 6000 chars
- Resumable: skips cases already in `benchmark_data/cases.json`; use `--force` with `--aaer-num` to re-run a case
- Original-only cases (no 10-K/A on EDGAR) produce records with `task2_narrative` empty
- Output schema: each record has a top-level `tasks` key with `task1_profit_source`, `task2_narrative`, `task3_pattern`; ground truth is under `label` (not `output`)
- `--patch-task1`: re-runs only task 1 extraction for records with empty `management_attribution` — does not re-run task 2/3

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

aaer_filtered_v2/
  aaer_dataset_v2.json     # LLM-enriched records (enrich_aaers.py output)
  aaer_dataset_v2.csv      # Flat CSV version
  aaer_by_category_v2.json # Grouped by fraud category
  comparison_vs_v1.json    # Diff between v1 and v2 filter results
  selected_cases.json      # High-confidence EDGAR-traceable cases (select_top_cases.py)
  selected_cases.csv

edgar_filings/
  {aaer_num}/
    meta.json              # CIK, accession numbers, local paths
    FY2018_original.htm    # Original 10-K (or .pdf)
    FY2018_restated.htm    # 10-K/A restatement

benchmark_data/
  cases.json               # Full structured benchmark records (task1/2/3 blocks)
  cases.csv                # Flat: one row per case/period
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

| Task | Description | Relevant Categories |
|---|---|---|
| Task 1: Source of Profit Change Detection | Identify when management misattributes earnings drivers | `REVENUE_TIMING`, `EARNINGS_SMOOTHING` |
| Task 2: Misleading Narrative Detection | Detect narrative claims unsupported by financial evidence | `NARRATIVE_DISTORTION` |
| Task 3: Fraud Pattern Recognition | Map reporting patterns to known manipulation types | All categories |
