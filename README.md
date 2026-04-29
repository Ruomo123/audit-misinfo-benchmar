# Financial Misinformation Detection in Auditing — Dataset Construction

This project builds a benchmark dataset for detecting misinformation in SEC-enforced financial reporting, grounded in real enforcement actions. See `Proposal_ Misinfo Audit.docx` for the full research proposal.

---

## Setup

```bash
conda create -n audit python=3.11 -y
conda activate audit
pip install requests beautifulsoup4 pdfplumber anthropic
```

---

## Files

### `download_aaers.py` — Bulk AAER Downloader

Downloads SEC Accounting and Auditing Enforcement Releases (AAERs) from:
`https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases`

**What it does:**
1. Scrapes the index (newest-first, ~100 entries/page) — detects total page count dynamically
2. Extracts per-entry metadata: respondent name, AAER number, release number, date, PDF URL
3. Downloads each AAER as a PDF from `https://www.sec.gov/files/litigation/admin/YYYY/34-XXXXX.pdf`
4. Saves a JSON sidecar (`AAER-XXXX.json`) alongside each PDF with metadata

**Options:**

| Flag | Description | Example |
|---|---|---|
| `--from-year` | Only download AAERs from this year onwards | `--from-year 2025` |
| `--from-month` | Starting month (use with `--from-year`) | `--from-month 6` |
| `--max-cases` | Cap total number of downloads | `--max-cases 100` |
| `--out-dir` | Custom output directory | `--out-dir my_data` |
| `--refresh-index` | Re-scrape index instead of using cached version | |

**Examples:**
```bash
conda activate audit

python download_aaers.py                               # all AAERs (~3,400)
python download_aaers.py --from-year 2025              # 2025 onwards
python download_aaers.py --from-year 2024 --from-month 6   # from June 2024
python download_aaers.py --max-cases 50                # 50 most recent only
python download_aaers.py --from-year 2025 --max-cases 100  # 2025+, capped at 100
python download_aaers.py --refresh-index               # re-scrape if new AAERs added
```

**Output:**
```
aaer_data/
  aaer_index.json       # full index of all entries scraped from the SEC website
  download_log.json     # per-file download status log
  AAER-4589.pdf         # individual AAER documents
  AAER-4589.json        # metadata sidecar for each PDF
  ...
```

> Respects SEC's rate limit (0.25s between requests = 4 req/sec, under the 10 req/sec cap). Checkpoints every 200 files.

---

### `filter_aaers.py` — Fraud Pattern Classifier

Reads downloaded AAER PDFs, extracts text, and classifies each filing by fraud/misinformation category relevant to the benchmark tasks in the proposal.

**Fraud categories:**

| Category | What it covers | Example signals |
|---|---|---|
| `REVENUE_TIMING` | Premature/improper revenue recognition | bill-and-hold, channel stuffing, early buy, contingent sale |
| `EXPENSE_DEFERRAL` | Capitalizing costs that should be expensed | improperly capitalized, defer expense, line cost capitalization |
| `ACCOUNTING_ESTIMATE` | Manipulating depreciation/valuation assumptions | useful life, salvage value, depreciation change |
| `EARNINGS_SMOOTHING` | Reserve manipulation to smooth reported income | cookie jar, big bath, reserve release, manage earnings |
| `NARRATIVE_DISTORTION` | Selective/misleading MD&A or press release disclosure | misleading MD&A, material omission, omit non-recurring |

Also flags whether the respondent is an **auditor** (CPA/audit firm) vs. a company executive.

**Output:**
```
aaer_filtered/
  aaer_dataset.json       # full structured records (all AAERs)
  aaer_dataset.csv        # flat CSV, one row per AAER
  aaer_by_category.json   # entries grouped by fraud category with matched snippets
  texts/AAER-XXXX.txt     # full extracted text for matched AAERs only
  extraction_failures.txt # PDFs that could not be read (e.g. scanned images)
```

**Run** (after download completes):
```bash
conda activate audit
python filter_aaers.py
```

---

### `filter_aaers_v2.py` — v2 Filtering Pipeline (LLM-based)

Rewrite of the v1 keyword classifier. Five changes:

1. **Structure-aware parsing** — detects Roman-numeral sections (I./II./III./...) and locates the SUMMARY / FINDINGS / FACTS subblock so classification runs on the narrative core, not header/citation noise.
2. **Document-type gate** — reinstatements, dismissals, withdrawals, appellate reviews, and lifted suspensions are classified as `NON_ENFORCEMENT` and excluded from fraud categories.
3. **Legal-signal extraction** — Rule 102(e), Section 10A, PCAOB AS, ASC 606/605/350/360/842/450, SAB 99/101/104, Section 13(a), Item 303, Rule 10b-5, Section 17(a) captured as structured boolean fields.
4. **LLM classification** — Gemini 2.5 Flash classifies the SUMMARY block and returns JSON: `primary_category`, `secondary_categories`, `respondent_type` (issuer / auditor / individual / mixed), `filing_entity`, `fiscal_periods_affected`, `dollar_impact`, `specific_mechanism`, `traceable_to_financials`, `reasoning`, `confidence`.
5. **HTML-content detection** — some AAER "PDFs" were served as HTML by SEC; those are flagged and skipped.

**Setup:**
```bash
pip install pdfplumber pypdf google-generativeai
export GEMINI_API_KEY=...
```

**Run:**
```bash
python filter_aaers_v2.py                # full run, 6 parallel LLM workers
python filter_aaers_v2.py --limit 50     # smoke test (optional)
```

**Output:**
```
aaer_filtered_v2/
  aaer_dataset_v2.json      # full structured records with LLM fields + legal signals
  aaer_dataset_v2.csv       # flat CSV
  aaer_by_category_v2.json  # grouped by primary_category
  comparison_vs_v1.json     # per-AAER diff vs v1 labels
  extraction_failures_v2.txt
```

---

### `select_top_cases.py` — Downstream Case Selector

Filters `aaer_dataset_v2.json` down to the highest-quality cases for model-testing input. Applied as two stages:

**Hard filter** (reject if any fails):
- `status == "ok"` (PDF extracted cleanly)
- `is_new_enforcement` (not a reinstatement/dismissal)
- `primary_category ∈ {REVENUE_TIMING, EXPENSE_DEFERRAL, ACCOUNTING_ESTIMATE, EARNINGS_SMOOTHING}` (add `NARRATIVE_DISTORTION` with `--include-narrative`)
- `respondent_type ∈ {issuer, mixed}` (case maps to a named issuer with filings)
- `traceable_to_financials == True`
- Non-empty `filing_entity`
- Non-empty `specific_mechanism` (≥30 chars)
- At least one entry in `fiscal_periods_affected`
- `llm_confidence ≥ --conf` (default 0.8)

**Ranking key** (desc): `(llm_confidence, has_dollar_impact, n_periods, mechanism_length)`.

**Run:**
```bash
python select_top_cases.py                          # top 20, conf≥0.8
python select_top_cases.py --top-n 50 --conf 0.7    # looser
python select_top_cases.py --include-narrative      # add NARRATIVE_DISTORTION
```

**Output:**
```
aaer_filtered_v2/
  selected_cases.json   # slim records with EDGAR-ready fields
  selected_cases.csv
```
Also prints per-reason rejection counts and a ranked summary to stdout.

---

### `download_litrel.py` — SEC Litigation Releases Downloader

Downloads complaint and judgment PDFs from the "See Also" section of each SEC Litigation Release:
`https://www.sec.gov/enforcement-litigation/litigation-releases`

**What it does:**
1. Scrapes the index (~119 pages, ~2,380 releases total, newest-first)
2. For each release, downloads all linked PDFs from the "See Also" block — typically an SEC Complaint and/or Final Judgment
3. Some releases have multiple PDFs (e.g. one per defendant)
4. Saves a JSON sidecar (`LR-XXXXX.json`) per release with metadata

> Note: Litigation releases cover all SEC civil court actions, not just accounting/auditing cases. Use `filter_aaers.py` after downloading to identify audit-relevant cases.

**Options:** identical to `download_aaers.py` (`--from-year`, `--from-month`, `--max-cases`, `--out-dir`, `--refresh-index`)

**Examples:**
```bash
conda activate audit

python download_litrel.py --from-year 2025
python download_litrel.py --from-year 2024 --from-month 6
python download_litrel.py --max-cases 50
```

**Output:**
```
litrel_data/
  litrel_index.json                              # full scraped index
  download_log.json                              # per-release download status
  LR-26525-sec-complaint-comp26525.pdf           # complaint PDF
  LR-26524-final-judgment-wall-judg26524.pdf     # judgment PDF
  LR-XXXXX.json                                  # metadata sidecar
  ...
```

---

### `enrich_aaers.py` — LLM Enrichment

Uses the Claude API (Haiku model) to extract structured metadata from each AAER's extracted text. This bridges the gap between free-text enforcement releases and the structured fields needed to find matching EDGAR filings.

**Fields extracted per AAER:**

| Field | Description |
|---|---|
| `filing_entity` | Exact public company name as it appears in EDGAR |
| `ticker` | Stock ticker, if mentioned |
| `fiscal_periods_affected` | e.g. `["FY2018", "Q1 2019"]` |
| `primary_category` | Best-fit fraud category (one of the 5) |
| `specific_mechanism` | 1–2 sentence description of how the fraud worked |
| `dollar_impact` | Dollar amounts quoted from the AAER text |
| `respondent_type` | `issuer` / `auditor` / `mixed` |
| `traceable_to_financials` | Whether a matching filing is expected to exist |
| `llm_confidence` | 0.0–1.0 confidence score |

**Output:** `aaer_filtered_v2/aaer_dataset_v2.json`

```bash
conda activate audit
python enrich_aaers.py                 # all matched AAERs
python enrich_aaers.py --max-cases 10  # trial run
python enrich_aaers.py --resume        # restart safely without reprocessing
python enrich_aaers.py --aaer-num 4247 # single case
```

> Requires `ANTHROPIC_API_KEY` in your environment. Run `filter_aaers.py` first so texts exist in `aaer_filtered/texts/`.

---

### `select_top_cases.py` — Candidate Selector

Filters the enriched dataset to cases that are likely to produce good benchmark records: new enforcement actions against issuers, traceable to public financial statements, with a meaningful fraud mechanism and sufficient confidence. Ranks by confidence, dollar impact, and period specificity.

**Hard filters applied:**
- `status == "ok"` and `is_new_enforcement == True`
- `respondent_type` in `{issuer, mixed}`
- `traceable_to_financials == True`
- `filing_entity` non-empty, at least one fiscal period
- `specific_mechanism` at least 30 characters
- `llm_confidence >= 0.8` (adjustable)

**Output:** `aaer_filtered_v2/selected_cases.json` and `selected_cases.csv`

```bash
python select_top_cases.py --top-n 30
python select_top_cases.py --top-n 50 --conf 0.75 --include-narrative
```

---

### `fetch_filings.py` — EDGAR Filing Downloader

For each selected case, finds and downloads the original filing (10-K or 10-Q) and its restatement amendment (10-K/A or 10-Q/A) from EDGAR.

**What it does:**
1. Searches EDGAR full-text search (EFTS) by company name to resolve CIK
2. Queries the EDGAR submissions API for all filings
3. Matches filings by form type and fiscal period (e.g. FY2018 → 10-K with `reportDate` ending in 2018)
4. Downloads the primary document for each matched filing
5. Saves a `meta.json` sidecar per case with CIK, accession numbers, and local paths

**Output:**
```
edgar_filings/
  {aaer_num}/
    meta.json                     # filing metadata index
    FY2018_original.htm           # original 10-K (or .pdf)
    FY2018_restated.htm           # 10-K/A restatement
    Q1-2019_original.htm          # quarterly filings if applicable
    Q1-2019_restated.htm
```

```bash
conda activate audit
python fetch_filings.py --dry-run             # verify CIK resolution, no download
python fetch_filings.py                       # download all cases
python fetch_filings.py --aaer-num 4247      # single case
```

---

### `build_benchmark.py` — Benchmark Record Builder

Assembles the final benchmark dataset from EDGAR filings. Uses two complementary approaches to avoid fragile text parsing:

- **Financial figures** → [EDGAR XBRL company facts API](https://data.sec.gov/api/xbrl/companyfacts/) provides exact revenue, AR, and net income values for both the original and restated filing, identified by accession number
- **Text passages** → Claude Sonnet extracts the misleading MD&A paragraph, explains why it's misleading, and cites the ground-truth disclosure from the restatement note

**Output per record** (`benchmark_data/cases.json`):

| Task block | Input | Label |
|---|---|---|
| `task1_profit_source` | Reported financials + management attribution | True explanation + restated financials |
| `task2_narrative` | Misleading passage + location | Why misleading + correcting quote from restatement |
| `task3_pattern` | Fraud mechanism description | Category + accounting standard violated |

```bash
conda activate audit
python build_benchmark.py                        # all cases
python build_benchmark.py --aaer-num 4247       # single case
python build_benchmark.py --skip-passages       # XBRL only, no Claude call
python build_benchmark.py --skip-xbrl          # Claude only (if no XBRL data)
```

**Output:**
```
benchmark_data/
  cases.json    # full structured records
  cases.csv     # flat table: one row per case/period
```

---

### `check_pdfs.py` — PDF Validator

Checks all downloaded PDFs for corruption or unreadable content (e.g. scanned image-only PDFs with no extractable text). Saves a list of problematic files to `aaer_data/bad_pdfs.json`. Optionally deletes bad PDFs and their JSON sidecars.

```bash
conda activate audit
python check_pdfs.py           # check only, report bad files
python check_pdfs.py --delete  # check and delete bad PDFs + sidecars
```

---

### `test_download.py` — Download Trial Script

Sanity check: scrapes index page 0 and downloads the first 3 PDFs only. Run before the full download to verify connectivity and parsing.

```bash
python test_download.py
```

---

### `test_filter.py` — Filter Trial Script

Sanity check: runs text extraction and keyword classifier on already-downloaded PDFs, writes results to `aaer_filtered/test_results.json`.

```bash
python test_filter.py
```

---

## Recommended Workflow

### Stage 1 — Collect AAERs

```bash
# 1. Verify download works
python test_download.py

# 2. Download AAERs (adjust flags as needed)
python download_aaers.py --from-year 2025

# 3. Download Litigation Releases (adjust flags as needed)
python download_litrel.py --from-year 2025

# 4. Check for bad PDFs (AAERs)
python check_pdfs.py           # check only
python check_pdfs.py --delete  # remove bad PDFs

# 5. Run v1 classifier (keyword-based, fast)
python filter_aaers.py

# 6. Run v2 classifier (LLM-based, requires GEMINI_API_KEY)
python filter_aaers_v2.py

# 7. Select top-N cases for downstream model input
python select_top_cases.py --top-n 50
```

### Stage 2 — Build Benchmark Dataset

```bash
# 6. LLM-enrich AAER records (extracts company name, periods, mechanism)
python enrich_aaers.py                    # all matched AAERs
python enrich_aaers.py --max-cases 10    # trial run
python enrich_aaers.py --resume          # safe restart after interruption

# 7. Select high-confidence, EDGAR-traceable cases
python select_top_cases.py --top-n 30

# 8. Find and download matching EDGAR filings (10-K + 10-K/A, etc.)
python fetch_filings.py --dry-run        # verify CIK resolution without downloading
python fetch_filings.py                  # download originals + restatements

# 9. Assemble benchmark records
python build_benchmark.py                # full run
python build_benchmark.py --aaer-num 4247  # single case (e.g. Pareteum)
```

---

## Data & Outputs in This Repo

Committed:

| Path | Size | What |
|---|---|---|
| `aaer_filtered_v2/` | 476K | v2 filter outputs + selected top cases (JSON + CSV) |
| `output/litrel_2026.zip` | 59M | Litigation Releases 2026 sample archive |

Not committed (too large for GitHub — regenerate locally):

| Path | How to regenerate |
|---|---|
| `aaer_data/` | `python download_aaers.py` |
| `litrel_data/` | `python download_litrel.py` |
| `aaer_filtered/` | `python filter_aaers.py` |
| `output/litrel_part1-5.zip` | split `litrel_data/` into ~1GB chunks after download |
| `aaer_data.zip`, `litrel_data.zip`, `aaer_filtered.zip` | archive snapshots — create on demand |

---

## Data Source

**SEC Accounting and Auditing Enforcement Releases (AAERs)**
- Index: https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases
- Each AAER is a formal SEC administrative order against CPAs, audit firms, or company executives for accounting/auditing violations
- Current range: AAER-1 through AAER-4589 (as of April 2026)
- Documents are confirmed genuine — header reads: *"ACCOUNTING AND AUDITING ENFORCEMENT Release No. XXXX"*

---

## Benchmark Tasks (from Proposal)

| Task | Description | Relevant Categories |
|---|---|---|
| Task 1: Source of Profit Change Detection | Identify when management misattributes earnings drivers | `REVENUE_TIMING`, `EARNINGS_SMOOTHING` |
| Task 2: Misleading Narrative Detection | Detect narrative claims unsupported by financial evidence | `NARRATIVE_DISTORTION` |
| Task 3: Fraud Pattern Recognition | Map reporting patterns to known manipulation types | All categories |
