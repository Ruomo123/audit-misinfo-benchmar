# Financial Misinformation Detection in Auditing — Dataset Construction

This project builds a benchmark dataset for detecting misinformation in SEC-enforced financial reporting, grounded in real enforcement actions. See `Proposal_ Misinfo Audit.docx` for the full research proposal.

---

## Setup

```bash
conda create -n audit python=3.11 -y
conda activate audit
pip install requests beautifulsoup4 pdfplumber
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

# 5. Run classifier on AAERs
python filter_aaers.py
```

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
