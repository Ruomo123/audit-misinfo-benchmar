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

Downloads all SEC Accounting and Auditing Enforcement Releases (AAERs) from:
`https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases`

**What it does:**
1. Scrapes the index across 34 pages (~3,400 entries total, paginated at `?page=N`)
2. Extracts per-entry metadata: respondent name, AAER number, release number, date, PDF URL
3. Downloads each AAER as a PDF from `https://www.sec.gov/files/litigation/admin/YYYY/34-XXXXX.pdf`
4. Saves a JSON sidecar (`AAER-XXXX.json`) alongside each PDF with metadata

**Output:**
```
aaer_data/
  aaer_index.json       # full index of all entries scraped from the SEC website
  download_log.json     # per-file download status log
  AAER-4589.pdf         # individual AAER documents
  AAER-4589.json        # metadata sidecar for each PDF
  ...
```

**Run:**
```bash
conda activate audit
python download_aaers.py
```

> Note: respects SEC's rate limit (0.25s between requests = 4 req/sec, under the 10 req/sec cap). Full download takes ~15–20 minutes. Progress checkpoints every 200 files.

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

### `test_download.py` — Download Trial Script

Sanity check: scrapes index page 0 and downloads the first 3 PDFs only. Run this before the full download to verify connectivity and parsing.

```bash
python test_download.py
```

---

### `test_filter.py` — Filter Trial Script

Sanity check: runs the text extraction and keyword classifier on the 3 already-downloaded PDFs and writes results to `aaer_filtered/test_results.json`.

```bash
python test_filter.py
```

---

### `peek_pdf.py` — PDF Content Inspector

One-off utility: extracts full text from `aaer_data/AAER-4589.pdf` and saves it to `aaer_filtered/peek_4589.txt` for manual inspection.

```bash
python peek_pdf.py
```

---

## Recommended Workflow

```
1. python test_download.py      # verify 3 PDFs download correctly
2. python download_aaers.py     # full bulk download (~3,400 AAERs)
3. python test_filter.py        # verify classifier on 3 PDFs
4. python filter_aaers.py       # classify all downloaded AAERs
```

---

## Data Source

**SEC Accounting and Auditing Enforcement Releases (AAERs)**
- Index: https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases
- Each AAER is a formal SEC administrative order against CPAs, audit firms, or company executives for accounting/auditing violations
- Current range: AAER-1 through AAER-4589 (as of April 2026)
- Documents confirm correct sourcing — header reads: *"ACCOUNTING AND AUDITING ENFORCEMENT Release No. XXXX"*

## Benchmark Tasks (from Proposal)

| Task | Description | Relevant Categories |
|---|---|---|
| Task 1: Source of Profit Change Detection | Identify when management misattributes earnings drivers | `REVENUE_TIMING`, `EARNINGS_SMOOTHING` |
| Task 2: Misleading Narrative Detection | Detect narrative claims unsupported by financial evidence | `NARRATIVE_DISTORTION` |
| Task 3: Fraud Pattern Recognition | Map reporting patterns to known manipulation types | All categories |
