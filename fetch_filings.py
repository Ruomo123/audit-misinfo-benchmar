"""
EDGAR filing fetcher for benchmark cases.

For each selected case, searches EDGAR for the company CIK, finds the original
and amended filing for the relevant fiscal period, and downloads them.

Input:  aaer_filtered_v2/selected_cases.json
Output: edgar_filings/{aaer_num}/
        edgar_filings/{aaer_num}/meta.json      — filing metadata index
        edgar_filings/{aaer_num}/original.*     — original 10-K or 10-Q
        edgar_filings/{aaer_num}/restated.*     — 10-K/A or 10-Q/A

Usage:
  python fetch_filings.py
  python fetch_filings.py --dry-run            # find filings but do not download
  python fetch_filings.py --aaer-num 4247      # single case
  python fetch_filings.py --in selected_cases.json
"""

import argparse
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path

import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
USER_AGENT    = "Columbia University Research contact@columbia.edu"
INPUT_FILE    = Path("aaer_filtered_v2/selected_cases.json")
OUTPUT_DIR    = Path("edgar_filings")
DELAY         = 0.3    # seconds between SEC requests
BASE_SEC      = "https://www.sec.gov"
EFTS_URL      = "https://efts.sec.gov/LATEST/search-index"
SUBMISSIONS   = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
FILING_ROOT   = "https://www.sec.gov/Archives/edgar/data/{cik}/{accn}/"
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("fetch_filings.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      USER_AGENT,
    "Accept":          "application/json",
    "Accept-Encoding": "gzip, deflate",
})


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def fetch(url: str, stream: bool = False, retries: int = 3):
    for attempt in range(retries):
        try:
            time.sleep(DELAY)
            resp = SESSION.get(url, timeout=30, stream=stream)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f"Rate-limited (429). Waiting {wait}s…")
                time.sleep(wait)
            else:
                log.warning(f"HTTP {resp.status_code}: {url} (attempt {attempt+1})")
        except requests.RequestException as e:
            log.warning(f"Request error: {e} (attempt {attempt+1})")
            time.sleep(5 * (attempt + 1))
    log.error(f"Giving up after {retries} attempts: {url}")
    return None


# ── EDGAR search ──────────────────────────────────────────────────────────────

def search_edgar(entity_name: str, forms: list[str], start_year: int, end_year: int) -> list[dict]:
    """
    Full-text search EDGAR EFTS for an entity name + form types.
    Returns list of hit dicts with keys: entity_name, entity_id (CIK),
    accession_no, file_date, form_type, period_of_report.
    """
    query = f'"{entity_name}"'
    params = {
        "q":           query,
        "forms":       ",".join(forms),
        "dateRange":   "custom",
        "startdt":     f"{start_year}-01-01",
        "enddt":       f"{end_year}-12-31",
        "hits.hits.total.relation": "eq",
    }
    url = EFTS_URL + "?" + urllib.parse.urlencode(params)
    resp = fetch(url)
    if not resp:
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    hits = data.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


def pick_best_cik(hits: list[dict], entity_name: str) -> str | None:
    """
    From EFTS hits, pick the CIK whose entity_name best matches.
    Returns the CIK string (zero-padded to 10 digits) or None.
    """
    entity_lower = entity_name.lower()
    scored = []
    for h in hits:
        name = (h.get("entity_name") or h.get("display_names") or [""])[0]
        if isinstance(name, dict):
            name = name.get("name", "")
        name_lower = name.lower()
        # Score: exact match > starts-with > word overlap
        if name_lower == entity_lower:
            score = 3
        elif entity_lower in name_lower or name_lower in entity_lower:
            score = 2
        else:
            words_e = set(entity_lower.split())
            words_n = set(name_lower.split())
            overlap = len(words_e & words_n)
            score = overlap / max(len(words_e), 1)
        cik = h.get("entity_id") or h.get("_id", "")
        scored.append((score, cik, name))

    if not scored:
        return None
    scored.sort(reverse=True)
    best_score, best_cik, best_name = scored[0]
    if best_score == 0:
        log.warning(f"  No name match above 0 for '{entity_name}' — best was '{best_name}'")
        return None
    log.info(f"  CIK resolved: {best_cik} ({best_name}, score={best_score:.2f})")
    return best_cik.lstrip("0") or "0"


# ── Submissions API ───────────────────────────────────────────────────────────

def get_submissions(cik: str) -> dict | None:
    """Fetch all filing metadata for a company from data.sec.gov."""
    cik_int = int(cik)
    url = SUBMISSIONS.format(cik=cik_int)
    resp = fetch(url)
    if not resp:
        return None
    return resp.json()


def parse_fiscal_period(period_str: str) -> tuple[str, int] | None:
    """
    Parse a period string like 'FY2018' or 'Q1 2019'.
    Returns (fp, fy) matching EDGAR conventions: ('FY',2018) or ('Q1',2019).
    """
    m = re.match(r"FY(\d{4})", period_str)
    if m:
        return ("FY", int(m.group(1)))
    m = re.match(r"Q([1-4])\s*(\d{4})", period_str)
    if m:
        return (f"Q{m.group(1)}", int(m.group(2)))
    return None


def find_filings_for_period(
    submissions: dict,
    fiscal_period: str,
    base_forms: list[str],
    amended_forms: list[str],
) -> tuple[dict | None, list[dict]]:
    """
    Search submissions for the original + amendment(s) matching fiscal_period.
    Returns (original_filing, [amended_filings]).
    Each filing dict has: form, accessionNumber, filingDate, reportDate, primaryDocument.
    """
    parsed = parse_fiscal_period(fiscal_period)
    if not parsed:
        return None, []
    fp, fy = parsed

    recent = submissions.get("filings", {}).get("recent", {})
    if not recent:
        return None, []

    # Zip the parallel arrays into records
    keys = ["accessionNumber", "filingDate", "form", "primaryDocument",
            "reportDate", "isXBRL", "isInlineXBRL"]
    n = len(recent.get("accessionNumber", []))
    filings = []
    for i in range(n):
        rec = {k: recent[k][i] for k in keys if k in recent}
        filings.append(rec)

    def period_matches(report_date: str) -> bool:
        if not report_date:
            return False
        try:
            year = int(report_date[:4])
            month = int(report_date[5:7])
        except (ValueError, IndexError):
            return False
        if fp == "FY":
            return year == fy
        # Quarters: Q1=Mar, Q2=Jun, Q3=Sep, Q4=Dec (fiscal, approximate)
        quarter_months = {"Q1": (1, 3), "Q2": (4, 6), "Q3": (7, 9), "Q4": (10, 12)}
        lo, hi = quarter_months.get(fp, (0, 0))
        return year == fy and lo <= month <= hi

    originals  = [f for f in filings if f["form"] in base_forms    and period_matches(f.get("reportDate", ""))]
    amendments = [f for f in filings if f["form"] in amended_forms and period_matches(f.get("reportDate", ""))]

    # Take the earliest original (most-recent EDGAR sort is newest-first, so last item)
    original = originals[-1] if originals else None
    return original, amendments


# ── Downloader ────────────────────────────────────────────────────────────────

def download_filing(cik: str, filing: dict, out_path: Path, label: str) -> bool:
    """Download the primary document of a filing. Returns True on success."""
    accn      = filing["accessionNumber"].replace("-", "")
    doc_name  = filing.get("primaryDocument", "")
    index_url = FILING_ROOT.format(cik=cik, accn=accn)

    if not doc_name:
        # Fall back to fetching the index page to find the primary document
        idx_url = f"{BASE_SEC}/Archives/edgar/data/{cik}/{accn}/{accn}-index.json"
        resp = fetch(idx_url)
        if resp:
            try:
                idx = resp.json()
                docs = idx.get("documents", [])
                primary = next((d for d in docs if d.get("type") in ("10-K", "10-K/A", "10-Q", "10-Q/A")), None)
                if primary:
                    doc_name = primary["name"]
            except Exception:
                pass

    if not doc_name:
        log.warning(f"  Cannot determine primary document for {label}")
        return False

    doc_url = index_url + doc_name
    suffix  = Path(doc_name).suffix or ".htm"
    dest    = out_path.with_suffix(suffix)

    if dest.exists():
        log.info(f"  Already downloaded: {dest.name}")
        filing["local_path"] = str(dest)
        return True

    resp = fetch(doc_url, stream=True)
    if not resp:
        return False

    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=16384):
            f.write(chunk)

    size_kb = dest.stat().st_size // 1024
    filing["local_path"] = str(dest)
    log.info(f"  Downloaded {dest.name} ({size_kb} KB)")
    return True


# ── Main pipeline ─────────────────────────────────────────────────────────────

def process_case(case: dict, dry_run: bool) -> dict:
    aaer_num    = case["aaer_num"]
    entity      = case.get("filing_entity") or ""
    periods     = case.get("fiscal_periods_affected") or []

    result = {
        "aaer_num":    aaer_num,
        "entity":      entity,
        "cik":         None,
        "periods":     [],
        "filings":     {},
        "status":      "pending",
    }

    if not entity:
        log.warning(f"  AAER-{aaer_num}: no filing_entity — skipping")
        result["status"] = "no_entity"
        return result

    if not periods:
        log.warning(f"  AAER-{aaer_num}: no fiscal_periods — skipping")
        result["status"] = "no_periods"
        return result

    # Determine form types based on period type
    has_annual  = any(p.startswith("FY") for p in periods)
    has_quarter = any(re.match(r"Q[1-4]", p) for p in periods)
    search_forms = []
    if has_annual:
        search_forms += ["10-K", "10-K/A"]
    if has_quarter:
        search_forms += ["10-Q", "10-Q/A"]
    if not search_forms:
        search_forms = ["10-K", "10-K/A"]

    # Determine search year range
    years = []
    for p in periods:
        m = re.search(r"(\d{4})", p)
        if m:
            years.append(int(m.group(1)))
    if not years:
        result["status"] = "no_years"
        return result
    start_year = min(years) - 1
    end_year   = max(years) + 3

    # Search EDGAR
    log.info(f"  Searching EDGAR for '{entity}' forms={search_forms} {start_year}–{end_year}")
    hits = search_edgar(entity, search_forms, start_year, end_year)
    if not hits:
        log.warning(f"  No EDGAR hits for '{entity}'")
        result["status"] = "not_found"
        return result

    cik = pick_best_cik(hits, entity)
    if not cik:
        result["status"] = "cik_not_resolved"
        return result
    result["cik"] = cik

    # Get full submissions
    submissions = get_submissions(cik)
    if not submissions:
        result["status"] = "submissions_failed"
        return result

    result["entity_name_edgar"] = submissions.get("name", entity)

    # Create output directory
    case_dir = OUTPUT_DIR / str(aaer_num)
    if not dry_run:
        case_dir.mkdir(parents=True, exist_ok=True)

    # For each fiscal period, find and download original + restated
    period_results = []
    for period in periods:
        parsed = parse_fiscal_period(period)
        if not parsed:
            continue
        fp, fy = parsed
        is_annual = (fp == "FY")
        base_forms     = ["10-K"] if is_annual else ["10-Q"]
        amended_forms  = ["10-K/A"] if is_annual else ["10-Q/A"]

        original, amendments = find_filings_for_period(
            submissions, period, base_forms, amended_forms
        )

        period_rec = {
            "period":    period,
            "original":  None,
            "restated":  None,
        }

        if not original:
            log.warning(f"  No original {base_forms[0]} found for {period}")
        else:
            log.info(f"  Found original: {original['form']} {original['filingDate']} (accn={original['accessionNumber']})")
            if not dry_run:
                out_path = case_dir / f"{period}_original"
                download_filing(cik, original, out_path, f"{period} original")
            period_rec["original"] = {
                "form":            original["form"],
                "accession":       original["accessionNumber"],
                "filed":           original["filingDate"],
                "report_date":     original.get("reportDate", ""),
                "primary_doc":     original.get("primaryDocument", ""),
                "local_path":      original.get("local_path", ""),
                "is_xbrl":         bool(original.get("isXBRL") or original.get("isInlineXBRL")),
            }

        if not amendments:
            log.warning(f"  No {amended_forms[0]} found for {period}")
        else:
            # Take the most recent amendment
            amendment = amendments[0]
            log.info(f"  Found restated: {amendment['form']} {amendment['filingDate']} (accn={amendment['accessionNumber']})")
            if not dry_run:
                out_path = case_dir / f"{period}_restated"
                download_filing(cik, amendment, out_path, f"{period} restated")
            period_rec["restated"] = {
                "form":        amendment["form"],
                "accession":   amendment["accessionNumber"],
                "filed":       amendment["filingDate"],
                "report_date": amendment.get("reportDate", ""),
                "primary_doc": amendment.get("primaryDocument", ""),
                "local_path":  amendment.get("local_path", ""),
                "is_xbrl":     bool(amendment.get("isXBRL") or amendment.get("isInlineXBRL")),
            }

        period_results.append(period_rec)

    result["periods"] = period_results

    # Save meta.json
    if not dry_run and period_results:
        meta_path = case_dir / "meta.json"
        meta_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    complete = all(
        p.get("original") and p.get("restated") for p in period_results
    )
    result["status"] = "complete" if complete else ("partial" if period_results else "no_filings")
    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Download EDGAR filings for benchmark cases")
    parser.add_argument("--in", dest="input_file", default=str(INPUT_FILE))
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--aaer-num", type=int, default=None,
                        help="Process a single AAER number")
    parser.add_argument("--dry-run", action="store_true",
                        help="Find filings and log results without downloading")
    return parser.parse_args()


def main():
    args    = parse_args()
    in_path = Path(args.input_file)
    global OUTPUT_DIR
    OUTPUT_DIR = Path(args.out_dir)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cases = json.loads(in_path.read_text(encoding="utf-8"))
    if args.aaer_num:
        cases = [c for c in cases if c.get("aaer_num") == args.aaer_num]
    log.info(f"Processing {len(cases)} cases{'  [DRY RUN]' if args.dry_run else ''}")

    results = []
    for i, case in enumerate(cases, 1):
        aaer = case.get("aaer_num", "?")
        entity = case.get("filing_entity", "")
        log.info(f"[{i}/{len(cases)}] AAER-{aaer} — {entity[:60]}")
        result = process_case(case, dry_run=args.dry_run)
        results.append(result)
        log.info(f"  status: {result['status']}")

    # Summary
    status_counts: dict[str, int] = {}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

    summary_path = OUTPUT_DIR / "fetch_summary.json"
    summary_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info(f"\n{'='*50}")
    for status, count in sorted(status_counts.items()):
        log.info(f"  {status:<25} {count}")
    log.info(f"Summary saved: {summary_path}")


if __name__ == "__main__":
    main()
