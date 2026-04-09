"""
SEC Accounting and Auditing Enforcement Releases (AAER) Bulk Downloader

Page structure (confirmed):
  - Index: https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases?page=N
  - Pages sorted newest-first, 100 entries per page
  - Each entry links to a PDF: https://www.sec.gov/files/litigation/admin/YYYY/34-XXXXX.pdf

SEC requirements:
  - User-Agent must include org name + email  (https://www.sec.gov/privacy.htm#security)
  - Max 10 requests/second → we use 0.25s delay (4 req/sec) to be safe

Usage:
  python download_aaers.py                          # defaults below
  python download_aaers.py --from-year 2025
  python download_aaers.py --from-year 2024 --from-month 6
  python download_aaers.py --from-year 2025 --max-cases 50
  python download_aaers.py --max-cases 100          # 100 most recent regardless of date
"""

import argparse
import re
import time
import json
import logging
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── DEFAULTS (overridable via CLI args) ───────────────────────────────────────
USER_AGENT  = "Columbia University Research contact@columbia.edu"
BASE_URL    = "https://www.sec.gov"
INDEX_URL   = "https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases"
OUTPUT_DIR  = Path("aaer_data")
DELAY       = 0.25      # seconds between requests (4 req/sec, under SEC's 10 req/sec cap)
# ──────────────────────────────────────────────────────────────────────────────

# Date formats the SEC uses on the index page
DATE_FORMATS = [
    "%B %d, %Y",    # April 8, 2026
    "%b. %d, %Y",   # Apr. 8, 2026
    "%b %d, %Y",    # Apr 8, 2026
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("aaer_download.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      USER_AGENT,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Host":            "www.sec.gov",
})


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_date(date_str: str) -> datetime | None:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def fetch(url: str, retries: int = 3, stream: bool = False):
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
            elif resp.status_code == 403:
                log.warning(f"403 Forbidden: {url}")
                return None
            else:
                log.warning(f"HTTP {resp.status_code}: {url} (attempt {attempt+1})")
        except requests.RequestException as e:
            log.warning(f"Request error: {e} (attempt {attempt+1})")
            time.sleep(5 * (attempt + 1))
    log.error(f"Giving up after {retries} attempts: {url}")
    return None


def get_last_page(soup: BeautifulSoup) -> int:
    """Detect the last page number dynamically from pagination links."""
    page_nums = []
    for a in soup.select("a[href*='?page=']"):
        m = re.search(r"\?page=(\d+)", a["href"])
        if m:
            page_nums.append(int(m.group(1)))
    return max(page_nums) if page_nums else 0


# ── Index scraping ────────────────────────────────────────────────────────────

def scrape_index_page(page_num: int) -> list:
    """
    Scrape one page of the AAER index. Returns list of entry dicts:
      {respondent, pdf_url, release_no, aaer_num, order_num, date, see_also}
    """
    url = f"{INDEX_URL}?page={page_num}"
    resp = fetch(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    entries = []

    for row in soup.select("tr.pr-list-page-row"):
        entry = {}

        # Date
        dt = row.find("time")
        entry["date"] = dt.get_text(strip=True) if dt else ""

        # Respondent name + PDF link
        resp_div = row.find("div", class_="release-view__respondents")
        if resp_div:
            a = resp_div.find("a", href=True)
            if a:
                entry["respondent"] = a.get_text(strip=True)
                href = a["href"]
                entry["pdf_url"] = href if href.startswith("http") else BASE_URL + href
            else:
                entry["respondent"] = resp_div.get_text(strip=True)
                entry["pdf_url"] = ""
        else:
            entry["respondent"] = ""
            entry["pdf_url"] = ""

        # Release number (e.g. "34-105184, AAER-4589")
        rel_span = row.find("span", class_="view-table_subfield_value")
        release_text = rel_span.get_text(strip=True) if rel_span else ""
        entry["release_no"] = release_text

        aaer_match = re.search(r"AAER-(\d+)", release_text)
        entry["aaer_num"] = int(aaer_match.group(1)) if aaer_match else None

        order_match = re.search(r"34-(\d+)", release_text)
        entry["order_num"] = order_match.group(1) if order_match else None

        # See Also links
        see_also_div = row.find("div", class_="view-table_subfield_see_also")
        see_also = []
        if see_also_div:
            for a in see_also_div.find_all("a", href=True):
                href = a["href"]
                see_also.append({
                    "label": a.get_text(strip=True),
                    "url": href if href.startswith("http") else BASE_URL + href,
                })
        entry["see_also"] = see_also

        entries.append(entry)

    # Return soup too so caller can detect last page on page 0
    return entries, soup


def build_index(cutoff: datetime | None, max_cases: int | None) -> list:
    """
    Scrape index pages newest-first, stopping when:
      - an entry's date is older than `cutoff`, OR
      - we've collected `max_cases` entries
    Detects the last page number dynamically.
    """
    all_entries = []
    last_page   = None
    page        = 0
    stop        = False

    while not stop:
        log.info(f"Scraping index page {page}…")
        result = scrape_index_page(page)
        if not result:
            break
        entries, soup = result

        # Detect last page from first page's pagination
        if last_page is None:
            last_page = get_last_page(soup)
            log.info(f"  Detected {last_page + 1} total pages (0–{last_page})")

        for entry in entries:
            # Date filter
            if cutoff:
                parsed = parse_date(entry["date"])
                if parsed and parsed < cutoff:
                    log.info(f"  Reached cutoff date ({entry['date']}) — stopping.")
                    stop = True
                    break

            all_entries.append(entry)

            # Case count limit
            if max_cases and len(all_entries) >= max_cases:
                log.info(f"  Reached max_cases={max_cases} — stopping.")
                stop = True
                break

        log.info(f"  → {len(entries)} entries on page {page} (total so far: {len(all_entries)})")

        if not entries or page >= (last_page or 999):
            break
        page += 1

    return all_entries


# ── PDF downloader ────────────────────────────────────────────────────────────

def download_pdf(entry: dict, out_dir: Path) -> dict:
    aaer_num = entry.get("aaer_num") or entry.get("order_num") or "unknown"
    pdf_url  = entry.get("pdf_url", "")

    if not pdf_url:
        log.warning(f"No PDF URL for AAER-{aaer_num} ({entry.get('respondent')})")
        entry["status"] = "no_url"
        return entry

    label    = f"AAER-{aaer_num}" if entry.get("aaer_num") else f"34-{aaer_num}"
    out_pdf  = out_dir / f"{label}.pdf"
    out_meta = out_dir / f"{label}.json"

    if out_pdf.exists():
        entry["local_pdf"] = str(out_pdf)
        entry["status"]    = "cached"
        return entry

    resp = fetch(pdf_url, stream=True)
    if not resp:
        entry["status"] = "failed"
        return entry

    with open(out_pdf, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    size_kb = out_pdf.stat().st_size // 1024
    entry["local_pdf"] = str(out_pdf)
    entry["size_kb"]   = size_kb
    entry["status"]    = "ok"

    out_meta.write_text(json.dumps(entry, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"  Saved {label}.pdf ({size_kb} KB) — {entry['respondent']}")
    return entry


# ── CLI + Main ────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Download SEC AAER PDFs")
    parser.add_argument(
        "--from-year", type=int, default=None,
        help="Only download AAERs from this year onwards (e.g. 2025)"
    )
    parser.add_argument(
        "--from-month", type=int, default=1,
        help="Combined with --from-year: starting month (1-12, default 1)"
    )
    parser.add_argument(
        "--max-cases", type=int, default=None,
        help="Maximum number of AAERs to download (most recent first)"
    )
    parser.add_argument(
        "--out-dir", type=str, default=str(OUTPUT_DIR),
        help=f"Output directory (default: {OUTPUT_DIR})"
    )
    parser.add_argument(
        "--refresh-index", action="store_true",
        help="Re-scrape the index even if aaer_index.json already exists"
    )
    return parser.parse_args()


def main():
    args      = parse_args()
    out_dir   = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)

    # Build date cutoff
    cutoff = None
    if args.from_year:
        cutoff = datetime(args.from_year, args.from_month, 1)
        log.info(f"Date filter: on or after {cutoff.strftime('%B %Y')}")
    if args.max_cases:
        log.info(f"Case limit: {args.max_cases}")

    index_file = out_dir / "aaer_index.json"

    # ── Step 1: Build or load the index ──
    if index_file.exists() and not args.refresh_index:
        log.info(f"Loading cached index from {index_file}  (use --refresh-index to re-scrape)")
        all_entries = json.loads(index_file.read_text(encoding="utf-8"))

        # Apply filters to cached index
        if cutoff:
            all_entries = [
                e for e in all_entries
                if (d := parse_date(e.get("date", ""))) is None or d >= cutoff
            ]
            log.info(f"After date filter: {len(all_entries)} entries")
        if args.max_cases:
            all_entries = all_entries[:args.max_cases]
            log.info(f"After case limit: {len(all_entries)} entries")
    else:
        log.info("Scraping AAER index…")
        all_entries = build_index(cutoff=cutoff, max_cases=args.max_cases)
        index_file.write_text(json.dumps(all_entries, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info(f"Index saved: {len(all_entries)} entries → {index_file}")

    log.info(f"Downloading {len(all_entries)} AAER PDFs…")

    # ── Step 2: Download PDFs ──
    results = []
    ok = cached = failed = 0

    for i, entry in enumerate(all_entries, 1):
        log.info(f"[{i}/{len(all_entries)}] AAER-{entry.get('aaer_num')} | {entry.get('respondent','')[:50]}")
        result = download_pdf(entry, out_dir)
        results.append(result)

        status = result.get("status")
        if status == "ok":       ok += 1
        elif status == "cached": cached += 1
        else:                    failed += 1

        if i % 200 == 0:
            (out_dir / "download_log.json").write_text(
                json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            log.info(f"  ── Checkpoint: ok={ok} cached={cached} failed={failed}")

    (out_dir / "download_log.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info(f"\n{'='*50}")
    log.info(f"Done.  OK: {ok} | Cached: {cached} | Failed: {failed}")
    log.info(f"Files saved to: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
