"""
SEC Litigation Releases Bulk Downloader

Downloads complaint PDFs and judgment PDFs linked under each litigation release's
"See Also" section from:
https://www.sec.gov/enforcement-litigation/litigation-releases

Page structure (confirmed):
  - Same Drupal template as AAER index (tr.pr-list-page-row)
  - ~119 pages (0–118), ~20 entries/page, ~2,380 releases total
  - Complaint PDFs: /files/litigation/complaints/YYYY/compXXXXX.pdf
  - Judgment PDFs:  /files/litigation/litreleases/YYYY/judgXXXXX.pdf
  - Some entries have multiple See Also files (e.g. multiple defendants)

Note: Litigation releases cover all SEC civil court actions. Many will be insider
trading / unregistered securities cases. Filter with filter_aaers.py to identify
those relevant to accounting/auditing misinformation.

Usage:
  python download_litrel.py                              # all releases
  python download_litrel.py --from-year 2025
  python download_litrel.py --from-year 2024 --from-month 6
  python download_litrel.py --max-cases 50
  python download_litrel.py --from-year 2025 --max-cases 100
  python download_litrel.py --refresh-index
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

# ── DEFAULTS ──────────────────────────────────────────────────────────────────
USER_AGENT = "Columbia University Research contact@columbia.edu"
BASE_URL   = "https://www.sec.gov"
INDEX_URL  = "https://www.sec.gov/enforcement-litigation/litigation-releases"
OUTPUT_DIR = Path("litrel_data")
DELAY      = 0.25   # 4 req/sec, under SEC's 10 req/sec cap
# ──────────────────────────────────────────────────────────────────────────────

DATE_FORMATS = [
    "%B %d, %Y",   # April 9, 2026
    "%b. %d, %Y",  # Apr. 9, 2026
    "%b %d, %Y",   # Apr 9, 2026
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("litrel_download.log", encoding="utf-8"),
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
    page_nums = []
    for a in soup.select("a[href*='?page=']"):
        m = re.search(r"\?page=(\d+)", a["href"])
        if m:
            page_nums.append(int(m.group(1)))
    return max(page_nums) if page_nums else 0


# ── Index scraping ────────────────────────────────────────────────────────────

def scrape_index_page(page_num: int):
    """
    Scrape one page of the litigation releases index.
    Returns (entries, soup) where entries is a list of dicts:
      {respondent, lr_num, release_no, date, detail_url, see_also}
    """
    url = f"{INDEX_URL}?page={page_num}"
    resp = fetch(url)
    if not resp:
        return [], None

    soup = BeautifulSoup(resp.text, "html.parser")
    entries = []

    for row in soup.select("tr.pr-list-page-row"):
        entry = {}

        # Date
        dt = row.find("time")
        entry["date"] = dt.get_text(strip=True) if dt else ""

        # Respondent name + detail page link
        resp_div = row.find("div", class_="release-view__respondents")
        if resp_div:
            a = resp_div.find("a", href=True)
            if a:
                entry["respondent"] = a.get_text(strip=True)
                href = a["href"]
                entry["detail_url"] = href if href.startswith("http") else BASE_URL + href
            else:
                entry["respondent"] = resp_div.get_text(strip=True)
                entry["detail_url"] = ""
        else:
            entry["respondent"] = ""
            entry["detail_url"] = ""

        # Release number (e.g. "LR-26525")
        rel_span = row.find("span", class_="view-table_subfield_value")
        release_text = rel_span.get_text(strip=True) if rel_span else ""
        entry["release_no"] = release_text

        lr_match = re.search(r"LR-(\d+)", release_text, re.IGNORECASE)
        entry["lr_num"] = int(lr_match.group(1)) if lr_match else None

        # See Also: complaint PDFs, judgment PDFs, etc.
        see_also_div = row.find("div", class_="view-table_subfield_see_also")
        see_also = []
        if see_also_div:
            for a in see_also_div.find_all("a", href=True):
                href = a["href"]
                see_also.append({
                    "label": a.get_text(strip=True),
                    "url":   href if href.startswith("http") else BASE_URL + href,
                })
        entry["see_also"] = see_also

        entries.append(entry)

    return entries, soup


def build_index(cutoff: datetime | None, max_cases: int | None) -> list:
    """Scrape index pages newest-first, stopping at cutoff date or max_cases."""
    all_entries = []
    last_page   = None
    page        = 0
    stop        = False

    while not stop:
        log.info(f"Scraping index page {page}…")
        entries, soup = scrape_index_page(page)

        if not entries:
            log.warning(f"No entries on page {page} — stopping.")
            break

        if last_page is None and soup:
            last_page = get_last_page(soup)
            log.info(f"  Detected {last_page + 1} total pages (0–{last_page})")

        for entry in entries:
            if cutoff:
                parsed = parse_date(entry["date"])
                if parsed and parsed < cutoff:
                    log.info(f"  Reached cutoff ({entry['date']}) — stopping.")
                    stop = True
                    break

            all_entries.append(entry)

            if max_cases and len(all_entries) >= max_cases:
                log.info(f"  Reached max_cases={max_cases} — stopping.")
                stop = True
                break

        log.info(f"  → {len(entries)} entries on page {page} (total: {len(all_entries)})")

        if stop or page >= (last_page or 999):
            break
        page += 1

    return all_entries


# ── Downloader ────────────────────────────────────────────────────────────────

def download_release(entry: dict, out_dir: Path) -> dict:
    """
    Download all See Also PDFs for one litigation release.
    Saves each file as LR-XXXXX-complaint.pdf, LR-XXXXX-judgment-smith.pdf, etc.
    Returns updated entry dict with download results.
    """
    lr_num   = entry.get("lr_num") or "unknown"
    see_also = entry.get("see_also", [])
    label    = f"LR-{lr_num}"
    out_meta = out_dir / f"{label}.json"

    if not see_also:
        log.warning(f"  {label}: no See Also files — skipping download")
        entry["status"]        = "no_files"
        entry["downloaded"]    = []
        return entry

    downloaded = []
    for item in see_also:
        file_url   = item["url"]
        file_label = item["label"]

        if not file_url.lower().endswith(".pdf"):
            continue  # skip non-PDF links (e.g. HTML admin summaries)

        # Build a clean filename: LR-26525-sec-complaint.pdf
        slug = re.sub(r"[^\w]+", "-", file_label.lower()).strip("-")
        # Extract original filename from URL for disambiguation (e.g. judg26515-prisno.pdf)
        url_filename = file_url.split("/")[-1]
        out_pdf = out_dir / f"{label}-{slug}-{url_filename}"

        if out_pdf.exists():
            log.debug(f"  {out_pdf.name} already exists — skipping")
            downloaded.append({"label": file_label, "url": file_url,
                                "local": str(out_pdf), "status": "cached"})
            continue

        resp = fetch(file_url, stream=True)
        if not resp:
            downloaded.append({"label": file_label, "url": file_url,
                                "local": None, "status": "failed"})
            continue

        with open(out_pdf, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = out_pdf.stat().st_size // 1024
        log.info(f"  Saved {out_pdf.name} ({size_kb} KB)")
        downloaded.append({"label": file_label, "url": file_url,
                            "local": str(out_pdf), "size_kb": size_kb, "status": "ok"})

    entry["downloaded"] = downloaded
    entry["status"]     = "ok" if any(d["status"] == "ok" for d in downloaded) else (
                          "cached" if all(d["status"] == "cached" for d in downloaded) else "failed")

    out_meta.write_text(json.dumps(entry, indent=2, ensure_ascii=False), encoding="utf-8")
    return entry


# ── CLI + Main ────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download SEC Litigation Release complaint/judgment PDFs"
    )
    parser.add_argument("--from-year",      type=int, default=None,
                        help="Only download releases from this year onwards (e.g. 2025)")
    parser.add_argument("--from-month",     type=int, default=1,
                        help="Starting month, use with --from-year (1-12, default 1)")
    parser.add_argument("--max-cases",      type=int, default=None,
                        help="Maximum number of releases to download (most recent first)")
    parser.add_argument("--out-dir",        type=str, default=str(OUTPUT_DIR),
                        help=f"Output directory (default: {OUTPUT_DIR})")
    parser.add_argument("--refresh-index",  action="store_true",
                        help="Re-scrape the index even if litrel_index.json already exists")
    return parser.parse_args()


def main():
    args    = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)

    cutoff = None
    if args.from_year:
        cutoff = datetime(args.from_year, args.from_month, 1)
        log.info(f"Date filter: on or after {cutoff.strftime('%B %Y')}")
    if args.max_cases:
        log.info(f"Case limit: {args.max_cases}")

    index_file = out_dir / "litrel_index.json"

    # ── Step 1: Build or load index ──
    if index_file.exists() and not args.refresh_index:
        log.info(f"Loading cached index from {index_file}  (use --refresh-index to re-scrape)")
        all_entries = json.loads(index_file.read_text(encoding="utf-8"))

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
        log.info("Scraping litigation releases index…")
        all_entries = build_index(cutoff=cutoff, max_cases=args.max_cases)
        index_file.write_text(json.dumps(all_entries, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info(f"Index saved: {len(all_entries)} entries → {index_file}")

    log.info(f"Downloading PDFs for {len(all_entries)} litigation releases…")

    # ── Step 2: Download PDFs ──
    results  = []
    ok = cached = failed = no_files = 0

    for i, entry in enumerate(all_entries, 1):
        log.info(f"[{i}/{len(all_entries)}] LR-{entry.get('lr_num')} | {entry.get('respondent','')[:50]}")
        result = download_release(entry, out_dir)
        results.append(result)

        status = result.get("status")
        if status == "ok":        ok       += 1
        elif status == "cached":  cached   += 1
        elif status == "no_files": no_files += 1
        else:                     failed   += 1

        if i % 200 == 0:
            (out_dir / "download_log.json").write_text(
                json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            log.info(f"  ── Checkpoint: ok={ok} cached={cached} no_files={no_files} failed={failed}")

    (out_dir / "download_log.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info(f"\n{'='*50}")
    log.info(f"Done.  OK: {ok} | Cached: {cached} | No files: {no_files} | Failed: {failed}")
    log.info(f"Output: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
