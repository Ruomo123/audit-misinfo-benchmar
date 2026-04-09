"""
SEC Accounting and Auditing Enforcement Releases (AAER) Bulk Downloader

Page structure (confirmed):
  - Index: https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases?page=N
  - Pages 0..33 (100 entries each, ~3,400 total AAERs)
  - Each entry links to a PDF: https://www.sec.gov/files/litigation/admin/YYYY/34-XXXXX.pdf

SEC requirements:
  - User-Agent must include org name + email  (https://www.sec.gov/privacy.htm#security)
  - Max 10 requests/second → we use 0.25s delay (4 req/sec) to be safe
"""

import os
import re
import time
import json
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── CONFIG ────────────────────────────────────────────────────────────────────
USER_AGENT  = "Columbia University Research contact@columbia.edu"  # ← update if needed
BASE_URL    = "https://www.sec.gov"
INDEX_URL   = "https://www.sec.gov/enforcement-litigation/accounting-auditing-enforcement-releases"
OUTPUT_DIR  = Path("aaer_data")
DELAY       = 0.25      # seconds between requests
MAX_PAGES   = 34        # pages 0-33 (update if SEC adds more)
# ──────────────────────────────────────────────────────────────────────────────

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


# ── HTTP helper ───────────────────────────────────────────────────────────────

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


# ── Index scraping ────────────────────────────────────────────────────────────

def scrape_index_page(page_num: int) -> list:
    """
    Scrape one page of the AAER index. Returns a list of entry dicts:
      {respondent, pdf_url, release_no, aaer_num, date, see_also}
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

        # Respondent name + PDF link (primary document)
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

        # See Also links (Administrative Summary, etc.)
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

    return entries


def build_index() -> list:
    """Scrape all index pages and return the full list of entries."""
    all_entries = []
    for page in range(MAX_PAGES):
        log.info(f"Scraping index page {page}/{MAX_PAGES - 1}…")
        entries = scrape_index_page(page)
        all_entries.extend(entries)
        log.info(f"  → {len(entries)} entries (total: {len(all_entries)})")
        if not entries:
            log.warning(f"  No entries on page {page} — stopping early.")
            break
    return all_entries


# ── PDF downloader ────────────────────────────────────────────────────────────

def download_pdf(entry: dict, out_dir: Path) -> dict:
    """Download the PDF for one AAER entry. Returns updated entry dict."""
    aaer_num  = entry.get("aaer_num") or entry.get("order_num") or "unknown"
    pdf_url   = entry.get("pdf_url", "")

    if not pdf_url:
        log.warning(f"No PDF URL for AAER-{aaer_num} ({entry.get('respondent')})")
        entry["status"] = "no_url"
        return entry

    # Output filename: AAER-4589.pdf  (or order number if AAER num unknown)
    label = f"AAER-{aaer_num}" if entry.get("aaer_num") else f"34-{aaer_num}"
    out_pdf  = out_dir / f"{label}.pdf"
    out_meta = out_dir / f"{label}.json"

    if out_pdf.exists():
        entry["local_pdf"] = str(out_pdf)
        entry["status"] = "cached"
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    index_file = OUTPUT_DIR / "aaer_index.json"

    # ── Step 1: Build or load the index ──
    if index_file.exists():
        log.info(f"Loading cached index from {index_file}")
        entries = json.loads(index_file.read_text(encoding="utf-8"))
    else:
        log.info("Scraping AAER index (34 pages)…")
        entries = build_index()
        index_file.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info(f"Index saved: {len(entries)} entries → {index_file}")

    log.info(f"Total entries to download: {len(entries)}")

    # ── Step 2: Download PDFs ──
    results = []
    ok = cached = failed = 0

    for i, entry in enumerate(entries, 1):
        log.info(f"[{i}/{len(entries)}] AAER-{entry.get('aaer_num')} | {entry.get('respondent','')[:50]}")
        result = download_pdf(entry, OUTPUT_DIR)
        results.append(result)

        status = result.get("status")
        if status == "ok":       ok += 1
        elif status == "cached": cached += 1
        else:                    failed += 1

        # Checkpoint every 200 entries
        if i % 200 == 0:
            (OUTPUT_DIR / "download_log.json").write_text(
                json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            log.info(f"  ── Checkpoint at {i}: ok={ok} cached={cached} failed={failed}")

    # ── Final summary ──
    (OUTPUT_DIR / "download_log.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info(f"\n{'='*50}")
    log.info(f"Done.  OK: {ok} | Cached: {cached} | Failed: {failed}")
    log.info(f"Files saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
