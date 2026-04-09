"""
AAER Filtering & Text Extraction Pipeline

Reads downloaded AAER PDFs, extracts text, classifies each filing by
fraud/misinformation category, and outputs a structured dataset.

Fraud categories (from proposal):
  REVENUE_TIMING      – premature / improper revenue recognition
  EXPENSE_DEFERRAL    – capitalizing costs that should be expensed
  ACCOUNTING_ESTIMATE – manipulating depreciation, useful life, salvage values
  EARNINGS_SMOOTHING  – reserve manipulation, cookie jar, big bath
  NARRATIVE_DISTORTION– selective / misleading MD&A disclosure

Output files:
  aaer_filtered/
    aaer_dataset.json          – full structured records
    aaer_dataset.csv           – flat CSV for analysis
    aaer_by_category.json      – entries grouped by fraud category
    extraction_failures.txt    – PDFs that could not be read
"""

import csv
import json
import logging
import re
from pathlib import Path
from collections import defaultdict

# PDF extraction — tries pdfplumber first, falls back to pypdf
try:
    import pdfplumber
    PDF_BACKEND = "pdfplumber"
except ImportError:
    pdfplumber = None
    PDF_BACKEND = None

try:
    import pypdf
    if PDF_BACKEND is None:
        PDF_BACKEND = "pypdf"
except ImportError:
    pypdf = None

if PDF_BACKEND is None:
    raise ImportError("Install at least one PDF library:  pip install pdfplumber  OR  pip install pypdf")

# ── CONFIG ────────────────────────────────────────────────────────────────────
AAER_DIR    = Path("aaer_data")
OUTPUT_DIR  = Path("aaer_filtered")
SNIPPET_CTX = 300   # characters of context to capture around each keyword match
MIN_CHARS   = 200   # skip PDFs with fewer extracted characters (likely scanned image)
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("filter_aaers.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Fraud category keyword taxonomy ──────────────────────────────────────────
# Each category: list of (label, regex_pattern) tuples.
# Patterns are case-insensitive; word-boundary anchored where useful.

CATEGORIES = {
    "REVENUE_TIMING": [
        ("bill-and-hold",       r"bill[\s\-]and[\s\-]hold"),
        ("channel stuffing",    r"channel[\s\-]stuffing"),
        ("early buy / ship",    r"early[\s\-](?:buy|ship(?:ment)?)"),
        ("extended credit",     r"extended\s+credit\s+term"),
        ("premature revenue",   r"premature(?:ly)?\s+recogni[zs]ed?\s+revenue"),
        ("improper revenue",    r"improp(?:er(?:ly)?|erly)\s+recogni[zs]"),
        ("round-trip",          r"round[\s\-]trip\s+(?:transaction|sale)"),
        ("fictitious revenue",  r"fictitious\s+(?:revenue|sale|transaction)"),
        ("side agreement",      r"side\s+agreement"),
        ("contingent sale",     r"contingent\s+sale"),
    ],
    "EXPENSE_DEFERRAL": [
        ("improper capitaliz.", r"improp(?:er(?:ly)?|erly)\s+capitali[zs]"),
        ("capitalize expense",  r"capitali[zs]e[d]?\s+(?:operating\s+)?(?:expense|cost)"),
        ("line cost capital.",  r"line\s+cost(?:s)?\s+(?:were\s+)?capitali[zs]"),
        ("defer expense",       r"defer(?:red)?\s+(?:operating\s+)?(?:expense|cost)"),
        ("assetize",            r"assetize"),
        ("period cost",         r"period\s+cost(?:s)?\s+(?:were\s+)?(?:improperly\s+)?capitali[zs]"),
    ],
    "ACCOUNTING_ESTIMATE": [
        ("useful life",         r"useful\s+li(?:fe|ves)"),
        ("salvage value",       r"salvage\s+value"),
        ("depreciation manip.", r"(?:manipulat|alter|extend)\w*\s+depreciation"),
        ("depreciation change", r"change[d]?\s+(?:in\s+)?depreciation\s+(?:method|rate|period|polic)"),
        ("reserve estimate",    r"(?:manipulat|alter|inflate)\w*\s+(?:accounting\s+)?(?:reserve|estimate)"),
        ("goodwill impairment", r"(?:delay|avoid|manipulat)\w*\s+goodwill\s+impairment"),
        ("pension assumption",  r"(?:manipulat|alter)\w*\s+pension\s+assumption"),
    ],
    "EARNINGS_SMOOTHING": [
        ("cookie jar reserve",  r"cookie[\s\-]jar"),
        ("big bath",            r"big[\s\-]bath"),
        ("reserve release",     r"reserve\s+release"),
        ("excess reserve",      r"excess(?:ive)?\s+reserve"),
        ("income smoothing",    r"(?:income|earnings?)\s+smooth(?:ing)?"),
        ("manage earnings",     r"(?:manag(?:ed?|ing)|manipulat\w+)\s+earnings?"),
        ("discretionary accrual", r"discretionary\s+accrual"),
        ("restructuring abuse", r"restructuring\s+(?:charge|reserve)(?:s)?\s+(?:improperly|inflat|manipulat)"),
    ],
    "NARRATIVE_DISTORTION": [
        ("misleading MD&A",     r"misleading\s+(?:and\s+)?(?:MD&A|management.s\s+discussion)"),
        ("omit material",       r"omit(?:ted)?\s+material\s+(?:fact|information|disclosure)"),
        ("non-recurring omit.", r"(?:omit|fail\w*\s+to\s+disclose)\w*\s+(?:non[\s\-]recurring|one[\s\-]time)"),
        ("pro forma abuse",     r"pro\s+forma\s+(?:earnings?|income|results?)\s+(?:mislead|manipulat|omit|exclud)"),
        ("selective disclose",  r"selective(?:ly)?\s+disclose[d]?"),
        ("misleading press",    r"misleading\s+(?:press\s+release|earnings?\s+release|announcement)"),
        ("material omission",   r"material\s+omission"),
        ("false narrative",     r"false\s+(?:and\s+misleading\s+)?(?:narrative|statement|description)"),
        ("misleading disclose", r"misleading\s+disclosure"),
    ],
}

# Auditor-specific patterns (helps identify audit-failure vs. company-fraud AAERs)
AUDITOR_PATTERNS = [
    r"\bauditor\b",
    r"\bregistered\s+public\s+accounting\s+firm\b",
    r"\bCPA\b",
    r"\baudit\s+(?:failure|opinion|report|committee|partner|engagement)\b",
    r"\bPCAOB\b",
    r"\bGAAP\b",
    r"\bgenerally\s+accepted\s+auditing\s+standard",
]


def extract_text_pdfplumber(pdf_path: Path) -> str:
    text_parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text_parts.append(t)
    return "\n".join(text_parts)


def extract_text_pypdf(pdf_path: Path) -> str:
    reader = pypdf.PdfReader(str(pdf_path))
    return "\n".join(
        page.extract_text() or "" for page in reader.pages
    )


def extract_text(pdf_path: Path) -> tuple[str, str]:
    """
    Returns (text, backend_used).
    Tries pdfplumber first, then pypdf.
    """
    if PDF_BACKEND == "pdfplumber" or pdfplumber:
        try:
            text = extract_text_pdfplumber(pdf_path)
            if text.strip():
                return text, "pdfplumber"
        except Exception as e:
            log.debug(f"pdfplumber failed on {pdf_path.name}: {e}")

    if pypdf:
        try:
            text = extract_text_pypdf(pdf_path)
            if text.strip():
                return text, "pypdf"
        except Exception as e:
            log.debug(f"pypdf failed on {pdf_path.name}: {e}")

    return "", "failed"


def find_matches(text: str) -> dict:
    """
    Run all category patterns against text.
    Returns dict: {category: [{label, snippet}, ...]}
    """
    text_lower = text  # patterns already case-insensitive via re.IGNORECASE
    hits = {}

    for category, patterns in CATEGORIES.items():
        category_hits = []
        for label, pattern in patterns:
            for m in re.finditer(pattern, text_lower, re.IGNORECASE):
                start = max(0, m.start() - SNIPPET_CTX)
                end   = min(len(text), m.end() + SNIPPET_CTX)
                snippet = text[start:end].replace("\n", " ")
                snippet = re.sub(r"\s+", " ", snippet).strip()
                category_hits.append({
                    "keyword_label": label,
                    "match":         m.group(),
                    "snippet":       snippet,
                })
                break  # one match per pattern per doc is enough for labeling
        if category_hits:
            hits[category] = category_hits

    return hits


def auditor_involved(text: str) -> bool:
    """True if the AAER mentions auditors / audit conduct."""
    return any(re.search(p, text, re.IGNORECASE) for p in AUDITOR_PATTERNS)


def process_pdf(pdf_path: Path, meta: dict) -> dict | None:
    """
    Full processing pipeline for one AAER PDF.
    Returns structured record or None on failure.
    """
    text, backend = extract_text(pdf_path)

    if len(text.strip()) < MIN_CHARS:
        log.warning(f"  Short/empty text ({len(text)} chars) from {pdf_path.name} — may be scanned image")
        return {
            **meta,
            "status":       "low_text",
            "backend":      backend,
            "char_count":   len(text),
            "categories":   [],
            "auditor":      False,
            "matches":      {},
        }

    matches   = find_matches(text)
    categories = list(matches.keys())
    involves_auditor = auditor_involved(text)

    # First 800 chars as summary preview
    summary = re.sub(r"\s+", " ", text[:800]).strip()

    return {
        **meta,
        "status":       "ok",
        "backend":      backend,
        "char_count":   len(text),
        "categories":   categories,
        "auditor":      involves_auditor,
        "matches":      matches,
        "text_preview": summary,
        "full_text":    text,   # keep for downstream use; remove if storage is a concern
    }


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Load metadata index
    index_path = AAER_DIR / "aaer_index.json"
    meta_index = {}
    if index_path.exists():
        for entry in json.loads(index_path.read_text(encoding="utf-8")):
            if entry.get("aaer_num"):
                meta_index[entry["aaer_num"]] = entry

    # Collect PDFs
    pdf_files = sorted(AAER_DIR.glob("AAER-*.pdf"))
    log.info(f"Found {len(pdf_files)} AAER PDFs to process (backend: {PDF_BACKEND})")

    results       = []
    failures      = []
    by_category   = defaultdict(list)

    for i, pdf_path in enumerate(pdf_files, 1):
        # Parse AAER number from filename
        num_match = re.search(r"AAER-(\d+)", pdf_path.stem)
        aaer_num  = int(num_match.group(1)) if num_match else None

        meta = meta_index.get(aaer_num, {
            "aaer_num":    aaer_num,
            "respondent":  "",
            "date":        "",
            "release_no":  "",
            "pdf_url":     "",
        })

        log.info(f"[{i}/{len(pdf_files)}] {pdf_path.name} — {meta.get('respondent','')[:40]}")

        try:
            record = process_pdf(pdf_path, meta)
        except Exception as e:
            log.error(f"  ERROR processing {pdf_path.name}: {e}")
            failures.append(str(pdf_path.name))
            continue

        if record:
            results.append(record)
            for cat in record["categories"]:
                by_category[cat].append({
                    "aaer_num":   record.get("aaer_num"),
                    "respondent": record.get("respondent"),
                    "date":       record.get("date"),
                    "release_no": record.get("release_no"),
                    "auditor":    record.get("auditor"),
                    "snippets":   record["matches"].get(cat, []),
                })

        # Progress checkpoint every 500
        if i % 500 == 0:
            _save_outputs(results, by_category, failures)
            log.info(f"  ── Checkpoint at {i}")

    _save_outputs(results, by_category, failures)

    # Summary
    matched = sum(1 for r in results if r.get("categories"))
    log.info(f"\n{'='*55}")
    log.info(f"Total processed : {len(results)}")
    log.info(f"Matched (any cat): {matched}  ({100*matched//max(len(results),1)}%)")
    for cat in CATEGORIES:
        n = len(by_category[cat])
        log.info(f"  {cat:<25} {n:>4} matches")
    log.info(f"Failures        : {len(failures)}")
    log.info(f"Output dir      : {OUTPUT_DIR.resolve()}")


def _save_outputs(results, by_category, failures):
    # Full JSON (without full_text to keep it manageable)
    slim = [{k: v for k, v in r.items() if k != "full_text"} for r in results]
    (OUTPUT_DIR / "aaer_dataset.json").write_text(
        json.dumps(slim, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Full text saved separately per record (only for matched ones)
    texts_dir = OUTPUT_DIR / "texts"
    texts_dir.mkdir(exist_ok=True)
    for r in results:
        if r.get("full_text") and r.get("categories"):
            aaer_num = r.get("aaer_num", "unknown")
            (texts_dir / f"AAER-{aaer_num}.txt").write_text(
                r["full_text"], encoding="utf-8"
            )

    # CSV (flat — one row per AAER)
    csv_path = OUTPUT_DIR / "aaer_dataset.csv"
    fieldnames = ["aaer_num", "respondent", "date", "release_no", "pdf_url",
                  "status", "char_count", "auditor", "categories", "backend"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in slim:
            row = dict(r)
            row["categories"] = "|".join(r.get("categories", []))
            writer.writerow(row)

    # By-category JSON
    (OUTPUT_DIR / "aaer_by_category.json").write_text(
        json.dumps(dict(by_category), indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Failures list
    if failures:
        (OUTPUT_DIR / "extraction_failures.txt").write_text(
            "\n".join(failures), encoding="utf-8"
        )


if __name__ == "__main__":
    main()
