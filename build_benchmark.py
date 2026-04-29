"""
Benchmark record builder.

For each case with downloaded EDGAR filings:
  1. Financial figures  → EDGAR XBRL company facts API (exact, no text parsing)
  2. Text passages      → Claude API (MD&A, revenue note, restatement note)
  3. Assembles the full benchmark JSON matching the agreed schema

Input:
  aaer_filtered_v2/selected_cases.json
  edgar_filings/{aaer_num}/meta.json
  edgar_filings/{aaer_num}/*.htm  or  *.pdf

Output:
  benchmark_data/cases.json
  benchmark_data/cases.csv

Usage:
  python build_benchmark.py
  python build_benchmark.py --aaer-num 4247
  python build_benchmark.py --skip-xbrl     # if company has no XBRL data
  python build_benchmark.py --skip-passages # only run XBRL, skip Claude extraction
"""

import argparse
import csv
import json
import logging
import re
import time
from pathlib import Path

import anthropic
import requests

# PDF extraction
try:
    import pdfplumber
    _HAVE_PDFPLUMBER = True
except ImportError:
    _HAVE_PDFPLUMBER = False

try:
    import pypdf
    _HAVE_PYPDF = True
except ImportError:
    _HAVE_PYPDF = False

# ── CONFIG ────────────────────────────────────────────────────────────────────
CASES_FILE    = Path("aaer_filtered_v2/selected_cases.json")
FILINGS_DIR   = Path("edgar_filings")
AAER_TEXTS    = Path("aaer_filtered/texts")
OUTPUT_DIR    = Path("benchmark_data")
XBRL_URL      = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
USER_AGENT    = "Columbia University Research contact@columbia.edu"
DELAY         = 0.3
PASSAGE_MODEL = "claude-sonnet-4-6"

# XBRL concepts to pull (in priority order — first hit wins per category)
XBRL_CONCEPTS = {
    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
    ],
    "ar_net": [
        "AccountsReceivableNetCurrent",
        "ReceivablesNetCurrent",
    ],
    "net_income": [
        "NetIncomeLoss",
        "ProfitLoss",
    ],
    "gross_profit": [
        "GrossProfit",
    ],
}

# Max characters of filing text sent to Claude per section
MAX_SECTION_CHARS = 6000
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("build_benchmark.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SEC_SESSION = requests.Session()
SEC_SESSION.headers.update({
    "User-Agent":      USER_AGENT,
    "Accept":          "application/json",
    "Accept-Encoding": "gzip, deflate",
})


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch(url: str, retries: int = 3):
    for attempt in range(retries):
        try:
            time.sleep(DELAY)
            resp = SEC_SESSION.get(url, timeout=30)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f"Rate-limited. Waiting {wait}s…")
                time.sleep(wait)
            else:
                log.warning(f"HTTP {resp.status_code}: {url}")
        except requests.RequestException as e:
            log.warning(f"Request error: {e}")
            time.sleep(5 * (attempt + 1))
    return None


# ── Text extraction ───────────────────────────────────────────────────────────

def extract_text_from_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return _extract_pdf(path)
    # HTML / HTM — strip tags
    raw = path.read_text(encoding="utf-8", errors="replace")
    return _strip_html(raw)


def _extract_pdf(path: Path) -> str:
    if _HAVE_PDFPLUMBER:
        try:
            import pdfplumber
            with pdfplumber.open(path) as pdf:
                parts = [p.extract_text() or "" for p in pdf.pages]
            text = "\n".join(parts)
            if text.strip():
                return text
        except Exception:
            pass
    if _HAVE_PYPDF:
        try:
            reader = pypdf.PdfReader(str(path))
            return "\n".join(p.extract_text() or "" for p in reader.pages)
        except Exception:
            pass
    return ""


def _strip_html(html: str) -> str:
    # Remove scripts, styles, and tags
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r"<[^>]+>", " ", html)
    html = re.sub(r"&nbsp;", " ", html)
    html = re.sub(r"&amp;", "&", html)
    html = re.sub(r"&lt;", "<", html)
    html = re.sub(r"&gt;", ">", html)
    html = re.sub(r"&#\d+;", " ", html)
    html = re.sub(r"\s{3,}", "\n\n", html)
    return html.strip()


# ── Section splitter ──────────────────────────────────────────────────────────

# Headings that delimit 10-K sections
_ITEM_PATTERN = re.compile(
    r"(?:^|\n)\s*(?:ITEM|Item)\s+(\d+[A-Za-z]?)[.\s]",
    re.MULTILINE,
)

def extract_section(text: str, start_item: str, end_items: list[str]) -> str:
    """
    Extract the text between Item {start_item} and the first of {end_items}.
    Falls back to the 6000 chars after the start heading if boundaries are unclear.
    """
    matches = list(_ITEM_PATTERN.finditer(text))
    start_pos = None
    end_pos   = None

    for m in matches:
        item_num = m.group(1).upper()
        if item_num == start_item.upper() and start_pos is None:
            start_pos = m.start()
        elif start_pos is not None and item_num in [e.upper() for e in end_items]:
            end_pos = m.start()
            break

    if start_pos is None:
        # Keyword fallback
        keywords = {
            "7":  ["management", "discussion", "md&a"],
            "7A": ["quantitative", "market risk"],
            "8":  ["financial statements", "report of independent"],
        }
        kws = keywords.get(start_item.upper(), [start_item])
        for kw in kws:
            idx = text.lower().find(kw)
            if idx != -1:
                start_pos = idx
                break

    if start_pos is None:
        return ""

    chunk = text[start_pos: end_pos] if end_pos else text[start_pos: start_pos + MAX_SECTION_CHARS * 2]
    return chunk[:MAX_SECTION_CHARS]


def find_restatement_note(text: str) -> str:
    """Find the restatement disclosure note in a 10-K/A."""
    patterns = [
        r"(?i)(restatement|restate[d]?|revision|revision of|error correction).{0,200}",
        r"(?i)note\s+\d+.*?restat",
        r"(?i)previously\s+reported",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.DOTALL)
        if m:
            start = m.start()
            return text[start: start + MAX_SECTION_CHARS]
    # Fall back to last 20% of doc (notes are at the end)
    cutoff = int(len(text) * 0.75)
    return text[cutoff: cutoff + MAX_SECTION_CHARS]


# ── XBRL ─────────────────────────────────────────────────────────────────────

def fetch_xbrl_facts(cik: str) -> dict | None:
    """Fetch all company XBRL facts from data.sec.gov."""
    url = XBRL_URL.format(cik=int(cik))
    resp = fetch(url)
    if not resp:
        return None
    try:
        return resp.json()
    except Exception:
        return None


def get_xbrl_values(
    facts: dict,
    concept_group: str,
    period_end: str,
    original_accn: str,
    restated_accn: str,
) -> dict:
    """
    For a given concept group (e.g. 'revenue'), find the values reported in the
    original filing (original_accn) and the restated filing (restated_accn)
    for period_end (YYYY-MM-DD).

    Returns {"concept": str, "original": int|None, "restated": int|None}.
    """
    usgaap = facts.get("facts", {}).get("us-gaap", {})
    concepts = XBRL_CONCEPTS.get(concept_group, [])

    for concept in concepts:
        node = usgaap.get(concept)
        if not node:
            continue
        entries = node.get("units", {}).get("USD", [])

        original_val = None
        restated_val = None

        for e in entries:
            # Match on period end and accession number
            if e.get("end") != period_end:
                continue
            accn = e.get("accn", "").replace("-", "")
            o_accn = original_accn.replace("-", "")
            r_accn = restated_accn.replace("-", "")
            if accn == o_accn:
                original_val = e.get("val")
            elif accn == r_accn:
                restated_val = e.get("val")

        if original_val is not None or restated_val is not None:
            return {
                "concept":  concept,
                "original": original_val,
                "restated": restated_val,
                "delta":    (restated_val - original_val) if (original_val is not None and restated_val is not None) else None,
            }

    return {"concept": None, "original": None, "restated": None, "delta": None}


# ── Claude passage extraction ─────────────────────────────────────────────────

PASSAGE_PROMPT = """\
You are an expert in SEC financial disclosures and accounting fraud detection.

You are given:
1. The MD&A section from a company's ORIGINAL filing (10-K or 10-Q)
2. The restatement disclosure from the RESTATED filing (10-K/A or 10-Q/A)
3. The fraud mechanism identified from the SEC enforcement action (AAER)

Your task: extract structured information for a benchmark dataset designed to test AI models on financial misinformation detection.

Return ONLY a valid JSON object with these fields:

{{
  "task1": {{
    "misleading_attribution": "<exact quote from original MD&A that attributes revenue growth or profit change to a legitimate business reason>",
    "true_explanation": "<what actually drove the numbers, based on the restatement>"
  }},
  "task2": {{
    "misleading_passage": "<exact quote from original filing that is false or misleading>",
    "passage_location": "<section name, e.g. 'MD&A, Results of Operations' or 'Note 2, Revenue Recognition'>",
    "misleading_type": "<one of: false_compliance_claim | material_omission | misleading_framing | fabricated_metric>",
    "why_misleading": "<1-2 sentences explaining what is false or misleading about the passage and why>",
    "ground_truth_disclosure": "<exact quote from restated filing that corrects or contradicts the original passage>",
    "ground_truth_location": "<section in the restated filing>"
  }},
  "task3": {{
    "fraud_category": "<REVENUE_TIMING | EXPENSE_DEFERRAL | ACCOUNTING_ESTIMATE | EARNINGS_SMOOTHING | NARRATIVE_DISTORTION>",
    "mechanism_summary": "<1-2 sentence description of the specific fraud mechanism>",
    "standard_violated": "<accounting standard, e.g. ASC 606, ASC 350, GAAP>"
  }},
  "extraction_confidence": <0.0 to 1.0>,
  "extraction_notes": "<any caveats — e.g. 'MD&A section was truncated' or 'restatement note not clearly located'>"
}}

FRAUD MECHANISM FROM AAER:
{mechanism}

ORIGINAL FILING — MD&A SECTION:
{original_mda}

RESTATED FILING — RESTATEMENT NOTE:
{restated_note}

Return only valid JSON, no markdown fences.
"""


def extract_passages(
    client: anthropic.Anthropic,
    mechanism: str,
    original_mda: str,
    restated_note: str,
) -> dict:
    prompt = PASSAGE_PROMPT.format(
        mechanism=mechanism[:1000],
        original_mda=original_mda[:MAX_SECTION_CHARS],
        restated_note=restated_note[:MAX_SECTION_CHARS],
    )
    try:
        message = client.messages.create(
            model=PASSAGE_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning(f"  JSON parse error from Claude: {e}")
        return {"extraction_confidence": 0.0, "extraction_notes": f"parse_error: {e}"}
    except Exception as e:
        log.warning(f"  Claude API error: {e}")
        return {"extraction_confidence": 0.0, "extraction_notes": f"api_error: {e}"}


# ── Benchmark record assembly ─────────────────────────────────────────────────

def build_record(
    case: dict,
    period_info: dict,
    xbrl_facts: dict | None,
    passages: dict,
) -> dict:
    period       = period_info["period"]
    original_fi  = period_info.get("original") or {}
    restated_fi  = period_info.get("restated") or {}
    aaer_num     = case["aaer_num"]

    case_id = f"AAER-{aaer_num}-{period.replace(' ', '-')}"

    # Financial figures from XBRL
    financials: dict[str, dict] = {}
    if xbrl_facts and original_fi.get("accession") and restated_fi.get("accession"):
        period_end = original_fi.get("report_date", "")
        for group in XBRL_CONCEPTS:
            financials[group] = get_xbrl_values(
                xbrl_facts,
                group,
                period_end,
                original_fi["accession"],
                restated_fi["accession"],
            )

    task1 = passages.get("task1", {})
    task2 = passages.get("task2", {})
    task3 = passages.get("task3", {})

    record = {
        "case_id":      case_id,
        "aaer_num":     aaer_num,
        "company":      case.get("filing_entity") or case.get("aaer_respondent"),
        "fiscal_period": period,
        "fraud_category": case.get("primary_category"),

        "documents": {
            "original": {
                "type":        original_fi.get("form", ""),
                "period_end":  original_fi.get("report_date", ""),
                "filed":       original_fi.get("filed", ""),
                "accession":   original_fi.get("accession", ""),
                "local_path":  original_fi.get("local_path", ""),
                "is_xbrl":     original_fi.get("is_xbrl", False),
            },
            "restated": {
                "type":        restated_fi.get("form", ""),
                "period_end":  restated_fi.get("report_date", ""),
                "filed":       restated_fi.get("filed", ""),
                "accession":   restated_fi.get("accession", ""),
                "local_path":  restated_fi.get("local_path", ""),
                "is_xbrl":     restated_fi.get("is_xbrl", False),
            },
            "enforcement": {
                "type":       "AAER",
                "aaer_num":   aaer_num,
                "local_path": str(Path("aaer_data") / f"AAER-{aaer_num}.pdf"),
            },
        },

        "tasks": {
            "task1_profit_source": {
                "input": {
                    "reported_financials": {
                        k: v.get("original") for k, v in financials.items()
                    },
                    "management_attribution": task1.get("misleading_attribution", ""),
                },
                "label": {
                    "attribution_correct": False,
                    "true_explanation": task1.get("true_explanation", ""),
                    "restated_financials": {
                        k: v.get("restated") for k, v in financials.items()
                    },
                },
            },

            "task2_narrative": {
                "input": {
                    "passage":  task2.get("misleading_passage", ""),
                    "location": task2.get("passage_location", ""),
                },
                "label": {
                    "is_misleading":          True,
                    "misleading_type":        task2.get("misleading_type", ""),
                    "explanation":            task2.get("why_misleading", ""),
                    "ground_truth_source":    task2.get("ground_truth_disclosure", ""),
                    "ground_truth_location":  task2.get("ground_truth_location", ""),
                },
            },

            "task3_pattern": {
                "input": case.get("specific_mechanism", ""),
                "label": {
                    "fraud_category":    task3.get("fraud_category", case.get("primary_category")),
                    "mechanism":         task3.get("mechanism_summary", case.get("specific_mechanism", "")),
                    "standard_violated": task3.get("standard_violated", ""),
                },
            },
        },

        "xbrl_financials": financials,

        "metadata": {
            "dollar_impact":     case.get("dollar_impact", ""),
            "llm_confidence":    case.get("llm_confidence"),
            "extraction_confidence": passages.get("extraction_confidence"),
            "extraction_notes":  passages.get("extraction_notes", ""),
            "cik":               period_info.get("cik", ""),
        },
    }
    return record


# ── Main ──────────────────────────────────────────────────────────────────────

def process_case(
    case: dict,
    client: anthropic.Anthropic,
    skip_xbrl: bool,
    skip_passages: bool,
) -> list[dict]:
    aaer_num = case["aaer_num"]
    meta_path = FILINGS_DIR / str(aaer_num) / "meta.json"

    if not meta_path.exists():
        log.warning(f"  AAER-{aaer_num}: no meta.json — run fetch_filings.py first")
        return []

    filing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    cik = filing_meta.get("cik")

    # Fetch XBRL once per company
    xbrl_facts = None
    if not skip_xbrl and cik:
        log.info(f"  Fetching XBRL facts for CIK {cik}")
        xbrl_facts = fetch_xbrl_facts(cik)
        if not xbrl_facts:
            log.warning(f"  No XBRL data for CIK {cik}")

    records = []
    for period_info in filing_meta.get("periods", []):
        period_info["cik"] = cik
        period = period_info["period"]
        original_path = period_info.get("original", {}).get("local_path", "")
        restated_path = period_info.get("restated", {}).get("local_path", "")

        if not original_path or not restated_path:
            log.warning(f"  {period}: missing original or restated file path — skipping")
            continue

        orig_file = Path(original_path)
        rest_file = Path(restated_path)

        if not orig_file.exists() or not rest_file.exists():
            log.warning(f"  {period}: filing files not found on disk — skipping")
            continue

        passages: dict = {}
        if not skip_passages:
            log.info(f"  {period}: extracting text and calling Claude")
            orig_text = extract_text_from_file(orig_file)
            rest_text = extract_text_from_file(rest_file)

            original_mda  = extract_section(orig_text, "7", ["7A", "8"])
            restated_note = find_restatement_note(rest_text)

            if not original_mda:
                log.warning(f"  {period}: could not locate MD&A in original filing")

            passages = extract_passages(
                client,
                mechanism=case.get("specific_mechanism", ""),
                original_mda=original_mda,
                restated_note=restated_note,
            )
            log.info(f"  {period}: extraction confidence={passages.get('extraction_confidence', '?')}")

        record = build_record(case, period_info, xbrl_facts, passages)
        records.append(record)

    return records


def parse_args():
    parser = argparse.ArgumentParser(description="Build benchmark dataset from EDGAR filings")
    parser.add_argument("--cases", default=str(CASES_FILE))
    parser.add_argument("--filings-dir", default=str(FILINGS_DIR))
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--aaer-num", type=int, default=None)
    parser.add_argument("--skip-xbrl", action="store_true")
    parser.add_argument("--skip-passages", action="store_true",
                        help="Skip Claude extraction; assemble skeleton records only")
    return parser.parse_args()


def main():
    args = parse_args()
    global FILINGS_DIR, OUTPUT_DIR
    FILINGS_DIR = Path(args.filings_dir)
    OUTPUT_DIR  = Path(args.out_dir)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cases = json.loads(Path(args.cases).read_text(encoding="utf-8"))
    if args.aaer_num:
        cases = [c for c in cases if c.get("aaer_num") == args.aaer_num]
    log.info(f"Building benchmark for {len(cases)} cases")

    client = anthropic.Anthropic()
    all_records: list[dict] = []

    for i, case in enumerate(cases, 1):
        aaer = case.get("aaer_num", "?")
        entity = case.get("filing_entity") or case.get("aaer_respondent", "")
        log.info(f"[{i}/{len(cases)}] AAER-{aaer} — {entity[:60]}")

        try:
            records = process_case(case, client, args.skip_xbrl, args.skip_passages)
        except Exception as e:
            log.error(f"  Unexpected error: {e}")
            records = []

        all_records.extend(records)
        log.info(f"  → {len(records)} benchmark record(s) produced")

    # Save JSON
    out_json = OUTPUT_DIR / "cases.json"
    out_json.write_text(json.dumps(all_records, indent=2, ensure_ascii=False), encoding="utf-8")

    # Save flat CSV
    out_csv = OUTPUT_DIR / "cases.csv"
    flat_fields = [
        "case_id", "aaer_num", "company", "fiscal_period", "fraud_category",
        "original_form", "original_filed", "restated_form", "restated_filed",
        "revenue_original", "revenue_restated", "revenue_delta",
        "ar_original", "ar_restated",
        "misleading_passage", "misleading_type", "extraction_confidence",
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=flat_fields, extrasaction="ignore")
        writer.writeheader()
        for r in all_records:
            fin  = r.get("xbrl_financials", {})
            rev  = fin.get("revenue", {})
            ar   = fin.get("ar_net", {})
            t2   = r.get("tasks", {}).get("task2_narrative", {})
            docs = r.get("documents", {})
            writer.writerow({
                "case_id":              r["case_id"],
                "aaer_num":             r["aaer_num"],
                "company":              r["company"],
                "fiscal_period":        r["fiscal_period"],
                "fraud_category":       r["fraud_category"],
                "original_form":        docs.get("original", {}).get("type", ""),
                "original_filed":       docs.get("original", {}).get("filed", ""),
                "restated_form":        docs.get("restated", {}).get("type", ""),
                "restated_filed":       docs.get("restated", {}).get("filed", ""),
                "revenue_original":     rev.get("original"),
                "revenue_restated":     rev.get("restated"),
                "revenue_delta":        rev.get("delta"),
                "ar_original":          ar.get("original"),
                "ar_restated":          ar.get("restated"),
                "misleading_passage":   (t2.get("input") or {}).get("passage", "")[:200],
                "misleading_type":      (t2.get("label") or {}).get("misleading_type", ""),
                "extraction_confidence": r.get("metadata", {}).get("extraction_confidence"),
            })

    log.info(f"\n{'='*50}")
    log.info(f"Records built : {len(all_records)}")
    log.info(f"Output JSON   : {out_json.resolve()}")
    log.info(f"Output CSV    : {out_csv.resolve()}")


if __name__ == "__main__":
    main()
