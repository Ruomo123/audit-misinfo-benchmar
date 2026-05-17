"""
Benchmark record builder.

For each case with downloaded EDGAR filings:
  1. Financial figures  → EDGAR XBRL company facts API (exact, no text parsing)
  2. Text passages      → DeepSeek API (MD&A, revenue note, restatement note)
  3. Assembles the full benchmark JSON matching the agreed schema

Input:
  aaer_filtered_v2/selected_cases.json
  edgar_filings/{aaer_num}/meta.json
  edgar_filings/{aaer_num}/*.htm  or  *.pdf

Output:
  benchmark_data/cases.json
  benchmark_data/cases.csv

Usage:
  python build_benchmark.py                          # full run; skips already-built cases
  python build_benchmark.py --aaer-num 4247          # single case (skipped if already built)
  python build_benchmark.py --aaer-num 4247 --force  # force re-run of one case
  python build_benchmark.py --skip-xbrl             # skip XBRL lookup
  python build_benchmark.py --skip-passages         # assemble skeleton records only (no DeepSeek)
"""

import argparse
import csv
import json
import logging
import os
import re
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

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
USER_AGENT    = "Columbia University Research yh3507@columbia.edu"
DELAY         = 0.3
PASSAGE_MODEL = "deepseek-chat"
DEEPSEEK_BASE = "https://api.deepseek.com"

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

# Max characters of filing text sent to DeepSeek per section
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

def fetch_xbrl_facts(cik: str) -> "dict | None":
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


# ── LLM passage extraction ─────────────────────────────────────────────────

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
    "management_attribution": "<exact quote (1-3 sentences) from the original MD&A where management explains what drove revenue, earnings, or financial results for the period — extract the most relevant explanation of performance, even if it sounds reasonable. If no explicit attribution is found, write a 1-sentence summary of what management claims drove results>",
    "true_explanation": "<what actually drove the numbers — use the restatement note if available, otherwise base this on the AAER fraud mechanism. Explain the real accounting manipulation in 1-2 sentences>"
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
    client: OpenAI,
    mechanism: str,
    original_mda: str,
    restated_note: str,
) -> dict:
    restated_section = restated_note[:MAX_SECTION_CHARS] if restated_note else "[No restatement filing available — derive true_explanation from the AAER fraud mechanism above]"
    prompt = PASSAGE_PROMPT.format(
        mechanism=mechanism[:1000],
        original_mda=original_mda[:MAX_SECTION_CHARS],
        restated_note=restated_section,
    )
    try:
        resp = client.chat.completions.create(
            model=PASSAGE_MODEL,
            temperature=0,
            seed=42,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.DOTALL)
        raw = re.sub(r"\s*```$", "", raw, flags=re.DOTALL)
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning(f"  JSON parse error from DeepSeek: {e}")
        return {"extraction_confidence": 0.0, "extraction_notes": f"parse_error: {e}"}
    except Exception as e:
        log.warning(f"  DeepSeek API error: {e}")
        return {"extraction_confidence": 0.0, "extraction_notes": f"api_error: {e}"}


# ── Benchmark record assembly ─────────────────────────────────────────────────

def build_record(
    case: dict,
    period_info: dict,
    xbrl_facts: "dict | None",
    passages: dict,
) -> dict:
    period       = period_info["period"]
    original_fi  = period_info.get("original") or {}
    restated_fi  = period_info.get("restated") or {}
    aaer_num     = case["aaer_num"]

    case_id = f"AAER-{aaer_num}-{period.replace(' ', '-')}"

    # Financial figures from XBRL
    financials: dict[str, dict] = {}
    if xbrl_facts and original_fi.get("accession"):
        period_end = original_fi.get("report_date", "")
        for group in XBRL_CONCEPTS:
            financials[group] = get_xbrl_values(
                xbrl_facts,
                group,
                period_end,
                original_fi["accession"],
                restated_fi.get("accession", ""),
            )

    task1 = passages.get("task1") or {}
    task2 = passages.get("task2") or {}
    task3 = passages.get("task3") or {}

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
                    "management_attribution": task1.get("management_attribution", "") or task1.get("misleading_attribution", ""),
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
    client: OpenAI,
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
        original_path = (period_info.get("original") or {}).get("local_path", "")
        restated_path = (period_info.get("restated") or {}).get("local_path", "")

        if not original_path:
            log.warning(f"  {period}: missing original file path — skipping")
            continue

        orig_file = Path(original_path)
        if not orig_file.exists():
            log.warning(f"  {period}: original filing not found on disk — skipping")
            continue

        rest_file = Path(restated_path) if restated_path else None
        if rest_file and not rest_file.exists():
            log.warning(f"  {period}: restated filing not found on disk — proceeding without it")
            rest_file = None

        if not rest_file:
            log.info(f"  {period}: no restatement — task2_narrative will be empty")

        passages: dict = {}
        if not skip_passages:
            log.info(f"  {period}: extracting text and calling LLM")
            orig_text = extract_text_from_file(orig_file)
            rest_text = extract_text_from_file(rest_file) if rest_file else ""

            original_mda  = extract_section(orig_text, "7", ["7A", "8"])
            restated_note = find_restatement_note(rest_text) if rest_file else ""

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


TASK1_PROMPT = """\
You are an expert in SEC financial disclosures and accounting fraud detection.

Given the MD&A section of a company's original SEC filing and the fraud mechanism \
identified by the SEC, extract two fields:

1. management_attribution: A 1-3 sentence quote or faithful summary of what management \
claimed drove revenue, earnings, or financial results for the period. Extract an exact \
quote if one is clearly present; otherwise summarize the key performance claim management \
made. Do not leave this empty.

2. true_explanation: 1-2 sentences describing what actually drove the numbers, based on \
the restatement note (if provided) or the AAER fraud mechanism.

Return ONLY valid JSON:
{{
  "management_attribution": "...",
  "true_explanation": "..."
}}

FRAUD MECHANISM FROM AAER:
{mechanism}

ORIGINAL FILING — MD&A SECTION:
{original_mda}

RESTATEMENT NOTE (may be empty):
{restated_note}

Return only valid JSON, no markdown fences.
"""


def extract_task1(
    client: OpenAI,
    mechanism: str,
    original_mda: str,
    restated_note: str,
) -> dict:
    restated_section = restated_note[:MAX_SECTION_CHARS] if restated_note else "[Not available — use fraud mechanism for true_explanation]"
    prompt = TASK1_PROMPT.format(
        mechanism=mechanism[:1000],
        original_mda=original_mda[:MAX_SECTION_CHARS],
        restated_note=restated_section,
    )
    try:
        resp = client.chat.completions.create(
            model=PASSAGE_MODEL,
            temperature=0,
            seed=42,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.DOTALL)
        raw = re.sub(r"\s*```$", "", raw, flags=re.DOTALL)
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning(f"  JSON parse error from DeepSeek (task1): {e}")
        return {}
    except Exception as e:
        log.warning(f"  DeepSeek API error (task1): {e}")
        return {}


def patch_task1(args, client: OpenAI) -> None:
    """Re-run task1 extraction in-place for records with empty management_attribution."""
    out_json = OUTPUT_DIR / "cases.json"
    if not out_json.exists():
        log.error("No cases.json found — run full build first")
        return

    records = json.loads(out_json.read_text(encoding="utf-8"))
    cases_by_aaer = {}
    cases_data = json.loads(Path(args.cases).read_text(encoding="utf-8"))
    for c in cases_data:
        cases_by_aaer[c["aaer_num"]] = c

    patched = 0
    for rec in records:
        aaer_num = rec["aaer_num"]
        if args.aaer_num and aaer_num != args.aaer_num:
            continue

        t1 = (rec.get("tasks") or {}).get("task1_profit_source") or {}
        attr = (t1.get("input") or {}).get("management_attribution", "")
        if attr.strip() and not args.force:
            continue  # already has attribution, skip unless --force

        period = rec.get("fiscal_period", "?")
        log.info(f"  Patching task1 for AAER-{aaer_num} {period}")

        # Load filing files from meta.json
        meta_path = FILINGS_DIR / str(aaer_num) / "meta.json"
        if not meta_path.exists():
            log.warning(f"    No meta.json — skipping")
            continue
        filing_meta = json.loads(meta_path.read_text(encoding="utf-8"))

        # Find the period_info matching this record's fiscal_period
        period_info = next(
            (p for p in filing_meta.get("periods", []) if p.get("period") == period),
            None,
        )
        if not period_info:
            log.warning(f"    Period {period} not found in meta.json — skipping")
            continue

        orig_path = (period_info.get("original") or {}).get("local_path", "")
        rest_path = (period_info.get("restated") or {}).get("local_path", "")
        if not orig_path or not Path(orig_path).exists():
            log.warning(f"    Original file missing — skipping")
            continue

        orig_text = extract_text_from_file(Path(orig_path))
        rest_text = extract_text_from_file(Path(rest_path)) if rest_path and Path(rest_path).exists() else ""
        original_mda = extract_section(orig_text, "7", ["7A", "8"])
        restated_note = find_restatement_note(rest_text) if rest_text else ""

        case = cases_by_aaer.get(aaer_num, {})
        mechanism = case.get("specific_mechanism", "")

        result = extract_task1(client, mechanism, original_mda or orig_text[:3000], restated_note)
        if not result:
            log.warning(f"    Empty response — skipping")
            continue

        rec["tasks"]["task1_profit_source"]["input"]["management_attribution"] = result.get("management_attribution", "")
        if result.get("true_explanation"):
            rec["tasks"]["task1_profit_source"]["label"]["true_explanation"] = result["true_explanation"]
        patched += 1

    out_json.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Patched task1 for {patched} record(s). Saved to {out_json}")


def parse_args():
    parser = argparse.ArgumentParser(description="Build benchmark dataset from EDGAR filings")
    parser.add_argument("--cases", default=str(CASES_FILE))
    parser.add_argument("--filings-dir", default=str(FILINGS_DIR))
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--aaer-num", type=int, default=None)
    parser.add_argument("--force", action="store_true",
                        help="Re-run even if aaer_num already exists in output (use with --aaer-num)")
    parser.add_argument("--skip-xbrl", action="store_true")
    parser.add_argument("--skip-passages", action="store_true",
                        help="Skip DeepSeek extraction; assemble skeleton records only")
    parser.add_argument("--patch-task1", action="store_true",
                        help="Re-run task1 extraction in-place for records with empty management_attribution")
    return parser.parse_args()


def main():
    args = parse_args()
    global FILINGS_DIR, OUTPUT_DIR
    FILINGS_DIR = Path(args.filings_dir)
    OUTPUT_DIR  = Path(args.out_dir)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        log.warning("DEEPSEEK_API_KEY not set — passage extraction will fail")
    client = OpenAI(api_key=api_key, base_url=DEEPSEEK_BASE)

    if args.patch_task1:
        patch_task1(args, client)
        return

    cases = json.loads(Path(args.cases).read_text(encoding="utf-8"))
    if args.aaer_num:
        cases = [c for c in cases if c.get("aaer_num") == args.aaer_num]
    log.info(f"Building benchmark for {len(cases)} cases")

    # Resume: load existing output and skip already-built cases
    # --force only bypasses the skip-check; existing records for other cases are always preserved
    out_json = OUTPUT_DIR / "cases.json"
    existing_records: list[dict] = []
    done_aaers: set[int] = set()
    if out_json.exists():
        try:
            existing_records = json.loads(out_json.read_text(encoding="utf-8"))
            done_aaers = {r["aaer_num"] for r in existing_records}
            log.info(f"Loaded {len(done_aaers)} existing case(s)")
        except Exception as e:
            log.warning(f"Could not read existing output, starting fresh: {e}")

    all_records: list[dict] = []

    for i, case in enumerate(cases, 1):
        aaer = case.get("aaer_num", "?")
        entity = case.get("filing_entity") or case.get("aaer_respondent", "")

        if aaer in done_aaers and not args.force:
            log.info(f"[{i}/{len(cases)}] AAER-{aaer} — skipping (already built)")
            continue

        log.info(f"[{i}/{len(cases)}] AAER-{aaer} — {entity[:60]}")

        try:
            records = process_case(case, client, args.skip_xbrl, args.skip_passages)
        except Exception as e:
            log.error(f"  Unexpected error: {e}")
            records = []

        all_records.extend(records)
        log.info(f"  → {len(records)} benchmark record(s) produced")

    # Merge with existing: drop old records for any aaer_num we just rebuilt, then prepend existing
    rebuilt_aaers = {r["aaer_num"] for r in all_records}
    kept_existing = [r for r in existing_records if r["aaer_num"] not in rebuilt_aaers]
    all_records = kept_existing + all_records
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
