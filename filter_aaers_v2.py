"""
AAER Filtering & Classification Pipeline — v2

Improvements over v1 (filter_aaers.py):
  1. Structure-aware: parse Roman-numeral sections, locate the SUMMARY/FINDINGS
     block, and run classification only on that block (not full-document noise).
  2. Document-type gate: reinstatements, dismissals, application grants are
     flagged as NON_ENFORCEMENT and excluded from fraud categories.
  3. Legal-signal extraction: Rule 102(e), Section 10A, ASC codes, SAB guidance,
     filing-rule citations — diagnostic signals captured as structured fields.
  4. LLM classification (Gemini 2.5 Flash) on the SUMMARY block only, returning
     a structured JSON with primary/secondary category, respondent type,
     dollar impact, period, and confidence.
  5. HTML-content detection: some "PDFs" in aaer_data are actually HTML pages;
     those are flagged and skipped.

Outputs:
  aaer_filtered_v2/
    aaer_dataset_v2.json
    aaer_dataset_v2.csv
    aaer_by_category_v2.json
    comparison_vs_v1.json
    extraction_failures_v2.txt
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import pypdf
except ImportError:
    pypdf = None

if pdfplumber is None and pypdf is None:
    raise ImportError("Install at least one PDF library: pip install pdfplumber pypdf")

try:
    import google.generativeai as genai
except ImportError:
    genai = None


# ── CONFIG ────────────────────────────────────────────────────────────────────
AAER_DIR   = Path("aaer_data")
OUTPUT_DIR = Path("aaer_filtered_v2")
V1_DIR     = Path("aaer_filtered")  # for comparison
MIN_CHARS  = 200
LLM_MODEL  = "gemini-2.5-flash"
LLM_WORKERS = 6  # parallel Gemini calls
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("filter_v2")


FRAUD_CATEGORIES = [
    "REVENUE_TIMING",
    "EXPENSE_DEFERRAL",
    "ACCOUNTING_ESTIMATE",
    "EARNINGS_SMOOTHING",
    "NARRATIVE_DISTORTION",
    "OTHER_FRAUD",        # insider trading, bribery, audit independence lapses w/o the above
    "NON_ENFORCEMENT",    # reinstatement, dismissal, procedural
]


# ── Document-type gate (negative keywords) ───────────────────────────────────
# Match in the document TITLE block (first 1500 chars) to classify non-enforcement docs.
NON_ENFORCEMENT_TITLE_PATTERNS = [
    r"ORDER\s+GRANTING\s+APPLICATION\s+FOR\s+REINSTATEMENT",
    r"ORDER\s+OF\s+REINSTATEMENT",
    r"APPLICATION\s+FOR\s+REINSTATEMENT",
    r"ORDER\s+DISMISSING\s+PROCEEDINGS",
    r"NOTICE\s+OF\s+WITHDRAWAL",
    r"ORDER\s+LIFTING\s+SUSPENSION",
    r"OPINION\s+AND\s+ORDER\s+ON\s+REVIEW",  # appellate review, not new charge
]


# ── Legal / statute signal extraction ────────────────────────────────────────
LEGAL_SIGNALS = {
    # Auditor-specific
    "rule_102e":      r"Rule\s+102\s*\(\s*e\s*\)",
    "section_10A":    r"Section\s+10A\b",
    "section_4C":     r"Section\s+4C\b",
    "pcaob_as":       r"PCAOB\s+(?:Auditing\s+Standard|AS)\s+\d+",
    "au_c":           r"\bAU-C\s+\d+",

    # Revenue standards
    "asc_606":        r"ASC\s+606\b|Topic\s+606\b",
    "asc_605":        r"ASC\s+605\b|Topic\s+605\b",
    "sab_101":        r"SAB\s+(?:No\.?\s*)?101\b|Staff\s+Accounting\s+Bulletin\s+(?:No\.?\s*)?101",
    "sab_104":        r"SAB\s+(?:No\.?\s*)?104\b",

    # Capitalization / impairment
    "asc_350":        r"ASC\s+350\b|Topic\s+350\b",  # goodwill
    "asc_360":        r"ASC\s+360\b|Topic\s+360\b",  # PP&E impairment
    "asc_842":        r"ASC\s+842\b|Topic\s+842\b",  # leases

    # Reserves / smoothing
    "asc_450":        r"ASC\s+450\b|Topic\s+450\b",  # contingencies
    "sab_99":         r"SAB\s+(?:No\.?\s*)?99\b",     # materiality

    # Filing / disclosure
    "section_13a":    r"Section\s+13\s*\(\s*a\s*\)",
    "rule_10b5":      r"Rule\s+10b[\-\s]?5",
    "item_303":       r"Item\s+303\s+of\s+Regulation\s+S[\-\s]?K",
    "section_17a":    r"Section\s+17\s*\(\s*a\s*\)",
}


# ── Retained v1 lexicon for hybrid hit-count (not primary classifier) ───────
V1_KEYWORDS = {
    "REVENUE_TIMING": [
        r"bill[\s\-]and[\s\-]hold",
        r"channel[\s\-]stuffing",
        r"round[\s\-]trip\s+(?:transaction|sale)",
        r"fictitious\s+(?:revenue|sale|transaction)",
        r"side\s+agreement",
        r"premature(?:ly)?\s+recogni[zs]",
        r"improp(?:er(?:ly)?|erly)\s+recogni[zs]",
    ],
    "EXPENSE_DEFERRAL": [
        r"improp(?:er(?:ly)?|erly)\s+capitali[zs]",
        r"capitali[zs]e[d]?\s+(?:operating\s+)?(?:expense|cost)",
        r"defer(?:red)?\s+(?:operating\s+)?(?:expense|cost)",
    ],
    "ACCOUNTING_ESTIMATE": [
        r"useful\s+li(?:fe|ves)",
        r"salvage\s+value",
        r"(?:manipulat|alter|extend)\w*\s+depreciation",
        r"(?:delay|avoid|manipulat)\w*\s+goodwill\s+impairment",
    ],
    "EARNINGS_SMOOTHING": [
        r"cookie[\s\-]jar",
        r"big[\s\-]bath",
        r"excess(?:ive)?\s+reserve",
        r"(?:manag(?:ed?|ing)|manipulat\w+)\s+earnings?",
        r"discretionary\s+accrual",
    ],
    "NARRATIVE_DISTORTION": [
        r"misleading\s+(?:and\s+)?(?:MD&A|management.s\s+discussion)",
        r"material\s+omission",
        r"misleading\s+disclosure",
        r"false\s+(?:and\s+misleading\s+)?(?:narrative|statement)",
    ],
}


# ── PDF / text extraction ────────────────────────────────────────────────────
def is_html_content(text: str) -> bool:
    """Detect HTML pollution (some 'PDFs' were served as HTML by SEC)."""
    head = text[:500].lower()
    return "<!doctype html" in head or "<html" in head


def extract_text_pdfplumber(pdf_path: Path) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(p.extract_text() or "" for p in pdf.pages)


def extract_text_pypdf(pdf_path: Path) -> str:
    reader = pypdf.PdfReader(str(pdf_path))
    return "\n".join(p.extract_text() or "" for p in reader.pages)


def extract_text(pdf_path: Path) -> tuple[str, str]:
    """Returns (text, backend). Tries pdfplumber first, then pypdf."""
    if pdfplumber:
        try:
            t = extract_text_pdfplumber(pdf_path)
            if t.strip():
                return t, "pdfplumber"
        except Exception as e:
            log.debug(f"pdfplumber failed on {pdf_path.name}: {e}")
    if pypdf:
        try:
            t = extract_text_pypdf(pdf_path)
            if t.strip():
                return t, "pypdf"
        except Exception as e:
            log.debug(f"pypdf failed on {pdf_path.name}: {e}")
    return "", "failed"


# ── Structure parser ─────────────────────────────────────────────────────────
# Roman numeral sections I. II. III. IV. V. VI. VII. are the structural skeleton
# of an AAER. The "SUMMARY" subheader usually sits inside section III.
ROMAN_RE = re.compile(
    r"^\s*(I|II|III|IV|V|VI|VII|VIII|IX)\.\s*$",
    re.MULTILINE,
)
SUMMARY_HEADER_RE = re.compile(r"^\s*SUMMARY\s*$", re.MULTILINE | re.IGNORECASE)
FINDINGS_HEADER_RE = re.compile(r"^\s*FINDINGS\s*$", re.MULTILINE | re.IGNORECASE)
FACTS_HEADER_RE = re.compile(r"^\s*FACTS\s*$", re.MULTILINE | re.IGNORECASE)


def parse_structure(text: str) -> dict:
    """
    Split an AAER into sections keyed by Roman numeral, plus pull out
    the SUMMARY / FINDINGS / FACTS subblocks.

    Returns {
        "title_block": str,   # everything before I.
        "sections": {"I": str, "II": str, ...},
        "summary_block": str | "",
        "findings_block": str | "",
        "classification_context": str,  # best-effort chunk for LLM
    }
    """
    roman_matches = list(ROMAN_RE.finditer(text))

    if not roman_matches:
        # Fallback: no structure detected. Use first 3000 chars after header.
        title_block = text[:1500]
        body = text[1500:4500]
        return {
            "title_block":            title_block,
            "sections":               {},
            "summary_block":          "",
            "findings_block":         "",
            "classification_context": body.strip() or text[:3000],
        }

    title_block = text[:roman_matches[0].start()]
    sections = {}
    for i, m in enumerate(roman_matches):
        label = m.group(1)
        start = m.end()
        end = roman_matches[i + 1].start() if i + 1 < len(roman_matches) else len(text)
        sections[label] = text[start:end].strip()

    # Summary is usually inside III.; fall back to scanning all sections.
    summary_block = ""
    findings_block = ""
    for label in ("III", "IV", "II"):
        sec = sections.get(label, "")
        if not sec:
            continue
        m = SUMMARY_HEADER_RE.search(sec)
        if m:
            # Grab from SUMMARY header to next all-caps header or section end
            summary_block = _slice_until_caps_header(sec, m.end())
            break

    for label in ("III", "IV"):
        sec = sections.get(label, "")
        if not sec:
            continue
        m = FINDINGS_HEADER_RE.search(sec) or FACTS_HEADER_RE.search(sec)
        if m:
            findings_block = _slice_until_caps_header(sec, m.end())
            break

    # Best chunk for LLM: prefer SUMMARY, then FINDINGS, then first ~4000 chars of III.
    if summary_block and len(summary_block) > 200:
        context = summary_block[:4000]
    elif findings_block and len(findings_block) > 200:
        context = findings_block[:4000]
    elif sections.get("III"):
        context = sections["III"][:4000]
    else:
        context = text[1500:5500]

    return {
        "title_block":            title_block,
        "sections":               sections,
        "summary_block":          summary_block,
        "findings_block":         findings_block,
        "classification_context": context.strip(),
    }


def _slice_until_caps_header(text: str, start: int) -> str:
    """Slice from `start` until the next ALL-CAPS header line or end of text."""
    caps_header = re.compile(r"\n\s*[A-Z][A-Z ]{3,}\s*\n")
    m = caps_header.search(text, start)
    end = m.start() if m else len(text)
    return text[start:end].strip()


# ── Document-type gate ───────────────────────────────────────────────────────
def classify_document_type(title_block: str) -> dict:
    """
    Detects if this AAER is a NEW enforcement action or something procedural
    (reinstatement, dismissal, administrative review).
    """
    for pat in NON_ENFORCEMENT_TITLE_PATTERNS:
        if re.search(pat, title_block, re.IGNORECASE):
            return {
                "is_new_enforcement": False,
                "document_type": "non_enforcement",
                "type_match":    pat,
            }
    # Also gate on "CEASE-AND-DESIST" or "REMEDIAL SANCTIONS" presence — those indicate a new order.
    if re.search(r"CEASE[\-\s]AND[\-\s]DESIST|REMEDIAL\s+SANCTIONS|ADMINISTRATIVE\s+PROCEEDINGS", title_block, re.IGNORECASE):
        return {
            "is_new_enforcement": True,
            "document_type": "enforcement_order",
            "type_match":    None,
        }
    return {
        "is_new_enforcement": True,  # default positive; LLM will refine
        "document_type": "unknown",
        "type_match":    None,
    }


# ── Legal signal extraction ──────────────────────────────────────────────────
def extract_legal_signals(text: str) -> dict:
    """Return dict of which statute/standard codes appear in the text."""
    hits = {}
    for name, pattern in LEGAL_SIGNALS.items():
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            hits[name] = m.group(0)
    return hits


# ── Rule-based heuristic classification (fallback when LLM unavailable) ─────
def lexicon_classify(text: str) -> dict:
    """Count v1-style keyword hits per category. Used as a sanity baseline."""
    out = defaultdict(list)
    for cat, patterns in V1_KEYWORDS.items():
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                out[cat].append(pat)
    return dict(out)


# ── Gemini LLM classification ────────────────────────────────────────────────
LLM_SCHEMA = {
    "type": "object",
    "properties": {
        "primary_category": {"type": "string", "enum": FRAUD_CATEGORIES},
        "secondary_categories": {
            "type": "array",
            "items": {"type": "string", "enum": FRAUD_CATEGORIES},
        },
        "respondent_type": {
            "type": "string",
            "enum": ["issuer", "individual_executive", "CPA_individual",
                     "audit_firm", "mixed", "unclear"],
        },
        "is_new_enforcement": {"type": "boolean"},
        "dollar_impact": {"type": "string"},
        "period": {"type": "string"},
        # --- Downstream-usability fields: used by select_top_cases.py ----------
        "specific_mechanism": {
            "type": "string",
            "description": "One-sentence concrete description of the accounting manipulation: what was misstated, by how much, via what technique. Empty string if the AAER does not describe a specific accounting manipulation.",
        },
        "traceable_to_financials": {
            "type": "boolean",
            "description": "True if a reader could identify a specific filed 10-K/10-Q/20-F/earnings release whose numbers are the ones being alleged as misstated. False for CPA-only discipline cases, procedural orders, or cases without a named issuer.",
        },
        "fiscal_periods_affected": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of fiscal periods in canonical form, e.g. ['FY2001-Q2', 'FY2001-Q3', 'FY2001']. Empty if not stated.",
        },
        "filing_entity": {
            "type": "string",
            "description": "The issuer whose financial statements are the ones allegedly misstated. For a CPA case, the audit client. Empty if no issuer is named.",
        },
        "confidence": {"type": "number"},
        "reasoning": {"type": "string"},
    },
    "required": ["primary_category", "respondent_type", "is_new_enforcement",
                 "traceable_to_financials", "confidence"],
}

LLM_PROMPT_TEMPLATE = """You are an expert in SEC accounting enforcement actions.

Below is the most informative section from an SEC AAER (Accounting and Auditing Enforcement Release). Classify the primary accounting-fraud mechanism.

Category definitions:
- REVENUE_TIMING: Premature / improper revenue recognition. Bill-and-hold, channel stuffing, round-trip, fictitious revenue, side agreements, multi-element arrangement abuse, contingent sales treated as final.
- EXPENSE_DEFERRAL: Improperly capitalizing operating costs, deferring expenses that should be period costs, line-cost capitalization (WorldCom-style), capitalizing software/R&D outside GAAP.
- ACCOUNTING_ESTIMATE: Manipulating depreciation useful life/salvage, delayed goodwill impairment, pension actuarial assumption abuse, loan-loss or inventory-reserve estimate manipulation.
- EARNINGS_SMOOTHING: Cookie-jar reserves, big-bath restructuring charges, excess reserve accruals later released to smooth income, discretionary accrual abuse.
- NARRATIVE_DISTORTION: Misleading MD&A, misleading earnings releases/press releases, material omissions, selective disclosure, pro-forma abuse — even when underlying accounting is arguably compliant.
- OTHER_FRAUD: Auditor independence violations, insider trading, FCPA/bribery, audit-documentation destruction, Rule 102(e) discipline not tied to the above accounting schemes.
- NON_ENFORCEMENT: Reinstatement orders, dismissals, administrative-review opinions, application grants. Not a new charge.

Other fields:
- respondent_type: who is being charged
- is_new_enforcement: false if reinstatement/dismissal/review, true for new cease-and-desist/sanctions orders
- dollar_impact: the stated overstatement/understatement amount (e.g. "overstated revenue by approximately $34 million"), else empty string
- period: free-text fiscal period at issue (e.g. "FY2019-Q2 through FY2021-Q4"), else empty string
- specific_mechanism: ONE sentence concretely stating what was misstated, by how much, via what technique. Empty string if the AAER only cites statute violations without describing an accounting manipulation.
- traceable_to_financials: true ONLY if a reader could, in principle, pull up a specific filed 10-K / 10-Q / 20-F / earnings release whose numbers are alleged to be misstated. False for: pure CPA Rule 102(e) discipline without a named issuer/period; procedural orders; insider-trading-only cases.
- fiscal_periods_affected: canonicalized periods like ["FY2001-Q2", "FY2001-Q3", "FY2001"]. Empty if not clearly stated.
- filing_entity: the issuer whose financials are alleged misstated (company name as it would appear on EDGAR). For a CPA case, the audit client. Empty if no issuer named.
- confidence: 0–1 (be conservative if ambiguous)
- reasoning: one sentence explaining primary_category choice

Document SUMMARY/FINDINGS section:
---
{context}
---

Return valid JSON only, matching the provided schema."""


class GeminiClassifier:
    def __init__(self, api_key: str, model: str = LLM_MODEL):
        if genai is None:
            raise ImportError("Install google-generativeai: pip install google-generativeai")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": LLM_SCHEMA,
                "temperature": 0.0,
            },
        )

    def classify(self, context: str, retries: int = 3) -> dict | None:
        prompt = LLM_PROMPT_TEMPLATE.format(context=context[:6000])
        last_err = None
        for attempt in range(retries):
            try:
                resp = self.model.generate_content(prompt)
                return json.loads(resp.text)
            except Exception as e:
                last_err = e
                wait = 2 ** attempt
                log.debug(f"Gemini retry {attempt+1} after {wait}s: {e}")
                time.sleep(wait)
        log.warning(f"Gemini classification failed after {retries} tries: {last_err}")
        return None


# ── Per-PDF pipeline ─────────────────────────────────────────────────────────
def process_one(pdf_path: Path, meta: dict, classifier: GeminiClassifier | None) -> dict:
    text, backend = extract_text(pdf_path)
    record = {
        **meta,
        "pdf_name":   pdf_path.name,
        "backend":    backend,
        "char_count": len(text),
    }

    if len(text.strip()) < MIN_CHARS:
        record["status"] = "low_text"
        return record

    if is_html_content(text):
        record["status"] = "html_content"
        return record

    parsed = parse_structure(text)
    doctype = classify_document_type(parsed["title_block"])
    legal = extract_legal_signals(text)
    lex_hits = lexicon_classify(parsed["classification_context"] or text[:8000])

    record.update({
        "status":              "ok",
        "document_type":       doctype["document_type"],
        "is_new_enforcement_preview": doctype["is_new_enforcement"],
        "legal_signals":       legal,
        "lexicon_hits":        {k: len(v) for k, v in lex_hits.items()},
        "has_summary_block":   bool(parsed["summary_block"]),
        "has_findings_block":  bool(parsed["findings_block"]),
        "classification_context_chars": len(parsed["classification_context"]),
    })

    # LLM classification (skip if non-enforcement detected up front)
    if classifier is not None and parsed["classification_context"]:
        llm = classifier.classify(parsed["classification_context"])
        if llm:
            record["llm"] = llm
            record["primary_category"] = llm.get("primary_category")
            record["secondary_categories"] = llm.get("secondary_categories", [])
            record["respondent_type"] = llm.get("respondent_type")
            record["is_new_enforcement"] = llm.get("is_new_enforcement")
            record["dollar_impact"] = llm.get("dollar_impact")
            record["period"] = llm.get("period")
            record["specific_mechanism"] = llm.get("specific_mechanism")
            record["traceable_to_financials"] = llm.get("traceable_to_financials")
            record["fiscal_periods_affected"] = llm.get("fiscal_periods_affected", [])
            record["filing_entity"] = llm.get("filing_entity")
            record["llm_confidence"] = llm.get("confidence")
        else:
            record["llm_error"] = True

    return record


# ── Main ─────────────────────────────────────────────────────────────────────
def load_metadata_index() -> dict:
    index_path = AAER_DIR / "aaer_index.json"
    if not index_path.exists():
        return {}
    entries = json.loads(index_path.read_text(encoding="utf-8"))
    return {e["aaer_num"]: e for e in entries if e.get("aaer_num")}


def load_v1_dataset() -> dict:
    """Keyed by aaer_num for side-by-side comparison."""
    path = V1_DIR / "aaer_dataset.csv"
    if not path.exists():
        return {}
    out = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                out[int(row["aaer_num"])] = row
            except (ValueError, KeyError):
                continue
    return out


def pick_sample_pdfs(pdfs: list[Path], n: int, seed: int = 42) -> list[Path]:
    """Stratified sample across the AAER number range for temporal coverage."""
    rng = random.Random(seed)
    if len(pdfs) <= n:
        return pdfs
    # Sort by AAER number
    def num_of(p):
        m = re.search(r"AAER-(\d+)", p.stem)
        return int(m.group(1)) if m else 0
    sorted_pdfs = sorted(pdfs, key=num_of)
    # Divide into n buckets and pick one random PDF from each
    step = len(sorted_pdfs) / n
    sample = []
    for i in range(n):
        start = int(i * step)
        end = int((i + 1) * step)
        bucket = sorted_pdfs[start:end] or [sorted_pdfs[min(start, len(sorted_pdfs) - 1)]]
        sample.append(rng.choice(bucket))
    return sample


def save_outputs(results: list[dict], v1_map: dict):
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Main dataset
    (OUTPUT_DIR / "aaer_dataset_v2.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # CSV (flat)
    csv_path = OUTPUT_DIR / "aaer_dataset_v2.csv"
    fields = [
        "aaer_num", "pdf_name", "respondent", "date", "release_no",
        "status", "char_count", "document_type",
        "primary_category", "secondary_categories",
        "respondent_type", "is_new_enforcement",
        "traceable_to_financials", "filing_entity",
        "fiscal_periods_affected", "specific_mechanism",
        "dollar_impact", "period", "llm_confidence",
        "legal_signals_list", "lexicon_hits",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            row = dict(r)
            row["secondary_categories"] = "|".join(r.get("secondary_categories", []) or [])
            row["fiscal_periods_affected"] = "|".join(r.get("fiscal_periods_affected", []) or [])
            row["legal_signals_list"] = "|".join((r.get("legal_signals") or {}).keys())
            row["lexicon_hits"] = json.dumps(r.get("lexicon_hits") or {})
            w.writerow(row)

    # By-category
    by_cat = defaultdict(list)
    for r in results:
        cat = r.get("primary_category")
        if cat:
            by_cat[cat].append({
                "aaer_num": r.get("aaer_num"),
                "respondent": r.get("respondent"),
                "document_type": r.get("document_type"),
                "dollar_impact": r.get("dollar_impact"),
                "confidence": r.get("llm_confidence"),
            })
    (OUTPUT_DIR / "aaer_by_category_v2.json").write_text(
        json.dumps(dict(by_cat), indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # v1-vs-v2 comparison
    comparison = []
    for r in results:
        num = r.get("aaer_num")
        v1 = v1_map.get(num, {})
        comparison.append({
            "aaer_num": num,
            "respondent": r.get("respondent"),
            "v1_categories":  v1.get("categories", ""),
            "v1_auditor":     v1.get("auditor", ""),
            "v2_primary":     r.get("primary_category"),
            "v2_secondary":   r.get("secondary_categories"),
            "v2_respondent_type": r.get("respondent_type"),
            "v2_is_new_enforcement": r.get("is_new_enforcement"),
            "v2_document_type": r.get("document_type"),
            "v2_confidence":  r.get("llm_confidence"),
            "v2_legal_signals": list((r.get("legal_signals") or {}).keys()),
        })
    (OUTPUT_DIR / "comparison_vs_v1.json").write_text(
        json.dumps(comparison, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=50, help="sample size (0 = full run)")
    ap.add_argument("--no-llm", action="store_true", help="skip Gemini, use lexicon only")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    meta_index = load_metadata_index()
    v1_map = load_v1_dataset()
    log.info(f"Loaded v1 baseline: {len(v1_map)} AAERs")

    pdfs = sorted(AAER_DIR.glob("AAER-*.pdf"))
    log.info(f"Found {len(pdfs)} AAER PDFs in {AAER_DIR}")

    if args.sample > 0:
        pdfs = pick_sample_pdfs(pdfs, args.sample, seed=args.seed)
        log.info(f"Stratified sample: {len(pdfs)} PDFs")

    classifier = None
    if not args.no_llm:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            log.warning("GEMINI_API_KEY not set, falling back to lexicon only")
        else:
            classifier = GeminiClassifier(api_key)
            log.info(f"LLM classifier ready: {LLM_MODEL}")

    results = []
    # Run PDF extraction + rule-based sequentially; LLM calls parallelized inside
    with ThreadPoolExecutor(max_workers=LLM_WORKERS) as ex:
        futures = {}
        for pdf in pdfs:
            num_m = re.search(r"AAER-(\d+)", pdf.stem)
            num = int(num_m.group(1)) if num_m else None
            meta = meta_index.get(num, {
                "aaer_num": num, "respondent": "", "date": "", "release_no": "", "pdf_url": "",
            })
            fut = ex.submit(process_one, pdf, meta, classifier)
            futures[fut] = pdf

        for i, fut in enumerate(as_completed(futures), 1):
            pdf = futures[fut]
            try:
                rec = fut.result()
                results.append(rec)
            except Exception as e:
                log.error(f"  ERROR on {pdf.name}: {e}")
                results.append({"pdf_name": pdf.name, "status": "error", "error": str(e)})
            if i % 10 == 0:
                log.info(f"  progress {i}/{len(pdfs)}")

    results.sort(key=lambda r: r.get("aaer_num") or 0)
    save_outputs(results, v1_map)

    # Summary stats
    statuses = defaultdict(int)
    cats = defaultdict(int)
    doc_types = defaultdict(int)
    for r in results:
        statuses[r.get("status", "unknown")] += 1
        if r.get("primary_category"):
            cats[r["primary_category"]] += 1
        if r.get("document_type"):
            doc_types[r["document_type"]] += 1

    log.info(f"{'='*60}")
    log.info(f"Total:       {len(results)}")
    log.info(f"Statuses:    {dict(statuses)}")
    log.info(f"Doc types:   {dict(doc_types)}")
    log.info(f"Categories:  {dict(cats)}")
    log.info(f"Output dir:  {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
