"""
LLM enrichment of AAER records using the Claude API.

Reads AAER texts from aaer_filtered/texts/ and adds structured metadata
needed for EDGAR filing lookup and benchmark construction.

Output: aaer_filtered_v2/aaer_dataset_v2.json

Usage:
  python enrich_aaers.py
  python enrich_aaers.py --max-cases 10
  python enrich_aaers.py --resume        # skip already-processed records
  python enrich_aaers.py --model claude-haiku-4-5-20251001
"""

import argparse
import json
import logging
import re
import time
from pathlib import Path

import anthropic

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_DATASET  = Path("aaer_filtered/aaer_dataset.json")
TEXTS_DIR      = Path("aaer_filtered/texts")
OUTPUT_DIR     = Path("aaer_filtered_v2")
OUTPUT_FILE    = OUTPUT_DIR / "aaer_dataset_v2.json"
DEFAULT_MODEL  = "claude-haiku-4-5-20251001"
MAX_TEXT_CHARS = 8000   # truncate AAER text sent to API to control cost
DELAY          = 0.5    # seconds between API calls
# ──────────────────────────────────────────────────────────────────────────────

VALID_CATEGORIES = {
    "REVENUE_TIMING",
    "EXPENSE_DEFERRAL",
    "ACCOUNTING_ESTIMATE",
    "EARNINGS_SMOOTHING",
    "NARRATIVE_DISTORTION",
    "OTHER",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("enrich_aaers.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


EXTRACTION_PROMPT = """\
You are an expert in SEC enforcement actions and financial accounting fraud.

Below is the text of an SEC Accounting and Auditing Enforcement Release (AAER).
Extract the following fields and return ONLY a valid JSON object — no markdown, no explanation.

Fields to extract:
- filing_entity (string): The public company name exactly as it would appear in EDGAR filings. If the respondent is an individual (auditor, officer) rather than a public company, set to null.
- ticker (string|null): Stock ticker symbol if mentioned, else null.
- fiscal_periods_affected (list of strings): Fiscal periods where fraud occurred. Use format "FY{year}" for full years (e.g. "FY2018"), "Q{n} {year}" for quarters (e.g. "Q1 2019"). Return [] if unclear.
- primary_category (string): The single best-fit fraud category from this list: REVENUE_TIMING, EXPENSE_DEFERRAL, ACCOUNTING_ESTIMATE, EARNINGS_SMOOTHING, NARRATIVE_DISTORTION, OTHER.
- secondary_categories (list of strings): Any additional applicable categories from the same list (can be empty).
- specific_mechanism (string): 1-2 sentence description of exactly how the fraud was carried out (e.g. "Recognized full purchase order value at contract signing before SIM cards shipped or customers activated services, violating ASC 606 transfer-of-control requirements.").
- dollar_impact (string): Key dollar amounts mentioned (revenue overstated, restatement size, etc.). Quote directly from text. Empty string if not mentioned.
- respondent_type (string): "issuer" if the primary respondent is the company itself, "auditor" if primarily an audit firm or CPA, "mixed" if both are charged.
- traceable_to_financials (boolean): True if the fraud directly affected reported financial statements (income statement, balance sheet) and we could expect to find a 10-K or 10-Q filing to compare against a restatement.
- is_new_enforcement (boolean): True if this is an original enforcement action. False if it is a reinstatement, dismissal, or administrative proceeding to lift a prior bar.
- legal_signals (object): Keys are signal names present in the text, values are true. Possible keys: "consent_order", "disgorgement", "civil_penalty", "officer_bar", "practice_bar", "permanent_injunction", "suspension".
- llm_confidence (float): Your confidence 0.0-1.0 that this case is traceable to a specific public company EDGAR filing and would make a good benchmark case.
- reasoning (string): 1-2 sentence justification for your confidence score and primary category.

AAER TEXT:
{aaer_text}

Return only valid JSON, no markdown fences.
"""


def load_base_dataset() -> dict[int, dict]:
    """Load existing aaer_dataset.json keyed by aaer_num."""
    if not INPUT_DATASET.exists():
        log.warning(f"Base dataset not found: {INPUT_DATASET}. Proceeding with text files only.")
        return {}
    records = json.loads(INPUT_DATASET.read_text(encoding="utf-8"))
    return {r["aaer_num"]: r for r in records if r.get("aaer_num")}


def load_existing_output() -> dict[int, dict]:
    """Load already-processed records from output file (for --resume)."""
    if not OUTPUT_FILE.exists():
        return {}
    records = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
    return {r["aaer_num"]: r for r in records if r.get("aaer_num")}


def call_claude(client: anthropic.Anthropic, model: str, aaer_text: str) -> dict:
    prompt = EXTRACTION_PROMPT.format(aaer_text=aaer_text[:MAX_TEXT_CHARS])
    message = client.messages.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text.strip()
    # Strip markdown fences if model wrapped the JSON anyway
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def validate_llm_output(data: dict) -> dict:
    """Coerce / fill defaults for any missing or malformed fields."""
    data.setdefault("filing_entity", None)
    data.setdefault("ticker", None)
    data.setdefault("fiscal_periods_affected", [])
    if data.get("primary_category") not in VALID_CATEGORIES:
        data["primary_category"] = "OTHER"
    data["secondary_categories"] = [
        c for c in data.get("secondary_categories", []) if c in VALID_CATEGORIES
    ]
    data.setdefault("specific_mechanism", "")
    data.setdefault("dollar_impact", "")
    if data.get("respondent_type") not in ("issuer", "auditor", "mixed"):
        data["respondent_type"] = "issuer"
    data["traceable_to_financials"] = bool(data.get("traceable_to_financials", False))
    data["is_new_enforcement"] = bool(data.get("is_new_enforcement", True))
    data.setdefault("legal_signals", {})
    conf = data.get("llm_confidence", 0.5)
    data["llm_confidence"] = max(0.0, min(1.0, float(conf)))
    data.setdefault("reasoning", "")
    return data


def enrich_record(
    client: anthropic.Anthropic,
    model: str,
    aaer_num: int,
    base: dict,
    text_path: Path,
) -> dict:
    text = text_path.read_text(encoding="utf-8")
    try:
        llm_data = call_claude(client, model, text)
        llm_data = validate_llm_output(llm_data)
        status = "enriched"
    except json.JSONDecodeError as e:
        log.warning(f"  JSON parse error for AAER-{aaer_num}: {e}")
        llm_data = validate_llm_output({})
        status = "parse_error"
    except Exception as e:
        log.warning(f"  API error for AAER-{aaer_num}: {e}")
        llm_data = validate_llm_output({})
        status = "api_error"

    record = {
        **base,
        "aaer_num":                aaer_num,
        "pdf_name":                f"AAER-{aaer_num}.pdf",
        "enrich_status":           status,
        "filing_entity":           llm_data["filing_entity"],
        "ticker":                  llm_data["ticker"],
        "fiscal_periods_affected": llm_data["fiscal_periods_affected"],
        "primary_category":        llm_data["primary_category"],
        "secondary_categories":    llm_data["secondary_categories"],
        "specific_mechanism":      llm_data["specific_mechanism"],
        "dollar_impact":           llm_data["dollar_impact"],
        "respondent_type":         llm_data["respondent_type"],
        "traceable_to_financials": llm_data["traceable_to_financials"],
        "is_new_enforcement":      llm_data["is_new_enforcement"],
        "legal_signals":           llm_data["legal_signals"],
        "llm_confidence":          llm_data["llm_confidence"],
        "llm": {
            "model":     model,
            "reasoning": llm_data["reasoning"],
        },
    }
    return record


def parse_args():
    parser = argparse.ArgumentParser(description="Enrich AAER records with LLM-extracted metadata")
    parser.add_argument("--max-cases", type=int, default=None)
    parser.add_argument("--resume", action="store_true",
                        help="Skip records already present in output file")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--aaer-num", type=int, default=None,
                        help="Process a single AAER number (for debugging)")
    return parser.parse_args()


def main():
    args = parse_args()
    OUTPUT_DIR.mkdir(exist_ok=True)

    client     = anthropic.Anthropic()
    base_index = load_base_dataset()

    existing = load_existing_output() if args.resume else {}
    if existing:
        log.info(f"Resuming: {len(existing)} records already processed")

    # Collect text files to process
    if args.aaer_num:
        text_files = [TEXTS_DIR / f"AAER-{args.aaer_num}.txt"]
        text_files = [p for p in text_files if p.exists()]
    else:
        text_files = sorted(TEXTS_DIR.glob("AAER-*.txt"))

    if args.max_cases:
        text_files = text_files[: args.max_cases]

    log.info(f"Processing {len(text_files)} AAER text files with {args.model}")

    results = dict(existing)
    ok = errors = skipped = 0

    for i, text_path in enumerate(text_files, 1):
        m = re.search(r"AAER-(\d+)", text_path.stem)
        if not m:
            continue
        aaer_num = int(m.group(1))

        if args.resume and aaer_num in existing:
            skipped += 1
            continue

        base = base_index.get(aaer_num, {"aaer_num": aaer_num, "status": "ok"})
        log.info(f"[{i}/{len(text_files)}] AAER-{aaer_num} — {base.get('respondent','')[:50]}")

        record = enrich_record(client, args.model, aaer_num, base, text_path)
        results[aaer_num] = record

        if record["enrich_status"] == "enriched":
            ok += 1
            log.info(
                f"  → {record['primary_category']} | conf={record['llm_confidence']:.2f} "
                f"| entity={record['filing_entity']} | periods={record['fiscal_periods_affected']}"
            )
        else:
            errors += 1

        # Checkpoint every 50 records
        if i % 50 == 0:
            _save(results)
            log.info(f"  ── Checkpoint: ok={ok} errors={errors} skipped={skipped}")

        time.sleep(DELAY)

    _save(results)
    log.info(f"\nDone. ok={ok} errors={errors} skipped={skipped}")
    log.info(f"Output: {OUTPUT_FILE.resolve()}")


def _save(results: dict):
    OUTPUT_FILE.write_text(
        json.dumps(list(results.values()), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
