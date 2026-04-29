"""
Select top-N AAER cases for downstream model-testing input.

Reads aaer_filtered_v2/aaer_dataset_v2.json and applies hard filters:
  - Valid PDF text (status == "ok")
  - New enforcement action (not reinstatement / dismissal)
  - Primary category is a balance-sheet / P&L manipulation (not OTHER or NARRATIVE alone)
  - Respondent type implies a named issuer with filings (issuer / mixed)
  - LLM-tagged traceable_to_financials == True
  - Has a filing_entity + at least one fiscal period
  - Has a specific_mechanism (non-empty)
  - Confidence >= CONF_THRESHOLD

Then ranks by (confidence, has_dollar_impact, period_specificity) and outputs top-N.

Outputs:
  aaer_filtered_v2/selected_cases.json   — structured records
  aaer_filtered_v2/selected_cases.csv    — flat table with EDGAR-ready fields
"""

import argparse
import csv
import json
from pathlib import Path

INPUT_JSON = Path("aaer_filtered_v2/aaer_dataset_v2.json")
OUT_JSON   = Path("aaer_filtered_v2/selected_cases.json")
OUT_CSV    = Path("aaer_filtered_v2/selected_cases.csv")

ACCOUNTING_CATS = {
    "REVENUE_TIMING",
    "EXPENSE_DEFERRAL",
    "ACCOUNTING_ESTIMATE",
    "EARNINGS_SMOOTHING",
    # NARRATIVE_DISTORTION is kept optional — see --include-narrative flag
}


def passes_hard_filter(r: dict, include_narrative: bool, conf_threshold: float) -> tuple[bool, str]:
    """Return (passes, reason_if_not)."""
    if r.get("status") != "ok":
        return False, f"status={r.get('status')}"
    if not r.get("is_new_enforcement"):
        return False, "not_new_enforcement"

    cats = set(ACCOUNTING_CATS)
    if include_narrative:
        cats.add("NARRATIVE_DISTORTION")
    if r.get("primary_category") not in cats:
        return False, f"category={r.get('primary_category')}"

    if r.get("respondent_type") not in ("issuer", "mixed"):
        return False, f"respondent_type={r.get('respondent_type')}"

    if not r.get("traceable_to_financials"):
        return False, "not_traceable"

    if not (r.get("filing_entity") or "").strip():
        return False, "no_filing_entity"

    periods = r.get("fiscal_periods_affected") or []
    if not periods:
        return False, "no_periods"

    mech = (r.get("specific_mechanism") or "").strip()
    if len(mech) < 30:
        return False, "no_mechanism"

    if (r.get("llm_confidence") or 0) < conf_threshold:
        return False, f"low_conf={r.get('llm_confidence')}"

    return True, ""


def score(r: dict) -> tuple:
    """Sort key — larger is better. Tuple sorted desc."""
    has_dollar = 1 if (r.get("dollar_impact") or "").strip() else 0
    n_periods  = len(r.get("fiscal_periods_affected") or [])
    mech_len   = len((r.get("specific_mechanism") or ""))
    return (
        r.get("llm_confidence") or 0,
        has_dollar,
        n_periods,
        mech_len,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=20)
    ap.add_argument("--conf", type=float, default=0.8)
    ap.add_argument("--include-narrative", action="store_true",
                    help="Include NARRATIVE_DISTORTION cases (pure disclosure cases)")
    args = ap.parse_args()

    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    print(f"Loaded {len(data)} records from {INPUT_JSON}")

    qualified = []
    rejections = {}
    for r in data:
        ok, reason = passes_hard_filter(r, args.include_narrative, args.conf)
        if ok:
            qualified.append(r)
        else:
            rejections[reason] = rejections.get(reason, 0) + 1

    print(f"\nRejection reasons:")
    for k, v in sorted(rejections.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<40} {v:>4}")

    qualified.sort(key=score, reverse=True)
    top = qualified[: args.top_n]
    print(f"\nQualified: {len(qualified)}    Returning top {len(top)}")

    # Select downstream-relevant fields
    slim = []
    for r in top:
        slim.append({
            "aaer_num":                r.get("aaer_num"),
            "aaer_respondent":         r.get("respondent"),
            "filing_entity":           r.get("filing_entity"),
            "primary_category":        r.get("primary_category"),
            "secondary_categories":    r.get("secondary_categories"),
            "specific_mechanism":      r.get("specific_mechanism"),
            "fiscal_periods_affected": r.get("fiscal_periods_affected"),
            "period_free_text":        r.get("period"),
            "dollar_impact":           r.get("dollar_impact"),
            "respondent_type":         r.get("respondent_type"),
            "llm_confidence":          r.get("llm_confidence"),
            "pdf_name":                r.get("pdf_name"),
            "release_no":              r.get("release_no"),
            "sec_date":                r.get("date"),
            "reasoning":               (r.get("llm") or {}).get("reasoning", ""),
            "legal_signals":           list((r.get("legal_signals") or {}).keys()),
        })

    OUT_JSON.write_text(json.dumps(slim, indent=2, ensure_ascii=False), encoding="utf-8")

    fields = [
        "aaer_num", "filing_entity", "aaer_respondent", "primary_category",
        "fiscal_periods_affected", "dollar_impact", "specific_mechanism",
        "respondent_type", "llm_confidence", "release_no", "sec_date", "pdf_name",
    ]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in slim:
            row = dict(r)
            row["fiscal_periods_affected"] = "|".join(r.get("fiscal_periods_affected") or [])
            w.writerow(row)

    print(f"\nSaved:")
    print(f"  {OUT_JSON}")
    print(f"  {OUT_CSV}")

    print(f"\n=== Top {len(top)} cases ===\n")
    for i, r in enumerate(slim, 1):
        periods = "|".join(r.get("fiscal_periods_affected") or [])
        print(f"{i:>2}. AAER-{r['aaer_num']}  {(r['filing_entity'] or '')[:35]:<35}  "
              f"{r['primary_category']:<22}  conf={r['llm_confidence']:.2f}")
        print(f"    periods: {periods}")
        print(f"    {(r['specific_mechanism'] or '')[:170]}")
        if r.get("dollar_impact"):
            print(f"    $: {r['dollar_impact'][:130]}")
        print()


if __name__ == "__main__":
    main()
