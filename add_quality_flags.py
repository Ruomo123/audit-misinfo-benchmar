"""
Add quality_flags to each record in benchmark_data/cases.json and generate
benchmark_data/review_flags.csv for human reviewer triage.

No API calls — pure heuristic checks on the existing data.

Run after: python build_benchmark.py --patch-task1 && python build_benchmark.py --patch-task2
"""
import csv
import json
from pathlib import Path

INPUT_JSON  = Path("benchmark_data/cases.json")
OUT_CSV     = Path("benchmark_data/review_flags.csv")

# Placeholder patterns for task1 attribution
_ATTR_PLACEHOLDER = (
    "no explicit attribution",
    "not provided",
    "not available",
    "management claimed",
    "management stated",
    "no specific attribution",
    "original md&a section is not provided",
    "original md&a section is truncated",
)

# Placeholder patterns for task2 passage
_PASSAGE_PLACEHOLDER = (
    "not provided", "not available", "truncated",
    "no misleading passage", "fraud mechanism indicates",
    "original mda section is not provided",
    "original md&a section is truncated",
)


def _is_placeholder(text: str, patterns: tuple) -> bool:
    if not (text or "").strip():
        return True
    low = text.lower()
    return any(p in low for p in patterns)


def compute_flags(rec: dict) -> list[str]:
    flags = []
    tasks = rec.get("tasks") or {}

    # ── Task 1 ────────────────────────────────────────────────────────────────
    t1  = tasks.get("task1_profit_source") or {}
    inp = t1.get("input") or {}
    lbl = t1.get("label") or {}

    fin = inp.get("reported_financials") or {}
    if not fin or not any(v is not None for v in fin.values()):
        flags.append("task1_no_xbrl")

    attr = inp.get("management_attribution") or ""
    if _is_placeholder(attr, _ATTR_PLACEHOLDER) or len(attr.strip()) < 60:
        flags.append("task1_attribution_placeholder")
    elif any(w in attr.lower() for w in ("restatement", "restated")):
        flags.append("task1_attribution_from_restatement")

    # ── Task 2 ────────────────────────────────────────────────────────────────
    t2      = tasks.get("task2_narrative") or {}
    t2_inp  = t2.get("input") or {}
    t2_lbl  = t2.get("label") or {}

    rest_path = (rec.get("documents") or {}).get("restated", {}).get("local_path", "")
    has_restatement = bool(rest_path and Path(rest_path).exists())

    passage = t2_inp.get("passage") or ""
    if _is_placeholder(passage, _PASSAGE_PLACEHOLDER):
        if not has_restatement:
            flags.append("task2_no_restatement")   # expected; not a bug
        else:
            flags.append("task2_passage_placeholder")  # fixable

    gt_source = t2_lbl.get("ground_truth_source") or ""
    if gt_source and has_restatement:
        low_gt = gt_source.lower()
        if "not available" in low_gt or "no restatement" in low_gt:
            flags.append("task2_source_unrelated")
        elif "exhibit" in low_gt and len(gt_source.strip()) < 150:
            flags.append("task2_source_is_exhibit")

    return flags


def priority(flags: list[str]) -> str:
    if "task1_no_xbrl" in flags and "task1_attribution_placeholder" in flags:
        return "HIGH"
    low_only = flags == ["task2_no_restatement"]
    if low_only or not flags:
        return "LOW"
    return "MEDIUM"


def main():
    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))

    flag_counts: dict[str, int] = {}
    csv_rows = []

    for rec in data:
        flags = compute_flags(rec)
        rec["quality_flags"] = flags

        for f in flags:
            flag_counts[f] = flag_counts.get(f, 0) + 1

        tasks = rec.get("tasks") or {}
        t1 = tasks.get("task1_profit_source") or {}
        t2 = tasks.get("task2_narrative") or {}
        t3 = tasks.get("task3_pattern") or {}

        t1_inp = t1.get("input") or {}
        t2_inp = t2.get("input") or {}
        t3_lbl = t3.get("label") or {}

        fin = t1_inp.get("reported_financials") or {}
        has_fin = any(v is not None for v in fin.values())
        attr = (t1_inp.get("management_attribution") or "").strip()
        passage = (t2_inp.get("passage") or "").strip()
        category = (t3_lbl.get("fraud_category") or "").strip()

        csv_rows.append({
            "case_id":       rec["case_id"],
            "company":       rec.get("company", ""),
            "fiscal_period": rec.get("fiscal_period", ""),
            "fraud_category": rec.get("fraud_category", ""),
            "flags":         "|".join(flags) if flags else "clean",
            "priority":      priority(flags),
            "task1_ok":      "Y" if (has_fin and attr and not _is_placeholder(attr, _ATTR_PLACEHOLDER)) else "N",
            "task2_ok":      "Y" if (passage and not _is_placeholder(passage, _PASSAGE_PLACEHOLDER)) else "N",
            "task3_ok":      "Y" if category else "N",
            "extraction_notes": (rec.get("metadata") or {}).get("extraction_notes", "")[:200],
        })

    # Write updated cases.json with quality_flags
    INPUT_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    # Write review CSV
    fields = ["case_id", "company", "fiscal_period", "fraud_category",
              "flags", "priority", "task1_ok", "task2_ok", "task3_ok", "extraction_notes"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(csv_rows)

    print(f"Updated {len(data)} records with quality_flags -> {INPUT_JSON}")
    print(f"Review CSV -> {OUT_CSV}")
    print(f"\nFlag frequency:")
    for flag, count in sorted(flag_counts.items(), key=lambda x: -x[1]):
        print(f"  {flag:<40} {count:>4}  ({100*count/len(data):.0f}%)")

    priority_counts = {}
    for row in csv_rows:
        p = row["priority"]
        priority_counts[p] = priority_counts.get(p, 0) + 1
    print(f"\nPriority distribution:")
    for p in ("HIGH", "MEDIUM", "LOW"):
        print(f"  {p:<8} {priority_counts.get(p, 0):>4}")


if __name__ == "__main__":
    main()
