"""
Clear management_attribution entries that were mistakenly pulled from the restated
filing (contain 'restatement'/'restated') so --patch-task1 can re-extract them.

Run before: python build_benchmark.py --patch-task1
"""
import json
from pathlib import Path

path = Path("benchmark_data/cases.json")
data = json.loads(path.read_text(encoding="utf-8"))

cleared = 0
for r in data:
    t1 = (r.get("tasks") or {}).get("task1_profit_source") or {}
    inp = t1.get("input") or {}
    attr = inp.get("management_attribution") or ""
    if any(w in attr.lower() for w in ("restatement", "restated")):
        inp["management_attribution"] = ""
        cleared += 1
        print(f"  Cleared: {r['case_id']}")

path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"\nCleared {cleared} attribution(s). Run: python build_benchmark.py --patch-task1")
