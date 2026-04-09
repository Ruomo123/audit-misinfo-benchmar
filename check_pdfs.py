"""
Check all downloaded PDFs for corruption / unreadability, then remove bad ones.

Usage:
  python check_pdfs.py           # check only, don't delete
  python check_pdfs.py --delete  # check and delete bad PDFs (+ their JSON sidecars)
"""
import sys, io, json, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from pathlib import Path
import pdfplumber

parser = argparse.ArgumentParser()
parser.add_argument("--delete", action="store_true", help="Delete bad PDFs after checking")
args = parser.parse_args()

aaer_dir = Path("aaer_data")
pdfs = sorted(aaer_dir.glob("AAER-*.pdf"))
print(f"Total PDFs: {len(pdfs)}\n")

bad = []
good = []

for pdf_path in pdfs:
    size_kb = pdf_path.stat().st_size // 1024
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = "".join(p.extract_text() or "" for p in pdf.pages)
        char_count = len(text.strip())
        if char_count < 100:
            bad.append({"file": pdf_path.name, "size_kb": size_kb, "chars": char_count, "status": "empty_text"})
        else:
            good.append(pdf_path.name)
    except Exception as e:
        bad.append({"file": pdf_path.name, "size_kb": size_kb, "chars": 0, "status": f"corrupt: {e}"})

print(f"Readable:    {len(good)}")
print(f"Problematic: {len(bad)}\n")

if bad:
    print("Problematic files:")
    for b in bad:
        print(f"  {b['file']:20s}  {b['size_kb']:>5} KB  chars={b['chars']}  [{b['status']}]")

    (aaer_dir / "bad_pdfs.json").write_text(json.dumps(bad, indent=2), encoding="utf-8")
    print(f"\nSaved list to aaer_data/bad_pdfs.json")

    if args.delete:
        print("\nDeleting bad PDFs and their sidecars...")
        deleted = 0
        for b in bad:
            pdf_path = aaer_dir / b["file"]
            json_path = pdf_path.with_suffix(".json")
            if pdf_path.exists():
                pdf_path.unlink()
                deleted += 1
                print(f"  Deleted {b['file']}")
            if json_path.exists():
                json_path.unlink()
                print(f"  Deleted {json_path.name}")
        print(f"\nRemoved {deleted} bad PDFs.")
    else:
        print("\nRun with --delete to remove them.")
else:
    print("All PDFs look good!")
