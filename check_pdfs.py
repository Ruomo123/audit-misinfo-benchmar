"""Check all downloaded PDFs for corruption / unreadability."""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from pathlib import Path
import pdfplumber

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
            status = "empty_text"   # likely scanned image
            bad.append({"file": pdf_path.name, "size_kb": size_kb, "chars": char_count, "status": status})
        else:
            good.append(pdf_path.name)
    except Exception as e:
        status = f"corrupt: {e}"
        bad.append({"file": pdf_path.name, "size_kb": size_kb, "chars": 0, "status": status})

print(f"Readable:      {len(good)}")
print(f"Problematic:   {len(bad)}\n")

if bad:
    print("Problematic files:")
    for b in bad:
        print(f"  {b['file']:20s}  {b['size_kb']:>5} KB  chars={b['chars']}  [{b['status']}]")

Path("aaer_data/bad_pdfs.json").write_text(
    json.dumps(bad, indent=2), encoding="utf-8"
)
print(f"\nSaved list to aaer_data/bad_pdfs.json")
