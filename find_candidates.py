import csv

with open('aaer_filtered/aaer_dataset.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    candidates = []
    for row in reader:
        if row['status'] == 'ok' and row['categories'] and row['char_count'] and int(row['char_count']) > 5000:
            candidates.append(row)

candidates.sort(key=lambda x: int(x['char_count']), reverse=True)
for c in candidates[:30]:
    print(f"AAER-{c['aaer_num']} | {c['char_count']} chars | {c['categories']} | auditor={c['auditor']} | {c['respondent'][:60]} | {c['date']}")
