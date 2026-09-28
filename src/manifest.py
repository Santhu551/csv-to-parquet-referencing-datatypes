import csv
import json
from pathlib import Path
from datetime import datetime, timezone

FIELDS=["timestamp","status","input","table","guid","output","rows","parquet_file","error"]

def now(): return datetime.now(timezone.utc).isoformat()

def append_record(path, record):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    new=not path.exists()
    with path.open("a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=FIELDS)
        if new: w.writeheader()
        w.writerow({k:record.get(k,"") for k in FIELDS})
        f.flush()

def latest_by_input(path):
    path=Path(path); result={}
    if not path.exists(): return result
    with path.open(newline="",encoding="utf-8") as f:
        for r in csv.DictReader(f): result[r.get("input","")]=r
    return result
