#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$ROOT/status/converter.pid"

if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "============================================================"
  echo "CSV -> PARQUET STATUS"
  echo "============================================================"
  echo "STATUS    : RUNNING"
  echo "PID       : $(cat "$PIDFILE")"
else
  rm -f "$PIDFILE"
  echo "============================================================"
  echo "CSV -> PARQUET STATUS"
  echo "============================================================"
  echo "STATUS    : NOT RUNNING"
fi

python3 - "$ROOT" <<'PY'
import json, sys
from pathlib import Path
root=Path(sys.argv[1])
settings=root/'config'/'settings.txt'
out=None
for line in settings.read_text(encoding='utf-8').splitlines():
    if line.startswith('OUTPUT_PATH='):
        out=Path(line.split('=',1)[1].strip())
        break
if out is None:
    raise SystemExit
out=out.resolve()
base=out.parent if out.name.lower()=='parquet' else out
app=base.name
reports=sorted(base.glob(f'{app}_????-??-??.json'), reverse=True)
if not reports:
    print('APPLICATION: -')
    print('REPORT     : -')
    raise SystemExit
report=reports[0]
try:
    data=json.loads(report.read_text(encoding='utf-8'))
except Exception as e:
    print(f'REPORT ERROR: {e}')
    raise SystemExit
runs=data.get('runs',[])
run=runs[-1] if runs else {}
print(f'APPLICATION: {app}')
print(f'RUN DATE   : {run.get("start_time", "-")}')
print(f'RUN STATUS : {run.get("status", "-")}')
print(f'INPUT      : {run.get("input_path", "-")}')
print(f'OUTPUT     : {run.get("output_path", "-")}')
print(f'TOTAL FILES: {run.get("csv_files", 0)}')
print(f'PROCESSED  : {run.get("processed", 0)}')
print(f'SUCCESS    : {run.get("success", 0)}')
print(f'FAILED     : {run.get("failed", 0)}')
print(f'TOTAL ROWS : {run.get("rows", 0)}')
print(f'REPORT     : {report}')
err=base/f'{app}_{report.stem.rsplit("_",1)[-1]}_error.log'
if err.exists():
    print(f'ERROR LOG  : {err}')
print('')
print('FILES')
print('------------------------------------------------------------')
for item in run.get('files',[]):
    status=item.get('status','')
    print(f'{status:<8} {item.get("table",""):<40} rows={item.get("rows",0)}')
    if item.get('output'): print(f'         output: {item["output"]}')
    if item.get('error'): print(f'         error : {item["error"]}')
PY
