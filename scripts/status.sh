#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$ROOT/status/converter.pid"
RUNINFO="$ROOT/status/current_run.json"

echo
echo "============================================================"
echo "             CSV -> PARQUET STATUS"
echo "============================================================"
echo

# ------------------------------------------------------------
# Process status
# ------------------------------------------------------------

PROCESS_STATUS="NOT RUNNING"
PID=""

if [[ -f "$PIDFILE" ]]; then
    PID="$(cat "$PIDFILE")"

    if kill -0 "$PID" 2>/dev/null; then
        PROCESS_STATUS="RUNNING"
    else
        rm -f "$PIDFILE"
    fi
fi

echo "PROCESS STATUS : $PROCESS_STATUS"

if [[ -n "$PID" ]]; then
    echo "PID            : $PID"
fi

echo

# ------------------------------------------------------------
# Current run information
# ------------------------------------------------------------

if [[ ! -f "$RUNINFO" ]]; then
    echo "No conversion run information found."
    echo
    exit 0
fi

python3 - "$RUNINFO" "$PROCESS_STATUS" <<'PY'
import json
import sys
from pathlib import Path

runinfo = Path(sys.argv[1])
process_status = sys.argv[2]

try:
    data = json.loads(runinfo.read_text(encoding="utf-8"))
except Exception as exc:
    print(f"ERROR: Unable to read run status: {exc}")
    sys.exit(1)

status = data.get("status", "UNKNOWN")

if status == "RUNNING":
    display_status = "RUNNING"
elif status == "SUCCESS":
    display_status = "SUCCESS"
elif status == "PARTIAL_SUCCESS":
    display_status = "PARTIAL SUCCESS"
elif status == "FAILED":
    display_status = "FAILED"
else:
    display_status = status

# ------------------------------------------------------------
# Run information
# ------------------------------------------------------------

print("------------------------------------------------------------")
print("CONVERSION INFORMATION")
print("------------------------------------------------------------")

print(f"STATUS       : {display_status}")
print(f"STARTED      : {data.get('start_time', '-')}")
print(f"COMPLETED    : {data.get('end_time') or '-'}")
print()

print(f"INPUT        : {data.get('input_path', '-')}")
print(f"OUTPUT       : {data.get('output_path', '-')}")
print(f"DATATYPE JSON: {data.get('datatype_json', '-')}")
print()

# ------------------------------------------------------------
# Progress
# ------------------------------------------------------------

print("------------------------------------------------------------")
print("CONVERSION PROGRESS")
print("------------------------------------------------------------")

total = data.get("csv_files", 0)
processed = data.get("processed", 0)
success = data.get("success", 0)
failed = data.get("failed", 0)
rows = data.get("rows", 0)

print(f"TOTAL FILES  : {total}")
print(f"PROCESSED    : {processed}")
print(f"SUCCESS      : {success}")
print(f"FAILED       : {failed}")
print(f"TOTAL ROWS   : {rows}")

# Show percentage while running
if isinstance(total, int) and total > 0:
    percentage = (processed / total) * 100
    print(f"PROGRESS     : {percentage:.2f}%")

# ------------------------------------------------------------
# Reports
# ------------------------------------------------------------

print()
print("------------------------------------------------------------")
print("REPORTS")
print("------------------------------------------------------------")

print(f"ALL FILE DETAILS : {data.get('report_path', '-')}")
print(f"ERROR LOG        : {data.get('error_log_path', '-')}")
print(f"DETAILED LOG     : {data.get('log_path', '-')}")

print()
print("The detailed report contains the result of every CSV file.")
print("The error log contains only failed-file error details.")
print()

PY