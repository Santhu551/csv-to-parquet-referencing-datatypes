#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PIDFILE="$ROOT/status/converter.pid"
LOGDIR="$ROOT/logs"
STATUSDIR="$ROOT/status"

INPUT_PATH="${1:-}"
OUTPUT_PATH="${2:-}"
DATATYPE_JSON="${3:-}"

echo
echo "============================================================"
echo "             CSV -> PARQUET BACKGROUND RUN"
echo "============================================================"
echo

if [[ -z "$INPUT_PATH" || -z "$OUTPUT_PATH" || -z "$DATATYPE_JSON" ]]; then
    echo "Usage:"
    echo
    echo "  ./scripts/background_run.sh <input_path> <output_path> <datatypes.json>"
    echo
    echo "Example:"
    echo
    echo "  ./scripts/background_run.sh \\"
    echo "    /home/postgres/input \\"
    echo "    /home/postgres/output \\"
    echo "    /home/postgres/input/datatypes_36.json"
    echo
    exit 1
fi


if [[ ! -e "$INPUT_PATH" ]]; then
    echo "ERROR: Input path does not exist:"
    echo "  $INPUT_PATH"
    exit 1
fi


if [[ ! -f "$DATATYPE_JSON" ]]; then
    echo "ERROR: Datatype JSON file does not exist:"
    echo "  $DATATYPE_JSON"
    exit 1
fi


mkdir -p "$OUTPUT_PATH"
mkdir -p "$LOGDIR"
mkdir -p "$STATUSDIR"


if [[ -f "$PIDFILE" ]]; then

    PID="$(cat "$PIDFILE")"

    if kill -0 "$PID" 2>/dev/null; then
        echo "ERROR: Conversion is already running."
        echo
        echo "PID:"
        echo "  $PID"
        echo
        echo "Check status using:"
        echo "  ./scripts/status.sh"
        echo
        exit 1
    else
        rm -f "$PIDFILE"
    fi

fi


LOGFILE="$LOGDIR/conversion_$(date '+%Y-%m-%d_%H-%M-%S').log"


echo "INPUT      : $INPUT_PATH"
echo "OUTPUT     : $OUTPUT_PATH"
echo "DATATYPES  : $DATATYPE_JSON"
echo "LOG        : $LOGFILE"
echo


nohup python3.12 "$ROOT/main.py" \
    "$INPUT_PATH" \
    "$OUTPUT_PATH" \
    "$DATATYPE_JSON" \
    > "$LOGFILE" 2>&1 &


PID=$!

echo "$PID" > "$PIDFILE"


echo "============================================================"
echo "Conversion started in background."
echo "============================================================"
echo
echo "PID        : $PID"
echo "Log        : $LOGFILE"
echo
echo "The conversion will continue even if this terminal/SSH session is disconnected."
echo 
echo
echo "Check status:"
echo
echo "  ./scripts/status.sh"
echo