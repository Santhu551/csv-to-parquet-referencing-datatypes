#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/logs" "$ROOT/status"
PIDFILE="$ROOT/status/converter.pid"
STARTLOG="$ROOT/logs/background_launcher.log"

if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "ALREADY RUNNING"
  echo "PID       : $(cat "$PIDFILE")"
  "$ROOT/scripts/status.sh"
  exit 0
fi
rm -f "$PIDFILE"

nohup bash -c '
  trap '\''rm -f "$1"'\'' EXIT
  trap '\''rm -f "$1"; exit 143'\'' TERM INT
  python3 "$2/main.py" --config "$2/config/settings.txt"
' _ "$PIDFILE" "$ROOT" >> "$STARTLOG" 2>&1 &
PID=$!
echo "$PID" > "$PIDFILE"

echo "STARTED"
echo "PID       : $PID"
echo "STATUS    : $ROOT/scripts/status.sh"
echo "LOG       : $STARTLOG"
