#!/bin/bash
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$ROOT/status/converter.pid"
if [[ -f "$PIDFILE" ]]; then
  PID=$(cat "$PIDFILE")
  if kill -0 "$PID" 2>/dev/null; then kill "$PID"; echo "Stopped $PID"; else echo "PID not running"; fi
  rm -f "$PIDFILE"
else echo "No PID file"; fi
