import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path


def application_name(output_path):
    p = Path(output_path).resolve()
    # OUTPUT_PATH is normally <application>/parquet.
    if p.name.lower() == "parquet":
        return p.parent.name
    return p.name


def report_paths(output_path, when=None):
    when = when or datetime.now()
    app = application_name(output_path)
    base = Path(output_path).resolve().parent if Path(output_path).resolve().name.lower() == "parquet" else Path(output_path).resolve()
    date = when.strftime("%Y-%m-%d")
    return (
        app,
        base / f"{app}_{date}.json",
        base / f"{app}_{date}_error.log",
    )


def _read(path):
    if not path.exists():
        return {"application": application_name(path.parent / "parquet"), "runs": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"application": application_name(path.parent / "parquet"), "runs": []}


def start_report(output_path, input_path, file_count):
    now = datetime.now(timezone.utc)
    app, path, error_path = report_paths(output_path, now)
    data = _read(path)
    data.setdefault("application", app)
    data.setdefault("runs", [])
    run_id = now.strftime("%Y%m%dT%H%M%S%fZ")
    run = {
        "run_id": run_id,
        "start_time": now.isoformat(),
        "end_time": None,
        "status": "RUNNING",
        "input_path": str(Path(input_path).resolve()),
        "output_path": str(Path(output_path).resolve()),
        "csv_files": file_count,
        "processed": 0,
        "success": 0,
        "failed": 0,
        "rows": 0,
        "files": [],
    }
    data["last_run_id"] = run_id
    data["last_updated"] = now.isoformat()
    data["runs"].append(run)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return {"app": app, "path": path, "error_path": error_path, "run_id": run_id}


def update_report(report, **updates):
    path = Path(report["path"])
    data = _read(path)
    for run in reversed(data.get("runs", [])):
        if run.get("run_id") == report["run_id"]:
            run.update(updates)
            data["last_updated"] = datetime.now(timezone.utc).isoformat()
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            return


def add_file_result(report, result):
    path = Path(report["path"])
    data = _read(path)
    for run in reversed(data.get("runs", [])):
        if run.get("run_id") == report["run_id"]:
            run["files"].append(result)
            run["processed"] = len(run["files"])
            run["success"] = sum(1 for x in run["files"] if x.get("status") == "SUCCESS")
            run["failed"] = sum(1 for x in run["files"] if x.get("status") == "FAILED")
            run["rows"] = sum(int(x.get("rows") or 0) for x in run["files"] if x.get("status") == "SUCCESS")
            data["last_updated"] = datetime.now(timezone.utc).isoformat()
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            return


def finish_report(report, status=None):
    path = Path(report["path"])
    data = _read(path)
    for run in reversed(data.get("runs", [])):
        if run.get("run_id") == report["run_id"]:
            run["end_time"] = datetime.now(timezone.utc).isoformat()
            if status is None:
                status = "FAILED" if run.get("failed", 0) else "SUCCESS"
            run["status"] = status
            data["last_updated"] = run["end_time"]
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            return


def append_error(report, csv_path, error_text):
    path = Path(report["error_path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write("=" * 100 + "\n")
        f.write(f"TIME    : {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"CSV     : {csv_path}\n")
        f.write("ERROR:\n")
        f.write(error_text.rstrip() + "\n")
