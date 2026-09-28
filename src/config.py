from pathlib import Path

def load_settings(path):
    values = {}
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        values[k.strip()] = v.strip()
    return values

def as_bool(values, key, default=False):
    return values.get(key, str(default)).strip().lower() in {"true", "1", "yes", "y"}
