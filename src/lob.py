from pathlib import Path
import subprocess
import time

import chardet


def _tika_settings(settings):
    return settings.get("TIKA_HOST", "localhost"), int(settings.get("TIKA_PORT", "9998")), settings.get("TIKA_JAR", "").strip()


def _ensure_tika(settings):
    host, port, jar = _tika_settings(settings)
    if not jar:
        return
    try:
        import socket
        with socket.create_connection((host, port), timeout=1):
            return
    except OSError:
        pass
    cmd=["java","-jar",jar,"--host",host,"--port",str(port)]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    deadline=time.time()+int(settings.get("TIKA_START_TIMEOUT","60"))
    while time.time()<deadline:
        try:
            import socket
            with socket.create_connection((host,port),timeout=1): return
        except OSError: time.sleep(1)


def mime_extension(path, spark_types, settings):
    try:
        from tika import detector
        try:
            _ensure_tika(settings)
            mime=detector.from_file(str(path), requestOptions={"timeout":60})
        except Exception:
            _ensure_tika(settings)
            mime=detector.from_file(str(path), requestOptions={"timeout":60})
        return spark_types.get("fileExtensions", {}).get(mime)
    except Exception:
        return None


def _candidate_paths(raw, base_dir):
    """Resolve Archive Viewer Linux LOB references without changing ordinary text.

    The original Linux reader accepts an absolute path directly. Real exports can
    also contain a relative path or a path that includes the ecm_output prefix.
    We try those equivalent filesystem representations before declaring the file
    missing. No directory-wide recursive scan is performed, so conversion does
    not build a driver-side list of every LOB path.
    """
    if raw is None:
        return []
    value=str(raw)
    if not value:
        return []
    normalized=value.replace("\\", "/")
    candidates=[]
    p=Path(normalized)
    if p.is_absolute():
        candidates.append(p)
    else:
        candidates.append(Path(base_dir)/p)
        parts=[x for x in normalized.split("/") if x]
        # If the CSV stores a path beginning at ecm_output, rebuild it from the
        # actual CSV's ecm_output directory.
        if "ecm_output" in parts:
            idx=parts.index("ecm_output")
            ecm_root=Path(base_dir)
            for parent in [Path(base_dir), *Path(base_dir).parents]:
                if parent.name == "ecm_output":
                    ecm_root=parent
                    break
            candidates.append(ecm_root.joinpath(*parts[idx+1:]))
        # Sidecar files are normally placed in a sibling directory of the CSV.
        # For a bare filename, check immediate child directories only.
        if len(parts)==1:
            try:
                for child in Path(base_dir).iterdir():
                    if child.is_dir():
                        candidates.append(child / parts[0])
            except OSError:
                pass
    seen=set()
    for candidate in candidates:
        key=str(candidate)
        if key not in seen:
            seen.add(key)
            yield candidate


def resolve_lob_path(raw, base_dir):
    for candidate in _candidate_paths(raw, base_dir):
        try:
            if candidate.exists() and candidate.is_file():
                return candidate
        except OSError:
            continue
    return None


def read_clob(path, base_dir=None):
    if not path:
        return None
    p=resolve_lob_path(path, base_dir) if base_dir else (Path(path) if Path(path).exists() else None)
    if p is None:
        return None
    data=p.read_bytes()
    enc=chardet.detect(data).get("encoding")
    return data.decode(enc, errors="replace") if enc else data.decode(errors="replace")


def read_native_text(path, base_dir=None):
    """Preserve native VARCHAR/NVARCHAR text unless the value resolves to a LOB file."""
    if not path:
        return None
    p=resolve_lob_path(path, base_dir) if base_dir else (Path(path) if Path(path).exists() else None)
    if p is None:
        return path
    data=p.read_bytes()
    enc=chardet.detect(data).get("encoding")
    return data.decode(enc, errors="replace") if enc else data.decode(errors="replace")


def read_blob(path, base_dir=None):
    if not path:
        return None
    p=resolve_lob_path(path, base_dir) if base_dir else (Path(path) if Path(path).exists() else None)
    if p is None:
        return None
    return p.read_bytes()


def lob_extension(path, spark_types, settings, base_dir=None):
    if not path:
        return None
    p=resolve_lob_path(path, base_dir) if base_dir else (Path(path) if Path(path).exists() else None)
    if p is None:
        return None
    return mime_extension(p, spark_types, settings)


def lob_filename(path, spark_types, settings, base_dir=None):
    if not path:
        return None
    p=resolve_lob_path(path, base_dir) if base_dir else (Path(path) if Path(path).exists() else None)
    if p is None:
        return None
    ext=lob_extension(path,spark_types,settings,base_dir)
    return p.stem if ext in (None, "none") else p.stem+ext
