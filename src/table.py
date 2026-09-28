import json
import re
from pathlib import Path


def fetch_tablename_csv(file):
    # Exact table-name rule used by the original fetch_tablename_csv().
    stem = Path(file).stem
    return "-".join(stem.split("-")[:-6])


def fetch_schema_and_table(file):
    """
    Extract an optional schema prefix from the CSV table token.

    Examples:
      OPTIM_PST_ACTIONS-<guid>-<timestamp>.CSV
        -> (None, "OPTIM_PST_ACTIONS")
      OPTIMSRC130.OPTIM_ORDERS2-<guid>-<timestamp>.CSV
        -> ("OPTIMSRC130", "OPTIM_ORDERS2")

    The original fetch_tablename_csv() rule is retained as the base parser;
    this helper only separates a schema prefix when the filename contains one.
    """
    raw = fetch_tablename_csv(file)
    if "." in raw:
        schema, table = raw.split(".", 1)
        if schema and table:
            return schema, table
    return None, raw


def candidate_guids(data):
    return [k for k, v in data.items() if isinstance(k, str) and isinstance(v, dict)]


def _find_key_case_insensitive(mapping, wanted):
    if not isinstance(mapping, dict):
        return None
    if wanted in mapping:
        return wanted
    wanted_l = str(wanted).lower()
    for key in mapping:
        if str(key).lower() == wanted_l:
            return key
    return None


def resolve_guid_and_table(data, table_name, guid=None, table_info_key="TABLE_INFO", qualified_name=None, schema=None):
    """
    Resolve the table exactly as the datatype JSON represents it.

    Important: schema-qualified CSV filenames and datatype JSON do not have to
    use the same representation. For example, a CSV may be
    OPTIMSRC130.RPT_CONNECTION_DETAILS-<guid>-<time>.CSV while TABLE_INFO may
    contain either:
      * "OPTIMSRC130.RPT_CONNECTION_DETAILS"
      * "RPT_CONNECTION_DETAILS"
      * {"OPTIMSRC130": {"RPT_CONNECTION_DETAILS": {...}}}

    Try the qualified form first, then the unqualified form, then a nested
    schema representation. This keeps schema detection separate from metadata
    lookup and prevents v13 from stripping the schema before lookup.
    """
    candidates = candidate_guids(data)
    requested = []
    if qualified_name:
        requested.append(str(qualified_name))
    if table_name:
        requested.append(str(table_name))
    # Avoid duplicate case-insensitive candidates.
    seen=set(); requested=[x for x in requested if not (x.lower() in seen or seen.add(x.lower()))]

    def find_in_guid(obj):
        if not isinstance(obj, dict):
            return None
        table_map = obj.get(table_info_key)
        if not isinstance(table_map, dict):
            return None

        # 1. Direct key lookup. Prefer the exact qualified CSV token.
        for wanted in requested:
            key = _find_key_case_insensitive(table_map, wanted)
            if key is not None:
                return key

        # 2. Nested schema representation: TABLE_INFO[schema][table].
        if schema:
            schema_key = _find_key_case_insensitive(table_map, schema)
            if schema_key is not None and isinstance(table_map[schema_key], dict):
                nested = table_map[schema_key]
                for wanted in (table_name,):
                    key = _find_key_case_insensitive(nested, wanted)
                    if key is not None:
                        return (schema_key, key)

        # 3. Some exports may nest more than one qualifier level. Search the
        # first two dictionary levels for the exact table token, without
        # inventing or rewriting table names.
        wanted_l = {x.lower() for x in requested}
        table_l = str(table_name).lower()
        for k1, v1 in table_map.items():
            if not isinstance(v1, dict):
                continue
            for k2, v2 in v1.items():
                if str(k2).lower() in wanted_l or str(k2).lower() == table_l:
                    return (k1, k2)
        return None

    if guid:
        obj = data.get(guid)
        found = find_in_guid(obj)
        if found is None:
            available = list(obj.get(table_info_key, {}))[:30] if isinstance(obj, dict) and isinstance(obj.get(table_info_key), dict) else []
            raise KeyError(f"Table '{qualified_name or table_name}' not found under GUID '{guid}'. Available: {available}")
        return guid, found

    matches=[]
    for g in candidates:
        found=find_in_guid(data[g])
        if found is not None:
            matches.append((g, found))
    if len(matches)==1:
        return matches[0]
    if len(matches)>1:
        raise KeyError(f"Multiple GUIDs contain table '{qualified_name or table_name}': {[g for g,_ in matches]}. Use GUID explicitly.")
    raise KeyError(f"Could not resolve table '{qualified_name or table_name}' under '{table_info_key}'. Candidates: {candidates}")


def column_types(data, guid, table_name, table_info_key="TABLE_INFO"):
    """Return the actual table metadata, including nested schema keys."""
    table_map = data[guid][table_info_key]
    if isinstance(table_name, tuple):
        obj = table_map
        for key in table_name:
            obj = obj[key]
        return obj
    return table_map[table_name]


def load_datatypes(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))



def normalize_column_lookup(mapping, column):
    if column in mapping:
        return mapping[column]
    # Spark CSV column names can differ only by case in some inputs; retain exact lookup first.
    lower = {str(k).lower(): v for k,v in mapping.items()}
    return lower.get(str(column).lower())
