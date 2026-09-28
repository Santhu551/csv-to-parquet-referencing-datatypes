# Standalone Archive Viewer CSV -> Parquet Converter

This project is a standalone extraction of the CSV -> Parquet conversion path from the supplied Archive Viewer source. It intentionally retains the original datatype/message-code concepts and the original Linux CLOB/BLOB behavior, while removing MetaDB, external-table creation, UI, user management, and unrelated Archive Viewer workflows.

## Main entry point

```bash
python3 main.py --config config/settings.txt
```

Edit **only** `config/settings.txt` for normal operation:
- INPUT_PATH: a CSV file or a directory containing CSVs
- OUTPUT_PATH: destination directory
- DATATYPE_JSON: `datatypes_<id>.json`
- WITH_CLOB / WITH_BLOB
- DELETE_LOB_FILES / DELETE_CSV
- Spark memory/timeout settings
- Tika settings

## Background execution

For a terminal-independent run:

```bash
./scripts/run_background.sh
./scripts/status.sh
./scripts/stop.sh
```

For server/reboot resilience, use the included systemd example. `nohup` survives terminal disconnects; systemd is the appropriate option if the process must be restarted after a machine reboot or unexpected service failure.

## Tracking

`status/conversion_status.csv` records RUNNING/SUCCESS/FAILED for every CSV, including table, GUID, output path, row count and error. A successful input/output is skipped on a later run.

The converter writes each table to:

```text
OUTPUT_PATH/<table>.parquet/
```

It writes to `<table>.parquet.__inprogress__` first and renames it only after Spark has produced Parquet data. This prevents an interrupted job from being mistaken for a completed conversion.

## Large files / memory

The original conversion used `toLocalIterator()` to copy every LOB path into a Python list. That is a driver-memory growth point. This standalone version does **not** collect the complete CSV or all LOB paths on the driver. Spark performs the CSV transformation and Parquet write; LOB bytes are read per executor row as required by the original BLOB/CLOB behavior. Cleanup, when enabled, streams paths with `toLocalIterator()` instead of accumulating them.

No Base64/hex conversion is used for BLOBs: BLOB data is returned as raw bytes through Spark `BinaryType`.

## Important original mappings

`221 -> TABLE_INFO`, `130 -> CHAR`, `131 -> timestamp`, `132 -> date`, `164 -> NUMBER`, `166 -> DECIMAL`, `249 -> linux`, `250 -> windows`, `265 -> CLOB`, `266 -> BLOB`, `353 -> LOB_COLUMNS`, `356 -> TIMESTAMP WITH LOCAL TIME ZONE`, `359 -> Enterprise Content Management(CSV)`, `360 -> NATIVE_LOB_PRESENT`, `361 -> SQL_SERVER_DATABASE`.

The actual datatype lookup is therefore:

```python
data[GUID][messages["messagecode"]["221"]][table][column]
#                         -> "TABLE_INFO"
```

For the supplied `datatypes_36.json`, this is:

```python
data[GUID]["TABLE_INFO"]["invoices"]["COMMENTS"]
# -> "NVARCHAR"
```

## Dependencies

The supplied environment information identifies PySpark 3.5.1, chardet 5.2.0 and tika 2.6.0 for the relevant conversion path. Install with:

```bash
python3 -m pip install -r requirements.txt
```

Java is required by Spark and by an Apache Tika server when Tika is configured for auto-start.

## Scope intentionally excluded

No MetaDB writes, external-table creation, PostgreSQL FDW creation, query-server operations, UI, user management, AF conversion, ECM conversion, or unrelated process tracking are performed by this standalone converter.


### Native VARCHAR/NVARCHAR handling
For SQL Server native LOB exports, not every VARCHAR/NVARCHAR column is a CLOB sidecar. The converter now preserves ordinary text values and dereferences a value only when it points to an existing LOB file. Explicit CLOB/NCLOB/etc. datatypes continue to use the CLOB reader. This prevents ordinary string columns from becoming NULL.


### v12 LOB path resolution
LOB values are resolved from absolute paths, CSV-relative paths, paths containing the `ecm_output` prefix, and bare filenames in immediate sibling sidecar directories. Ordinary native VARCHAR/NVARCHAR values are preserved as text when they do not resolve to a real file. BLOB remains raw bytes (`BinaryType`); CLOB remains decoded text using chardet. The converter does not recursively index all LOB files or collect all CSV LOB paths on the driver.

### Schema-qualified CSV output layout

The converter preserves the normal configured `OUTPUT_PATH` for CSVs whose table filename has no schema prefix. If the table token is schema-qualified, for example `OPTIMSRC130.OPTIM_ORDERS2-...CSV`, the converter creates a schema-specific sibling directory using the same layout used by Archive Viewer:

```text
<base>/parquet/                              # unqualified CSVs
<base>_OPTIMSRC130/parquet/                  # OPTIMSRC130 CSVs
<base>_OPTIMSRC144/parquet/                  # OPTIMSRC144 CSVs
```

For example, with `OUTPUT_PATH=/home/postgres/yashwanth/mem_err_yash_created_af/parquet`:

```text
/home/postgres/yashwanth/mem_err_yash_created_af/parquet/OPTIM_PST_ACTIONS.parquet/
/home/postgres/yashwanth/mem_err_yash_created_af_OPTIMSRC130/parquet/TBLEST_OPTIMDETAILS.parquet/
/home/postgres/yashwanth/mem_err_yash_created_af_OPTIMSRC144/parquet/<table>.parquet/
```

The schema prefix is taken from the CSV table token before the first `.`; the table name used for `TABLE_INFO` lookup is the portion after the `.`. Unqualified filenames continue to use the original table-name extraction behavior.


## Customer-friendly run reports

Each run also creates a date-based application report next to the configured `parquet` directory. If `OUTPUT_PATH` is:

```text
/home/postgres/santhosh_bsc/script_par_loc/new5test/parquet
```

the report files are:

```text
/home/postgres/santhosh_bsc/script_par_loc/new5test/new5test_2026-09-25.json
/home/postgres/santhosh_bsc/script_par_loc/new5test/new5test_2026-09-25_error.log   # created only when a file fails
```

The JSON contains the run start/end time, overall status, input/output paths, total CSV count, processed/success/failed counts, total rows, and a per-CSV result. The error log contains the complete traceback for failed CSVs.

`./scripts/status.sh` is intentionally customer-friendly. It shows RUNNING or the last completed result, application name, run time, file counts, rows, and each CSV's SUCCESS/FAILED status instead of dumping the full CSV manifest. The detailed `status/conversion_status.csv` remains available for technical troubleshooting.

The background launcher removes its PID file when the run completes or is terminated, so a completed run is no longer reported as a stale running process.
