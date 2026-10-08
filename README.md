# Standalone Archive Viewer CSV → Parquet Converter

A standalone Linux utility for converting CSV files generated from Archive Viewer into Parquet format.

The converter supports single or multiple CSV files, datatype mapping using `datatypes.json`, BLOB/CLOB processing, schema-qualified tables, ZSTD compression, conversion tracking, background execution, and detailed logging.

---

# 1. Customer Execution Guide

## 1.1 Prerequisites

Before starting the conversion, ensure the Linux server has:

- Python 3.12
- Java
- Required Python packages
- CSV files generated from Archive Viewer
- Corresponding `datatypes.json`
- Tika server when Tika-based processing is required

Install the Python dependencies:

```bash
python3.12 -m pip install -r requirements.txt
```

---

# 2. Input Files

The customer should have the CSV files and the corresponding datatype JSON available on the server.

Example:

```text
/home/postgres/input/
├── ecm_output/
│   └── 36/
│       ├── invoices-F54FD320-AD46-11EF-AEE6-005056A9AB1E-220926024328.CSV
│       └── invoice_archivefile_linux_mssqlserver_15_36/
│           └── invoices_COMMENTS_0.LOB
│
└── datatypes_36.json
```

The input can contain:

- A single CSV file
- Multiple CSV files
- CSV files with corresponding `.LOB` files

The converter automatically discovers CSV files when a directory is provided as the input.

---

# 3. Run the Conversion

## 3.1 Recommended Method

Run:

```bash
python3.12 main.py <INPUT_PATH> <OUTPUT_PATH> <DATATYPE_JSON> <TIKA_HOST>
```

Example:

```bash
python3.12 main.py \
/home/postgres/input/ecm_output/36 \
/home/postgres/output/parquet \
/home/postgres/input/datatypes_36.json \
http://svcoptim6035:9998
```

### Parameters

| Parameter | Description |
|---|---|
| `INPUT_PATH` | CSV file or directory containing CSV files |
| `OUTPUT_PATH` | Location where Parquet output will be created |
| `DATATYPE_JSON` | Path to the corresponding datatype JSON |
| `TIKA_HOST` | Tika server URL |

> **Note:** The output directory does not need to be created manually. The converter creates it automatically if it does not exist.

---

# 4. Run in Background

For large conversions, use the background execution script.

This allows the conversion to continue even if the terminal or SSH session is disconnected.

Run:

```bash
./scripts/run_background.sh \
<INPUT_PATH> \
<OUTPUT_PATH> \
<DATATYPE_JSON> \
<TIKA_HOST>
```

Example:

```bash
./scripts/run_background.sh \
/home/postgres/input/ecm_output/36 \
/home/postgres/output/parquet \
/home/postgres/input/datatypes_36.json \
http://svcoptim6035:9998
```

The script displays the process ID and log location.

Example:

```text
============================================================
             CSV -> PARQUET BACKGROUND RUN
============================================================

INPUT      : /home/postgres/input/ecm_output/36
OUTPUT     : /home/postgres/output/parquet
DATATYPES  : /home/postgres/input/datatypes_36.json
TIKA HOST  : http://svcoptim6035:9998
LOG        : /home/postgres/project/logs/conversion_2026-10-08_12-30-10.log

============================================================
Conversion started in background.
============================================================

PID        : 123456
Log        : /home/postgres/project/logs/conversion_2026-10-08_12-30-10.log

The conversion will continue even if this terminal/SSH session is disconnected.
```

---

# 5. Check Conversion Status

Run:

```bash
./scripts/status.sh
```

Example status output:

```text
============================================================
             CSV -> PARQUET STATUS
============================================================

STATUS       : PARTIAL SUCCESS
STARTED      : 2026-10-07 08:20:10
COMPLETED    : 2026-10-07 08:42:35

INPUT        : /home/postgres/input
OUTPUT       : /home/postgres/output

TOTAL FILES  : 5000
PROCESSED    : 4980
SUCCESS      : 4970
SKIPPED      : 20
FAILED       : 10
TOTAL ROWS   : 45231000

------------------------------------------------------------
REPORTS
------------------------------------------------------------

APPLICATION REPORT : /home/postgres/output/application_2026-10-07.json
ERROR LOG          : /home/postgres/output/application_2026-10-07_error.log
DETAILED LOG       : /home/postgres/output/logs/application_2026-10-07.log
```

### Status meanings

| Status | Meaning |
|---|---|
| `SUCCESS` | File was converted successfully |
| `FAILED` | Conversion failed |
| `SKIPPED` | File was already successfully converted and its output still exists |
| `PARTIAL SUCCESS` | Some files succeeded and some failed |
| `STARTED` | Conversion is currently running |

Detailed file-level information is stored in:

```text
status/conversion_status.csv
```

The customer does not need to inspect thousands of individual entries from the terminal.

---

# 6. Stop a Running Conversion

To stop the current conversion:

```bash
./scripts/stop.sh
```

---

# 7. Check the Output

For each successfully converted CSV, a corresponding Parquet directory is created.

Example:

```text
/home/postgres/output/parquet/
└── invoices.parquet/
    ├── part-00000-....parquet
    └── _SUCCESS
```

For schema-qualified tables, the converter follows the schema-specific output structure.

Example:

```text
/home/postgres/output_OPTIMSRC144/
└── parquet/
    └── OPTIM_ORDERS.parquet/
        ├── part-00000-....parquet
        └── _SUCCESS
```

---

# 8. If Some Files Fail

If the status shows:

```text
FAILED : 10
```

check the generated error log:

```text
<OUTPUT_PATH>/<application>_<YYYY-MM-DD>_error.log
```

The detailed file-level status is available in:

```text
status/conversion_status.csv
```

The source CSV is retained when conversion fails.

This allows the failed file to be investigated and processed again.

---

# 9. Running the Same Input Again

The converter keeps track of successfully converted files.

If a CSV was already successfully converted and its Parquet output still exists, that CSV is skipped during the next run.

For example:

```text
TOTAL FILES  : 5000
PROCESSED    : 10
SUCCESS      : 8
SKIPPED      : 4989
FAILED       : 3
```

This prevents already successful files from being unnecessarily converted again.

## Important

`OVERWRITE=true` controls an existing Parquet output **when a file actually reaches the conversion stage**.

It does **not** automatically force previously successful files to be reconverted.

---

# 10. Important Output Locations

After a conversion, the customer mainly needs to check the following locations:

| Output | Location |
|---|---|
| Parquet output | `<OUTPUT_PATH>/` |
| Application report | `<OUTPUT_PATH>/<application>_<YYYY-MM-DD>.json` |
| Error report | `<OUTPUT_PATH>/<application>_<YYYY-MM-DD>_error.log` |
| Conversion status | `status/conversion_status.csv` |
| Detailed application log | `logs/<application>_<YYYY-MM-DD>.log` |

---

# 11. Configuration File

For advanced configuration, the customer can use:

```text
config/settings.txt
```

The configuration file contains options such as:

```text
INPUT_PATH
OUTPUT_PATH
DATATYPE_JSON
WITH_CLOB
WITH_BLOB
DELETE_LOB_FILES
DELETE_CSV
COMPRESSION
Spark settings
Tika settings
```

For normal execution, the recommended method is to provide the required values directly through the command line:

```bash
python3.12 main.py \
<INPUT_PATH> \
<OUTPUT_PATH> \
<DATATYPE_JSON> \
<TIKA_HOST>
```

The command-line Tika host is passed directly to the converter, so the customer does not need to edit `settings.txt` for the normal execution flow.

---

# 12. Conversion Options

## 12.1 CLOB / BLOB

The customer can control whether CLOB and BLOB processing is enabled through the configuration.

Relevant options:

```text
WITH_CLOB
WITH_BLOB
```

---

## 12.2 Delete LOB Files

To remove successfully processed LOB files:

```text
DELETE_LOB_FILES=true
```

LOB files are removed only after successful processing.

---

## 12.3 Delete Source CSV

To remove the source CSV after successful Parquet generation:

```text
DELETE_CSV=true
```

By default, source CSV files should be retained.

---

# 13. Troubleshooting

## Check whether the process is running

```bash
./scripts/status.sh
```

## Stop the process

```bash
./scripts/stop.sh
```

## Check failed files

```text
status/conversion_status.csv
```

## Check detailed errors

```text
<OUTPUT_PATH>/<application>_<YYYY-MM-DD>_error.log
```

## Check detailed execution logs

```text
logs/
```

---

# 14. Technical Details

The following sections describe the internal conversion behavior.

## 14.1 Datatype Handling

The converter reads datatype information from the provided `datatypes.json`.

The datatype information is used to determine the appropriate Spark datatype for each CSV column.

Examples include:

```text
INTEGER     -> integer
BIGINT      -> long
VARCHAR     -> string
NVARCHAR    -> string
VARBINARY   -> binary
DATETIME    -> timestamp
DATE        -> date
DOUBLE      -> double
BLOB        -> binary
CLOB        -> string
```

The Archive Viewer datatype mappings are used during conversion so that the resulting Parquet schema matches the expected datatype representation.

---

## 14.2 BLOB / CLOB Handling

BLOB and CLOB values may be represented in the CSV using references to external LOB files.

### CLOB

CLOB files are read as text and converted to Spark `StringType`.

### BLOB

BLOB files are read as raw bytes and converted to Spark `BinaryType`.

The converter does not convert BLOB content to Base64 or hexadecimal text.

---

## 14.3 Memory Handling

The converter is designed to process large CSV files without unnecessarily loading the complete dataset into Python driver memory.

LOB-related processing is performed in a way that reduces unnecessary accumulation of file paths and values in driver memory.

Spark continues to perform the main CSV processing and Parquet writing.

---

## 14.4 Schema-Qualified Tables

Some Archive Viewer CSV files contain a schema-qualified table name.

For example:

```text
OPTIMSRC144.OPTIM_ORDERS
```

The converter uses the appropriate table representation for datatype metadata lookup while maintaining the expected table name for the Parquet output.

Example output:

```text
OPTIM_ORDERS.parquet/
```

---

## 14.5 Parquet and ZSTD Compression

Parquet files are generated using ZSTD compression.

The configured compression value is:

```text
zstd
```

The converter writes the Parquet output only after successful processing.

---

## 14.6 Archive Viewer Mappings

The converter follows the datatype mappings used by the Archive Viewer conversion flow.

The mapping determines how source database datatypes are represented as Spark datatypes before Parquet generation.

Examples:

| Source datatype | Spark datatype |
|---|---|
| `INTEGER` | `integer` |
| `BIGINT` | `long` |
| `VARCHAR` | `string` |
| `NVARCHAR` | `string` |
| `VARBINARY` | `binary` |
| `BLOB` | `binary` |
| `CLOB` | `string` |
| `DATETIME` | `timestamp` |
| `DATE` | `date` |
| `DOUBLE` | `double` |

---

## 14.7 Scope Intentionally Excluded

This standalone converter is focused only on:

```text
CSV
 ↓
Datatype Mapping
 ↓
BLOB / CLOB Processing
 ↓
Spark DataFrame
 ↓
Parquet
```

The following Archive Viewer functionality is outside the scope of this standalone converter:

- Archive file to CSV conversion
- AF/ECM conversion
- MetaDB updates
- External table creation
- Query server management
- User management
- UI functionality
- Archive Viewer application workflows

---

## 14.8 Dependencies

The converter requires:

- Python 3.12
- Java
- PySpark
- Required Python packages listed in `requirements.txt`
- Archive Viewer generated CSV files
- Corresponding `datatypes.json`
- Tika server when required by the conversion

Install Python dependencies using:

```bash
python3.12 -m pip install -r requirements.txt
```

---

# 15. Quick Reference

### Direct conversion

```bash
python3.12 main.py \
<INPUT_PATH> \
<OUTPUT_PATH> \
<DATATYPE_JSON> \
<TIKA_HOST>
```

### Background conversion

```bash
./scripts/run_background.sh \
<INPUT_PATH> \
<OUTPUT_PATH> \
<DATATYPE_JSON> \
<TIKA_HOST>
```

### Check status

```bash
./scripts/status.sh
```

### Stop conversion

```bash
./scripts/stop.sh
```

### Detailed status

```text
status/conversion_status.csv
```

### Detailed logs

```text
logs/
```

---

# 16. Summary

The recommended customer workflow is:

```text
1. Prepare CSV + LOB files
          ↓
2. Prepare corresponding datatypes.json
          ↓
3. Ensure Tika is available when required
          ↓
4. Run conversion
          ↓
5. Monitor status
          ↓
6. Check Parquet output
          ↓
7. Investigate failed files using the error log
```

For large datasets, use the background execution script so that the conversion continues even if the SSH or terminal session is disconnected.