# Standalone Archive Viewer CSV -> Parquet Converter

1. Customer Execution Guide
1.1 Prerequisites
Before starting the conversion, ensure the Linux server has:
- Python 3.12
- Java
- Required Python packages
- CSV files generated from Archive Viewer
- Required datatypes.json
Install the Python dependencies:
python3.12 -m pip install -r requirements.txt

2. Input Files
The customer should have the CSV files and corresponding datatype JSON available on the server.
Example:
/home/postgres/input/
├── ecm_output/
│   └── 36/
│       ├── invoices-F54FD320-AD46-11EF-AEE6-005056A9AB1E-220926024328.CSV
│       └── invoice_archivefile_linux_mssqlserver_15_36/
│           └── invoices_COMMENTS_0.LOB
│
└── datatypes_36.json

The CSV can be a single file or the input can contain multiple CSV files.

3. Run the Conversion
Recommended method
Run:
python3.12 main.py <INPUT_PATH> <OUTPUT_PATH> <DATATYPE_JSON>

Example:
python3.12 main.py \
/home/postgres/input/ecm_output/36 \
/home/postgres/output/parquet \
/home/postgres/input/datatypes_36.json

Parameters
Parameter	Description
INPUT_PATH	CSV file or directory containing CSV files
OUTPUT_PATH	Location where Parquet output will be created
DATATYPE_JSON	Path to the corresponding datatype JSON


Note: The output directory does not need to be created manually. The converter creates it automatically if it does not exist.

4. Run in Background
For large conversions, use the background execution script so that the conversion continues even after the terminal session is disconnected.
./scripts/run_background.sh

Check the conversion status:
./scripts/status.sh

Stop a running conversion:
./scripts/stop.sh

5. Check Conversion Status
Run:
./scripts/status.sh

The status displays the overall conversion information, for example:
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

APPLICATION REPORT :
/home/postgres/output/application_2026-10-07.json

ERROR LOG :
/home/postgres/output/application_2026-10-07_error.log

DETAILED LOG :
/home/postgres/output/logs/application-2026-10-07.log

The customer does not need to inspect thousands of individual entries from the terminal.
Detailed file-level information is stored in:
status/conversion_status.csv

6. Check the Output
For each successfully converted CSV, a corresponding Parquet directory is created.
Example:
/home/postgres/output/parquet/
└── invoices.parquet/

For schema-qualified tables, the converter follows the schema-specific output structure.
Example:
/home/postgres/output_OPTIMSRC144/parquet/
└── OPTIM_ORDERS.parquet/

7. If Some Files Fail
If the status shows:
FAILED : 10

check the generated error log:
<application>_YYYY-MM-DD_error.log

The source CSV is retained when conversion fails, so the failed file can be investigated and processed again.
The detailed file-level status is available in:
status/conversion_status.csv

8. Running the Same Input Again
The converter keeps track of successfully converted files.
If a CSV was already successfully converted and its Parquet output still exists, it is skipped on the next run.
Therefore, the customer does not need to manually remove successful entries before running the conversion again.
OVERWRITE=true is used when a file actually reaches the conversion stage and an existing Parquet output needs to be replaced. It does not by itself force previously successful files to be reconverted.

9. Important Output Locations
After a conversion, the customer mainly needs to check these locations:
Parquet output
    ↓
<OUTPUT_PATH>/

Application report
    ↓
<application>_YYYY-MM-DD.json

Error report
    ↓
<application>_YYYY-MM-DD_error.log

Detailed conversion status
    ↓
status/conversion_status.csv

Detailed application log
    ↓
logs/<application>-YYYY-MM-DD.log

10. Configuration File
For advanced configuration, the customer can use:
config/settings.txt

This contains options such as:
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

For normal execution, the recommended method is the direct:
python3.12 main.py <INPUT_PATH> <OUTPUT_PATH> <DATATYPE_JSON>

11. Conversion Options
CLOB/BLOB
The customer can control whether CLOB/BLOB processing is enabled through the configuration.
Delete LOB files
DELETE_LOB_FILES=true

removes successfully processed LOB files.
Delete source CSV
DELETE_CSV=true

removes the source CSV only after successful Parquet generation.
By default, source files should be retained unless cleanup is explicitly enabled.

12. Troubleshooting
Check whether the process is running
./scripts/status.sh

Stop the process
./scripts/stop.sh

Check failed files
status/conversion_status.csv

Check detailed error
<application>_YYYY-MM-DD_error.log

Check detailed execution log
logs/

13. Technical Details
This section should come after all customer execution steps, not before them.
Then we can keep the existing sections you already have, but make them much shorter:
- Datatype handling
- BLOB/CLOB handling
- Memory handling
- Schema-qualified tables
- Parquet/ZSTD
- Archive Viewer mappings
- Scope intentionally excluded
- Dependencies

