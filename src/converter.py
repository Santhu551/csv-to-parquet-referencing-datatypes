import json
import logging
import os
import re
import shutil
import traceback
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import rtrim, udf
from pyspark.sql.types import BinaryType, StringType

from .table import (
    fetch_tablename_csv,
    fetch_schema_and_table,
    resolve_guid_and_table,
    load_datatypes,
    column_types,
    normalize_column_lookup,
)
from .lob import (
    read_clob,
    read_native_text,
    read_blob,
    lob_extension,
    lob_filename,
    resolve_lob_path,
)
from .manifest import (
    append_record,
    latest_by_input,
    now,
)
from .run_report import (
    start_report,
    update_report,
    add_file_result,
    finish_report,
    append_error,
)
# ============================================================
# LOGGER
# ============================================================
def logger(log_dir, output_path=None):
    """
    Application logger.

    INFO messages:
        -> detailed log file only

    WARNING / ERROR messages:
        -> detailed log file
        -> customer terminal

    This keeps the terminal clean while preserving all
    developer information in the detailed log.
    """
    Path(log_dir).mkdir(
        parents=True,
        exist_ok=True,
    )
    log = logging.getLogger("csv_to_parquet")
    log.setLevel(logging.INFO)
    # Do not send messages to the root logger.
    log.propagate = False
    if not log.handlers:
        if (
            output_path
            and Path(output_path).resolve().name.lower() == "parquet"
        ):
            app = Path(output_path).resolve().parent.name

        elif output_path:
            app = Path(output_path).resolve().name

        else:
            app = "csv_to_parquet"

        date = __import__("datetime").datetime.now().strftime(
            "%Y-%m-%d"
        )

        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )
        # ----------------------------------------------------
        # Detailed log file
        # ----------------------------------------------------
        log_file = (
            Path(log_dir)
            / f"{app}_{date}.log"
        )
        file_handler = logging.FileHandler(
            log_file,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            formatter
        )
        log.addHandler(
            file_handler
        )
        # ----------------------------------------------------
        # Customer terminal
        #
        # Only WARNING and ERROR messages are displayed.
        # INFO messages remain in the log file.
        # ----------------------------------------------------
        console_handler = logging.StreamHandler()
        console_handler.setLevel(
            logging.WARNING
        )
        console_handler.setFormatter( formatter )
        log.addHandler( console_handler)

    return log
# ============================================================
# SPARK SESSION
# ============================================================
def spark_session(settings, base_dir):
    project_dir = str(
        Path(base_dir).resolve()
    )
    b = (
        SparkSession.builder
        .appName(
            "ArchiveViewer-CSV-to-Parquet-Standalone"))
    for k, v in {
        "spark.driver.memory": settings.get(
            "SPARK_DRIVER_MEMORY",
            "4g",
        ),
        "spark.executor.memory": settings.get(
            "SPARK_EXECUTOR_MEMORY",
            "4g",
        ),
        "spark.sql.shuffle.partitions": settings.get(
            "SPARK_SQL_SHUFFLE_PARTITIONS",
            "200",
        ),
        "spark.network.timeout": settings.get(
            "SPARK_NETWORK_TIMEOUT",
            "800s",
        ),
        "spark.executor.heartbeatInterval": settings.get(
            "SPARK_EXECUTOR_HEARTBEAT_INTERVAL",
            "60s",
        ),
        "spark.driver.extraPythonPath": project_dir,
        "spark.executorEnv.PYTHONPATH": project_dir,
    }.items():
        b = b.config(
            k,
            v,
        )
    spark = b.getOrCreate()
    # --------------------------------------------------------
    # Keep Spark console output quiet.
    # Detailed application information is already written
    # to the application log.
    # --------------------------------------------------------
    spark.sparkContext.setLogLevel(
        "ERROR"
    )
    return spark
# ============================================================
# CAST COLUMN
# ============================================================
def cast_column(
    df,
    col,
    source_type,
    spark_types,
):
    st = source_type
    if st is None:
        return df, "unmapped"

    if st == "NUMBER":
        return (
            df.withColumn(
                col,
                F.col(col).cast(
                    spark_types[
                        "sparkDatatypes"
                    ]["NUMBER"]
                ),
            ),
            "double",
        )
    if re.match(
        r"NUMBER\(\d+,\d+\)",
        st,
    ):
        return (
            df.withColumn(
                col,
                F.col(col).cast(
                    spark_types[
                        "sparkDatatypes"
                    ]["NUMBER"]
                ),
            ),
            "double",
        )
    if st.startswith("DECIMAL"):
        # Exact original branch:
        # DECIMAL and DECIMAL(p,s) are cast using
        # the configured DECIMAL mapping.
        target = spark_types[
            "sparkDatatypes"
        ]["DECIMAL"]
        return (
            df.withColumn(
                col,
                F.col(col).cast(target),
            ),
            target,
        )
    mapped = spark_types[
        "sparkDatatypes"
    ].get(st)
    if mapped is None:
        return df, "unmapped"
    if mapped == "timestamp":
        return (
            df.withColumn(
                col,
                F.to_timestamp(
                    F.col(col),
                    spark_types[
                        "datetimeformat"
                    ]["timestamp"],
                ),
            ),
            mapped,
        )
    if mapped == "date":
        return (
            df.withColumn(
                col,
                F.when(
                    F.col(col).rlike(
                        r"^\d{1,2}/\d{1,2}/\d{4} "
                        r"\d{1,2}:\d{2}:\d{1,2} "
                        r"(AM|PM)$"
                    ),
                    F.to_timestamp(
                        F.col(col),
                        "M/d/yyyy hh:mm:ss a",
                    ),
                )
                .when(
                    F.col(col).rlike(
                        r".*\d{2}:\d{2}:\d{2}.*"
                    ),
                    F.to_date(
                        F.col(col),
                        "M/d/yyyy hh:mm:ss a",
                    ),
                )
                .when(
                    F.col(col).rlike(
                        r"^\d{1,2}/\d{1,2}/\d{4}$"
                    ),
                    F.to_date(
                        F.col(col),
                        "M/d/yyyy",
                    ),
                )
                .otherwise(
                    F.col(col).cast("date")
                ),
            ),
            mapped,
        )

    return (
        df.withColumn(
            col,
            F.col(col).cast(mapped),
        ),
        mapped,
    )
# ============================================================
# PROCESS ONE CSV
# ============================================================
def process_csv(
    spark,
    csv_path,
    output_root,
    datatype_json,
    settings,
    spark_types,
    logger_obj,
    manifest_path,
):

    data = load_datatypes(
        datatype_json
    )

    schema, table = fetch_schema_and_table(
        csv_path
    )

    qualified_table = fetch_tablename_csv(
        csv_path
    )

    guid, metadata_table = resolve_guid_and_table(
        data,
        table,
        qualified_name=qualified_table,
        schema=schema,
        table_info_key="TABLE_INFO",
    )

    tinfo = column_types(
        data,
        guid,
        metadata_table,
    )

    native = bool(
        data[guid].get(
            "NATIVE_LOB_PRESENT",
            False,
        )
    )

    native_clob = bool(
        data[guid].get(
            "SQL_SERVER_DATABASE",
            False,
        )
    )

    lob_columns = []

    if isinstance(tinfo, dict):

        lob_columns = tinfo.get(
            "LOB_COLUMNS",
            [],
        )

    # --------------------------------------------------------
    # Detailed information goes to log file.
    # --------------------------------------------------------

    logger_obj.info(
        "CSV       : %s",
        csv_path,
    )

    logger_obj.info(
        "SCHEMA    : %s",
        schema
        if schema
        else "(default/no schema prefix)",
    )

    logger_obj.info(
        "TABLE     : %s",
        table,
    )

    logger_obj.info(
        "METADATA TABLE KEY: %s",
        metadata_table,
    )

    logger_obj.info(
        "GUID      : %s",
        guid,
    )

    logger_obj.info(
        "NATIVE    : %s",
        native,
    )

    logger_obj.info(
        "NATIVE CLOB/VARCHAR: %s",
        native_clob,
    )

    logger_obj.info(
        "LOB cols  : %s",
        lob_columns,
    )

    # --------------------------------------------------------
    # Read CSV
    # --------------------------------------------------------

    df = (
        spark.read
        .option(
            "escape",
            settings.get(
                "ESCAPE",
                "\\",
            ),
        )
        .option(
            "header",
            True,
        )
        .option(
            "nullValue",
            settings.get(
                "NULL_VALUE",
                "null",
            ),
        )
        .option(
            "quote",
            settings.get(
                "QUOTE",
                "₠",
            ),
        )
        .option(
            "delimiter",
            settings.get(
                "DELIMITER",
                ",",
            ),
        )
        .option(
            "multiLine",
            True,
        )
        .csv(
            str(csv_path),
            encoding=settings.get(
                "ENCODING",
                "utf-8",
            ),
        )
    )

    df = df.na.replace(
        "Φ",
        None,
    )

    with_clob = (
        settings.get(
            "WITH_CLOB",
            "true",
        ).lower()
        == "true"
    )

    with_blob = (
        settings.get(
            "WITH_BLOB",
            "true",
        ).lower()
        == "true"
    )

    cleanup_manifest = (
        Path(manifest_path)
        .with_suffix(".lobpaths")
    )

    cleanup_manifest.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if cleanup_manifest.exists():
        cleanup_manifest.unlink()

    cleanup_cols = []

    # --------------------------------------------------------
    # UDFs
    # --------------------------------------------------------

    def path_udf(
        func,
        return_type,
    ):
        return udf(
            func,
            return_type,
        )

    lob_base_dir = str(
        Path(csv_path).parent
    )

    clob_udf = path_udf(
        lambda p: read_clob(
            p,
            lob_base_dir,
        ),
        StringType(),
    )

    native_text_udf = path_udf(
        lambda p: read_native_text(
            p,
            lob_base_dir,
        ),
        StringType(),
    )

    blob_udf = path_udf(
        lambda p: read_blob(
            p,
            lob_base_dir,
        ),
        BinaryType(),
    )

    ext_udf = path_udf(
        lambda p: lob_extension(
            p,
            spark_types,
            settings,
            lob_base_dir,
        ),
        StringType(),
    )

    fn_udf = path_udf(
        lambda p: lob_filename(
            p,
            spark_types,
            settings,
            lob_base_dir,
        ),
        StringType(),
    )

    # --------------------------------------------------------
    # Process columns
    # --------------------------------------------------------

    for col in list(df.columns):

        st = normalize_column_lookup(
            tinfo,
            col,
        )

        if st is None:

            logger_obj.warning(
                "%s: no datatype mapping",
                col,
            )

            continue

        if st == "CHAR":

            df = df.withColumn(
                col,
                rtrim(
                    F.col(col)
                ),
            )

        is_clob = (
            st in spark_types[
                "clobdatatype"
            ]
        )

        is_blob = (
            st in spark_types[
                "blobdatatype"
            ]
        )

        # ----------------------------------------------------
        # Original converter excludes CLOB/BLOB columns
        # when corresponding switch is false.
        # ----------------------------------------------------

        if (
            is_clob
            and not with_clob
        ) or (
            is_blob
            and not with_blob
        ):

            cleanup_cols.append(
                col
            )

            logger_obj.info(
                "  %s: source='%s' -> "
                "excluded (switch disabled)",
                col,
                st,
            )

            df = df.drop(
                col
            )

            continue

        # ----------------------------------------------------
        # Native LOB handling
        # ----------------------------------------------------

        if native:

            if (
                native_clob
                and st in [
                    "VARCHAR",
                    "NVARCHAR",
                ]
                and with_clob
            ):

                cleanup_cols.append(
                    col
                )

                df = df.withColumn(
                    col,
                    native_text_udf(
                        F.col(col)
                    ),
                )

                mapped = "CLOB/TEXT"

            elif (
                is_blob
                and with_blob
            ):

                cleanup_cols.append(
                    col
                )

                df = (
                    df.withColumn(
                        col + "_ext",
                        ext_udf(
                            F.col(col)
                        ),
                    )
                    .withColumn(
                        col + "_filename",
                        fn_udf(
                            F.col(col)
                        ),
                    )
                    .withColumn(
                        col,
                        blob_udf(
                            F.col(col)
                        ),
                    )
                )

                mapped = "BLOB"

            elif (
                is_clob
                and with_clob
            ):

                cleanup_cols.append(
                    col
                )

                df = df.withColumn(
                    col,
                    clob_udf(
                        F.col(col)
                    ),
                )

                mapped = "CLOB"

            else:

                df, mapped = cast_column(
                    df,
                    col,
                    st,
                    spark_types,
                )

        else:

            in_lob = (
                bool(lob_columns)
                and col in lob_columns
            )

            if (
                st in [
                    "VARCHAR",
                    "NVARCHAR",
                ]
                and native_clob
                and with_clob
            ):

                df = df.withColumn(
                    col,
                    F.col(col).cast(
                        spark_types[
                            "sparkDatatypes"
                        ]["VARCHAR2"]
                    ),
                )

                mapped = "string"

            elif (
                is_blob
                and in_lob
                and with_blob
            ):

                cleanup_cols.append(
                    col
                )

                df = (
                    df.withColumn(
                        col + "_ext",
                        ext_udf(
                            F.col(col)
                        ),
                    )
                    .withColumn(
                        col + "_filename",
                        fn_udf(
                            F.col(col)
                        ),
                    )
                    .withColumn(
                        col,
                        blob_udf(
                            F.col(col)
                        ),
                    )
                )

                mapped = "BLOB"

            elif (
                is_clob
                and in_lob
                and with_clob
            ):

                cleanup_cols.append(
                    col
                )

                df = df.withColumn(
                    col,
                    clob_udf(
                        F.col(col)
                    ),
                )

                mapped = "CLOB"

            else:

                df, mapped = cast_column(
                    df,
                    col,
                    st,
                    spark_types,
                )

        logger_obj.info(
            "  %s: source='%s' -> %s",
            col,
            st,
            mapped,
        )

    # --------------------------------------------------------
    # Write Parquet
    # --------------------------------------------------------

    # No driver-side list of all LOB paths.
    # Path cleanup is optional and can be done by streaming
    # the CSV later.

    tmp = (
        Path(output_root)
        / (table + ".parquet.__inprogress__")
    )

    final = (
        Path(output_root)
        / (table + ".parquet")
    )

    if tmp.exists():

        shutil.rmtree(
            tmp
        )

    if (
        final.exists()
        and settings.get(
            "OVERWRITE",
            "false",
        ).lower()
        != "true"
    ):

        raise FileExistsError(
            f"Output already exists: {final}. "
            f"Set OVERWRITE=true to replace it."
        )

    if final.exists():

        shutil.rmtree(
            final
        )

    tmp.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger_obj.info(
        "OUTPUT    : %s",
        final,
    )

    # --------------------------------------------------------
    # Write Spark schema to detailed log instead of printing
    # it directly to the customer terminal.
    # --------------------------------------------------------

    schema_text = (
        df._jdf.schema().treeString()
    )

    logger_obj.info(
        "SPARK SCHEMA:\n%s",
        schema_text,
    )

    df.write \
        .mode("overwrite") \
        .option(
            "compression",
            settings.get(
                "COMPRESSION",
                "zstd",
            ),
        ) \
        .parquet(
            str(tmp)
        )

    if not any(
        tmp.glob("*.parquet")
    ):

        raise RuntimeError(
            "Spark completed without parquet "
            f"data: {tmp}"
        )

    tmp.rename(
        final
    )

    # --------------------------------------------------------
    # Row count
    # --------------------------------------------------------

    rows = (
        spark.read
        .parquet(
            str(final)
        )
        .count()
    )

    logger_obj.info(
        "SUCCESS   : %s rows=%s",
        final,
        rows,
    )

    # --------------------------------------------------------
    # Optional LOB deletion
    # --------------------------------------------------------

    if (
        settings.get(
            "DELETE_LOB_FILES",
            "false",
        ).lower()
        == "true"
        and cleanup_cols
    ):

        # Stream only the path values from the CSV.
        # Never accumulate all paths in memory.

        cleanup_df = (
            spark.read
            .option(
                "escape",
                settings.get(
                    "ESCAPE",
                    "\\",
                ),
            )
            .option(
                "header",
                True,
            )
            .option(
                "nullValue",
                settings.get(
                    "NULL_VALUE",
                    "null",
                ),
            )
            .option(
                "quote",
                settings.get(
                    "QUOTE",
                    "₠",
                ),
            )
            .option(
                "delimiter",
                settings.get(
                    "DELIMITER",
                    ",",
                ),
            )
            .option(
                "multiLine",
                True,
            )
            .csv(
                str(csv_path),
                encoding=settings.get(
                    "ENCODING",
                    "utf-8",
                ),
            )
            .select(
                *cleanup_cols
            )
        )

        for row in cleanup_df.toLocalIterator():

            for value in row:

                if value:

                    p = resolve_lob_path(
                        value,
                        str(
                            Path(csv_path).parent
                        ),
                    )

                    if p is not None:

                        try:

                            p.unlink()

                        except OSError as e:

                            logger_obj.warning(
                                "Could not delete LOB %s: %s",
                                p,
                                e,
                            )

    # --------------------------------------------------------
    # Optional CSV deletion
    # --------------------------------------------------------

    if (
        settings.get(
            "DELETE_CSV",
            "false",
        ).lower()
        == "true"
    ):

        Path(
            csv_path
        ).unlink()

    # --------------------------------------------------------
    # Manifest
    # --------------------------------------------------------

    append_record(
        manifest_path,
        {
            "timestamp": now(),
            "status": "SUCCESS",
            "input": str(csv_path),
            "table": table,
            "guid": guid,
            "output": str(final),
            "rows": rows,
            "parquet_file": str(
                next(
                    final.glob("*.parquet")
                )
            ),
            "error": "",
        },
    )

    return final


# ============================================================
# REPORT FAILURE CHECK
# ============================================================

def _report_failed(report):

    import json

    from pathlib import Path

    try:

        data = json.loads(
            Path(
                report["path"]
            ).read_text(
                encoding="utf-8"
            )
        )

        for run_data in reversed(
            data.get("runs", [])
        ):

            if (
                run_data.get("run_id")
                == report["run_id"]
            ):

                return bool(
                    run_data.get(
                        "failed",
                        0,
                    )
                )

    except Exception:
        pass

    return False


# ============================================================
# CURRENT RUN
# ============================================================

def write_current_run(
    path,
    data,
):
    """
    Persist the current conversion run state.

    This file is intentionally small and contains only summary
    information. Detailed per-file results remain in the run report.
    """

    path = Path(
        path
    )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            data,
            indent=2,
        ),
        encoding="utf-8",
    )


# ============================================================
# RUN
# ============================================================

def run(
    settings,
    base_dir,
):

    # --------------------------------------------------------
    # Paths
    # --------------------------------------------------------

    log_dir = Path(
        settings.get(
            "LOG_DIR",
            str(
                base_dir / "logs"
            ),
        )
    )

    status_dir = Path(
        settings.get(
            "STATUS_DIR",
            str(
                base_dir / "status"
            ),
        )
    )

    manifest = (
        status_dir
        / "conversion_status.csv"
    )

    current_run_path = (
        status_dir
        / "current_run.json"
    )

    inp = (
        Path(
            settings["INPUT_PATH"]
        )
        .expanduser()
        .resolve()
    )

    out = (
        Path(
            settings["OUTPUT_PATH"]
        )
        .expanduser()
        .resolve()
    )

    dtype = (
        Path(
            settings["DATATYPE_JSON"]
        )
        .expanduser()
        .resolve()
    )

    # --------------------------------------------------------
    # Create directories
    # --------------------------------------------------------

    out.mkdir(
        parents=True,
        exist_ok=True,
    )

    status_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    log_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # Logger
    # --------------------------------------------------------

    log = logger(
        log_dir,
        out,
    )

    # --------------------------------------------------------
    # Spark datatype configuration
    # --------------------------------------------------------

    spark_types = json.loads(
        (
            base_dir
            / "config"
            / "spark_datatypes.json"
        ).read_text(
            encoding="utf-8"
        )
    )

    # --------------------------------------------------------
    # Find CSV files
    # --------------------------------------------------------

    files = (
        [inp]
        if inp.is_file()
        else sorted(
            list(
                inp.rglob("*.CSV")
            )
            +
            list(
                inp.rglob("*.csv")
            )
        )
    )

    log.info(
        "Found %d CSV file(s)",
        len(files),
    )

    # --------------------------------------------------------
    # Previous conversion information
    # --------------------------------------------------------

    previous = latest_by_input(
        manifest
    )

    # --------------------------------------------------------
    # Report
    # --------------------------------------------------------

    report = start_report(
        out,
        inp,
        len(files),
    )

    log.info(
        "APPLICATION: %s",
        report["app"],
    )

    log.info(
        "RUN REPORT : %s",
        report["path"],
    )

    log.info(
        "ERROR LOG  : %s",
        report["error_path"],
    )

    # --------------------------------------------------------
    # Current run information
    # --------------------------------------------------------

    current_run = {
        "status": "RUNNING",

        "pid": os.getpid(),

        "start_time": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "end_time": "",

        "input_path": str(inp),

        "output_path": str(out),

        "datatype_json": str(dtype),

        "log_path": "",

        "report_path": str(
            report["path"]
        ),

        "error_log_path": str(
            report["error_path"]
        ),

        "csv_files": len(files),

        "processed": 0,

        "success": 0,

        "skipped": 0,

        "failed": 0,

        "rows": 0,
    }

    # --------------------------------------------------------
    # Determine detailed log path
    # --------------------------------------------------------

    for handler in log.handlers:

        if isinstance(
            handler,
            logging.FileHandler,
        ):

            current_run[
                "log_path"
            ] = str(
                Path(
                    handler.baseFilename
                ).resolve()
            )

            break

    write_current_run(
        current_run_path,
        current_run,
    )

    # --------------------------------------------------------
    # Start Spark
    # --------------------------------------------------------

    spark = spark_session(
        settings,
        base_dir,
    )

    try:

        # ----------------------------------------------------
        # Process files
        # ----------------------------------------------------

        for csv in files:

            previous_file = previous.get(
                str(csv),
                {},
            )

            # -----------------------------------------------
            # Skip previously successful file
            # -----------------------------------------------

            if (
                previous_file.get(
                    "status"
                )
                == "SUCCESS"

                and Path(
                    previous_file.get(
                        "output",
                        "",
                    )
                ).exists()
            ):

                current_run[
                    "skipped"
                ] += 1

                log.info(
                    "SKIP already successful: %s",
                    csv,
                )

                write_current_run(
                    current_run_path,
                    current_run,
                )

                continue

            # -----------------------------------------------
            # Determine schema/table
            # -----------------------------------------------

            schema, table = (
                fetch_schema_and_table(
                    csv
                )
            )

            csv_output = out

            # -----------------------------------------------
            # Schema-specific output
            # -----------------------------------------------

            if schema:

                parent = out.parent

                csv_output = (
                    parent
                    / f"{parent.name}_{schema}"
                    / out.name
                )

            csv_output.mkdir(
                parents=True,
                exist_ok=True,
            )

            # -----------------------------------------------
            # Running manifest entry
            # -----------------------------------------------

            append_record(
                manifest,
                {
                    "timestamp": now(),

                    "status": "RUNNING",

                    "input": str(csv),

                    "table": table,

                    "guid": "",

                    "output": str(
                        csv_output
                    ),

                    "rows": "",

                    "parquet_file": "",

                    "error": "",
                },
            )

            try:

                # -------------------------------------------
                # Actual conversion
                # -------------------------------------------

                final = process_csv(
                    spark,
                    csv,
                    csv_output,
                    dtype,
                    settings,
                    spark_types,
                    log,
                    manifest,
                )

                parquet_files = list(
                    final.glob(
                        "*.parquet"
                    )
                )

                # -------------------------------------------
                # Get row count from manifest
                # -------------------------------------------

                latest = (
                    latest_by_input(
                        manifest
                    ).get(
                        str(csv),
                        {},
                    )
                )

                try:

                    rows = int(
                        latest.get(
                            "rows"
                        )
                        or 0
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    rows = 0

                # -------------------------------------------
                # Add successful file to report
                # -------------------------------------------

                add_file_result(
                    report,
                    {
                        "status": "SUCCESS",

                        "csv": str(csv),

                        "schema": (
                            schema
                            or ""
                        ),

                        "table": table,

                        "output": str(
                            final
                        ),

                        "rows": rows,

                        "parquet_file": (
                            str(
                                parquet_files[0]
                            )
                            if parquet_files
                            else ""
                        ),
                    },
                )

                log.info(
                    "FILE SUCCESS: %s | rows=%s | output=%s",
                    table,
                    rows,
                    final,
                )

            except Exception as e:

                error_text = (
                    traceback.format_exc()
                )

                # Detailed traceback remains in
                # the developer log.

                log.exception(
                    "FAILED: %s",
                    csv,
                )

                append_record(
                    manifest,
                    {
                        "timestamp": now(),

                        "status": "FAILED",

                        "input": str(csv),

                        "table": table,

                        "guid": "",

                        "output": "",

                        "rows": "",

                        "parquet_file": "",

                        "error": repr(e),
                    },
                )

                append_error(
                    report,
                    csv,
                    error_text,
                )

                add_file_result(
                    report,
                    {
                        "status": "FAILED",

                        "csv": str(csv),

                        "schema": (
                            schema
                            or ""
                        ),

                        "table": table,

                        "output": "",

                        "rows": 0,

                        "error": repr(e),
                    },
                )

            # -----------------------------------------------
            # Update current run
            # -----------------------------------------------

            try:

                report_data = json.loads(
                    Path(
                        report["path"]
                    ).read_text(
                        encoding="utf-8"
                    )
                )

                runs = report_data.get(
                    "runs",
                    [],
                )

                latest_run = (
                    runs[-1]
                    if runs
                    else {}
                )

                current_run[
                    "processed"
                ] = latest_run.get(
                    "processed",
                    current_run[
                        "processed"
                    ] + 1,
                )

                current_run[
                    "success"
                ] = latest_run.get(
                    "success",
                    current_run[
                        "success"
                    ],
                )

                current_run[
                    "skipped"
                ] = latest_run.get(
                    "skipped",
                    current_run[
                        "skipped"
                    ],
                )

                current_run[
                    "failed"
                ] = latest_run.get(
                    "failed",
                    current_run[
                        "failed"
                    ],
                )

                current_run[
                    "rows"
                ] = latest_run.get(
                    "rows",
                    current_run[
                        "rows"
                    ],
                )

            except Exception:

                current_run[
                    "processed"
                ] += 1

            # -----------------------------------------------
            # Save current status
            # -----------------------------------------------

            write_current_run(
                current_run_path,
                current_run,
            )

        # ----------------------------------------------------
        # Final status
        # ----------------------------------------------------

        failed = _report_failed(
            report
        )

        final_status = (
            "FAILED"
            if failed
            else "SUCCESS"
        )

        finish_report(
            report,
            final_status,
        )

        # ----------------------------------------------------
        # Read final report statistics
        # ----------------------------------------------------

        try:

            report_data = json.loads(
                Path(
                    report["path"]
                ).read_text(
                    encoding="utf-8"
                )
            )

            runs = report_data.get(
                "runs",
                [],
            )

            latest_run = (
                runs[-1]
                if runs
                else {}
            )

            current_run[
                "processed"
            ] = latest_run.get(
                "processed",
                current_run[
                    "processed"
                ],
            )

            current_run[
                "success"
            ] = latest_run.get(
                "success",
                current_run[
                    "success"
                ],
            )

            current_run[
                "failed"
            ] = latest_run.get(
                "failed",
                current_run[
                    "failed"
                ],
            )

            current_run[
                "skipped"
            ] = latest_run.get(
                "skipped",
                current_run[
                    "skipped"
                ],
            )

            current_run[
                "rows"
            ] = latest_run.get(
                "rows",
                current_run[
                    "rows"
                ],
            )

            current_run[
                "end_time"
            ] = latest_run.get(
                "end_time",
                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            )

        except Exception as exc:

            log.warning(
                "Could not read final report statistics: %s",
                exc,
            )

            current_run[
                "end_time"
            ] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        # ----------------------------------------------------
        # Final current run status
        # ----------------------------------------------------

        current_run[
            "status"
        ] = final_status

        write_current_run(
            current_run_path,
            current_run,
        )

        # ----------------------------------------------------
        # CUSTOMER-FACING FINAL SUMMARY
        #
        # Do NOT use log.info() here.
        # INFO messages are intentionally file-only.
        # ----------------------------------------------------

        print()
        print("=" * 60)
        print(
            "             CONVERSION COMPLETED"
        )
        print("=" * 60)
        print()

        print(
            f"STATUS       : {final_status}"
        )

        print()

        print(
            f"TOTAL FILES  : "
            f"{current_run.get('csv_files', 0)}"
        )

        print(
            f"PROCESSED    : "
            f"{current_run.get('processed', 0)}"
        )

        print(
            f"SUCCESS      : "
            f"{current_run.get('success', 0)}"
        )

        print(
            f"SKIPPED      : "
            f"{current_run.get('skipped', 0)}"
        )

        print(
            f"FAILED       : "
            f"{current_run.get('failed', 0)}"
        )

        print(
            f"TOTAL ROWS   : "
            f"{current_run.get('rows', 0)}"
        )

        print()

        print(
            "REPORT       :",
            report["path"],
        )

        print(
            "ERROR LOG    :",
            report["error_path"],
        )

        print(
            "DETAILED LOG :",
            current_run.get(
                "log_path",
                "",
            ),
        )

        print()

        print("=" * 60)

    finally:

        spark.stop()

        # ----------------------------------------------------
        # Remove PID file if it belongs to this process
        # ----------------------------------------------------

        pid_file = (
            status_dir
            / "converter.pid"
        )

        if pid_file.exists():

            try:

                if (
                    pid_file.read_text(
                        encoding="utf-8"
                    ).strip()
                    == str(os.getpid())
                ):

                    pid_file.unlink()

            except OSError:

                pass