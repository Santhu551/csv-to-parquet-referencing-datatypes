import json
import logging
import os
import re
import shutil
import socket
import time
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
def safe_col(col_name):
    """Reference a Spark column by its exact name, including dots or backticks."""
    escaped_name = col_name.replace("`", "``")
    return F.col(f"`{escaped_name}`")


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
        console_handler = logging.StreamHandler()
        console_handler.setLevel(
            logging.WARNING
        )
        console_handler.setFormatter( formatter )
        log.addHandler( console_handler)

    return log
def create_debug_logger(debug_log_dir, output_path=None):
    """Create a per-run diagnostic log separate from normal logs."""
    debug_dir = Path(debug_log_dir).expanduser()
    debug_dir.mkdir(parents=True, exist_ok=True)

    if output_path and Path(output_path).resolve().name.lower() == "parquet":
        app = Path(output_path).resolve().parent.name
    elif output_path:
        app = Path(output_path).resolve().name
    else:
        app = "csv_to_parquet"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = debug_dir / f"{app}_{timestamp}_pid{os.getpid()}_debug.log"

    debug_log = logging.getLogger(
        f"csv_to_parquet.debug.{os.getpid()}.{timestamp}"
    )
    debug_log.setLevel(logging.DEBUG)
    debug_log.propagate = False

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d %(funcName)s] %(message)s"
    ))
    debug_log.addHandler(handler)

    debug_log.info(
        "DIAGNOSTIC_LOG_STARTED | log_file=%s | pid=%s | ppid=%s | host=%s | cwd=%s",
        log_path, os.getpid(), os.getppid(), socket.gethostname(), os.getcwd(),
    )
    return debug_log

def _debug_path_state(debug_log, path, label, max_entries=25):
    """Log filesystem metadata and a small directory snapshot; never mutate paths."""
    path = Path(path)
    try:
        debug_log.info(
            "PATH_STATE | label=%s | path=%s | exists=%s | is_symlink=%s | is_dir=%s",
            label, path, path.exists(), path.is_symlink(), path.is_dir(),
        )
        if path.exists() or path.is_symlink():
            st = path.lstat()
            debug_log.info(
                "PATH_STAT | label=%s | path=%s | mode=%s | device=%s | inode=%s | size=%s | mtime=%s",
                label, path, oct(st.st_mode), st.st_dev, st.st_ino, st.st_size,
                datetime.fromtimestamp(st.st_mtime).isoformat(),
            )
    except OSError:
        debug_log.exception("PATH_INSPECTION_FAILED | label=%s | path=%s", label, path)

    try:
        probe = path
        while not (probe.exists() or probe.is_symlink()):
            parent = probe.parent
            if parent == probe:
                break
            probe = parent
        if probe.is_file() or probe.is_symlink():
            probe = probe.parent
        fs = os.statvfs(str(probe))
        debug_log.info(
            "FILESYSTEM_SPACE | label=%s | probe=%s | total_bytes=%s | available_bytes=%s | free_inodes=%s | total_inodes=%s",
            label, probe, fs.f_blocks * fs.f_frsize,
            fs.f_bavail * fs.f_frsize, fs.f_favail, fs.f_files,
        )
    except OSError:
        debug_log.exception("FILESYSTEM_INSPECTION_FAILED | label=%s | path=%s", label, path)

    if not path.is_dir() or path.is_symlink():
        return
    try:
        entries = sorted(path.iterdir(), key=lambda item: item.name)[:max_entries]
        for item in entries:
            try:
                st = item.lstat()
                item_type = (
                    "symlink" if item.is_symlink()
                    else "directory" if item.is_dir()
                    else "file"
                )
                debug_log.info(
                    "PATH_ENTRY | label=%s | path=%s | type=%s | size=%s | inode=%s",
                    label, item, item_type, st.st_size, st.st_ino,
                )
            except OSError:
                debug_log.exception("PATH_ENTRY_INSPECTION_FAILED | path=%s", item)
        if len(entries) == max_entries:
            debug_log.info("PATH_ENTRY_LIMIT | label=%s | limit=%s", label, max_entries)
    except OSError:
        debug_log.exception("PATH_LIST_FAILED | label=%s | path=%s", label, path)

def _debug_spark_configuration(debug_log, spark):
    """Record effective Spark identity and relevant configuration once per run."""
    sc = spark.sparkContext
    debug_log.info(
        "SPARK_SESSION | version=%s | app_name=%s | app_id=%s | master=%s | python_pid=%s | ppid=%s | host=%s",
        spark.version, sc.appName, sc.applicationId, sc.master,
        os.getpid(), os.getppid(), socket.gethostname(),
    )
    conf = sc.getConf()
    for key in (
        "spark.local.dir",
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.sql.parquet.datetimeRebaseModeInWrite",
        "spark.sql.parquet.int96RebaseModeInWrite",
        # "spark.sql.legacy.timeParserPolicy"

    ):
        try:
            if key.startswith("spark.sql.parquet."):
                value = spark.conf.get(key)
            else:
                value = conf.get(key, "<unset>")
            debug_log.info("SPARK_CONFIG | %s=%s", key, value)
        except Exception:
            debug_log.exception("SPARK_CONFIG_READ_FAILED | key=%s", key)

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
        "spark.sql.parquet.datetimeRebaseModeInWrite": settings.get(
            "PARQUET_DATETIME_REBASE_MODE", "CORRECTED"
        ),
        "spark.sql.parquet.int96RebaseModeInWrite": settings.get(
            "PARQUET_INT96_REBASE_MODE", "CORRECTED"
        ),
        # "spark.sql.legacy.timeParserPolicy": settings.get(
        #     "LEGACY_TIME_PARSER_POLICY", "LEGACY"
        # ),

    }.items():
        b = b.config(
            k,
            v,
        )
    spark = b.getOrCreate()

    # Match Archive Viewer: sparkprocess.py explicitly uses UTC before CSV parsing.
    # Set it on the active session as well as ensuring the effective session config.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sparkContext.setLogLevel(
        "ERROR"
    )
    return spark
def _debug_timestamp_samples(debug_log, df, col, phase, max_rows=20):
    """Log sample timestamp values without changing the DataFrame."""
    if debug_log is None or col not in df.columns:
        return
    try:
        data_type = df.schema[col].dataType.simpleString()
        debug_log.info(
            "TIMESTAMP_DIAGNOSTIC | phase=%s | column=%s | spark_type=%s",
            phase, col, data_type,
        )
        selected = []
        if "ID" in df.columns and col.upper() != "ID":
            selected.append(F.col("ID").cast("string").alias("__debug_id"))
        selected.append(safe_col(col).cast("string").alias("__debug_value"))
        values = df.select(*selected)
        sentinel_rows = (
            values.where(F.col("__debug_value").rlike("1753|9999"))
            .limit(max_rows).collect()
        )
        debug_log.info(
            "TIMESTAMP_SENTINEL_VALUES | phase=%s | column=%s | count_sampled=%s | rows=%s",
            phase, col, len(sentinel_rows),
            [row.asDict(recursive=True) for row in sentinel_rows],
        )
        sample_rows = values.limit(min(5, max_rows)).collect()
        debug_log.info(
            "TIMESTAMP_GENERAL_SAMPLE | phase=%s | column=%s | rows=%s",
            phase, col, [row.asDict(recursive=True) for row in sample_rows],
        )
    except Exception:
        debug_log.exception(
            "TIMESTAMP_DIAGNOSTIC_FAILED | phase=%s | column=%s",
            phase, col,
        )

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
                safe_col(col).cast(
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
                safe_col(col).cast(
                    spark_types[
                        "sparkDatatypes"
                    ]["NUMBER"]
                ),
            ),
            "double",
        )
    if st.startswith("DECIMAL"):
        target = spark_types[
            "sparkDatatypes"
        ]["DECIMAL"]
        return (
            df.withColumn(
                col,
                safe_col(col).cast(target),
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
                    safe_col(col),
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
                    safe_col(col).rlike(
                        r"^\d{1,2}/\d{1,2}/\d{4} "
                        r"\d{1,2}:\d{2}:\d{1,2} "
                        r"(AM|PM)$"
                    ),
                    F.to_timestamp(
                        safe_col(col),
                        "M/d/yyyy hh:mm:ss a",
                    ),
                )
                .when(
                    safe_col(col).rlike(
                        r".*\d{2}:\d{2}:\d{2}.*"
                    ),
                    F.to_date(
                        safe_col(col),
                        "M/d/yyyy hh:mm:ss a",
                    ),
                )
                .when(
                    safe_col(col).rlike(
                        r"^\d{1,2}/\d{1,2}/\d{4}$"
                    ),
                    F.to_date(
                        safe_col(col),
                        "M/d/yyyy",
                    ),
                )
                .otherwise(
                    safe_col(col).cast("date")
                ),
            ),
            mapped,
        )

    return (
        df.withColumn(
            col,
            safe_col(col).cast(mapped),
        ),
        mapped,
    )
def process_csv(
    spark,
    csv_path,
    output_root,
    datatype_json,
    settings,
    spark_types,
    logger_obj,
    manifest_path,
    debug_log=None,
):

    if debug_log:
        debug_log.info(
            "CSV_PROCESS_START | csv=%s | output_root=%s | datatype_json=%s | pid=%s",
            csv_path, output_root, datatype_json, os.getpid(),
        )

    if debug_log:
        debug_log.info("STAGE_START | stage=load_datatypes | csv=%s", csv_path)
    data = load_datatypes(
        datatype_json
    )
    if debug_log:
        debug_log.info("STAGE_COMPLETE | stage=load_datatypes | csv=%s", csv_path)

    if debug_log:
        debug_log.info("STAGE_START | stage=resolve_table_metadata | csv=%s", csv_path)
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

    if debug_log:
        debug_log.info(
            "TABLE_METADATA_RESOLVED | csv=%s | schema=%s | table=%s | metadata_table=%s | guid=%s | native_lob=%s | native_clob=%s",
            csv_path, schema, table, metadata_table, guid, native, native_clob,
        )
        debug_log.info("STAGE_COMPLETE | stage=resolve_table_metadata | table=%s", table)

    if isinstance(tinfo, dict):

        lob_columns = tinfo.get(
            "LOB_COLUMNS",
            [],
        )

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

    if debug_log:
        debug_log.info("STAGE_START | stage=read_csv | table=%s | csv=%s", table, csv_path)

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
        .option("timestampFormat", "MM/dd/yyyy hh:mm:ss a")
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

    if debug_log:
        debug_log.info(
            "STAGE_COMPLETE | stage=read_csv | table=%s | columns=%s",
            table, df.columns,
        )
        debug_log.info(
            "CSV_READ_OPTIONS | encoding=%s | delimiter=%r | quote=%r | escape=%r | header=true | multiline=true",
            settings.get("ENCODING", "utf-8"),
            settings.get("DELIMITER", ","),
            settings.get("QUOTE", "₠"),
            settings.get("ESCAPE", "\\\\"),
        )
        debug_log.info(
            "TIMESTAMP_PARSE_FORMAT | configured_timestamp_format=%r",
            spark_types.get("datetimeformat", {}).get("timestamp"),
        )

    df = df.na.replace(
    "F",
    None,
)

    df.na.replace("Φ", None)

    df = df.na.replace(
    "\u03A6",
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

    for col in list(df.columns):

        st = normalize_column_lookup(
            tinfo,
            col,
        )

        expected_spark_type = (
            spark_types.get("sparkDatatypes", {}).get(st)
            if st is not None else None
        )
        if expected_spark_type == "timestamp":
            if debug_log:
                debug_log.info(
                    "TIMESTAMP_MAPPING | column=%s | source_type=%s | mapped_type=%s | configured_format=%r",
                    col, st, expected_spark_type,
                    spark_types.get("datetimeformat", {}).get("timestamp"),
                )
            _debug_timestamp_samples(debug_log, df, col, "RAW_BEFORE_CAST")

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
                    safe_col(col)
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
                        safe_col(col)
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
                            safe_col(col)
                        ),
                    )
                    .withColumn(
                        col + "_filename",
                        fn_udf(
                            safe_col(col)
                        ),
                    )
                    .withColumn(
                        col,
                        blob_udf(
                            safe_col(col)
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
                        safe_col(col)
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
                if expected_spark_type == "timestamp":
                    _debug_timestamp_samples(debug_log, df, col, "PARSED_AFTER_CAST")

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
                    safe_col(col).cast(
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
                            safe_col(col)
                        ),
                    )
                    .withColumn(
                        col + "_filename",
                        fn_udf(
                            safe_col(col)
                        ),
                    )
                    .withColumn(
                        col,
                        blob_udf(
                            safe_col(col)
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
                        safe_col(col)
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
                if expected_spark_type == "timestamp":
                    _debug_timestamp_samples(debug_log, df, col, "PARSED_AFTER_CAST")

        logger_obj.info(
            "  %s: source='%s' -> %s",
            col,
            st,
            mapped,
        )

    tmp = (
        Path(output_root)
        / (table + ".parquet.__inprogress__")
    )

    final = (
        Path(output_root)
        / (table + ".parquet")
    )

    if debug_log:
        debug_log.info(
            "TABLE_WRITE_PREPARE | table=%s | csv=%s | output_root=%s | staging=%s | final=%s | compression=%s | overwrite=%s",
            table, csv_path, output_root, tmp, final,
            settings.get("COMPRESSION", "zstd"),
            settings.get("OVERWRITE", "false"),
        )
        _debug_path_state(debug_log, tmp, "STAGING_BEFORE_CLEANUP")
        _debug_path_state(debug_log, final, "FINAL_BEFORE_WRITE")

    if tmp.exists():
        if debug_log:
            debug_log.info("STAGING_CLEANUP_START | table=%s | path=%s", table, tmp)
        shutil.rmtree(
            tmp
        )
        if debug_log:
            debug_log.info("STAGING_CLEANUP_COMPLETE | table=%s | path=%s", table, tmp)

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
        if debug_log:
            debug_log.info("FINAL_OUTPUT_CLEANUP_START | table=%s | path=%s", table, final)
        shutil.rmtree(
            final
        )
        if debug_log:
            debug_log.info("FINAL_OUTPUT_CLEANUP_COMPLETE | table=%s | path=%s", table, final)

    tmp.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger_obj.info(
        "OUTPUT    : %s",
        final,
    )

    schema_text = (
        df._jdf.schema().treeString()
    )

    logger_obj.info(
        "SPARK SCHEMA:\n%s",
        schema_text,
    )
    if debug_log:
        debug_log.info("DATAFRAME_SCHEMA | table=%s | schema=\\n%s", table, schema_text)
        for diagnostic_col in (
            "STARTDATE", "FACETSSTARTDATE", "ENDDATE", "FACETSENDDATE",
            "CREATEDDATE", "UPDATEDDATE", "FACETSSYNCDATE",
        ):
            if diagnostic_col in df.columns:
                _debug_timestamp_samples(
                    debug_log, df, diagnostic_col, "FINAL_BEFORE_PARQUET_WRITE"
                )

    write_started = time.perf_counter()
    if debug_log:
        debug_log.info(
            "PARQUET_WRITE_START | table=%s | csv=%s | staging=%s | final=%s",
            table, csv_path, tmp, final,
        )

    try:
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
    except Exception:
        if debug_log:
            debug_log.exception(
                "PARQUET_WRITE_FAILED | table=%s | csv=%s | staging=%s | elapsed_seconds=%.3f",
                table, csv_path, tmp, time.perf_counter() - write_started,
            )
            _debug_path_state(debug_log, tmp, "STAGING_AFTER_WRITE_FAILURE")
        raise
    else:
        if debug_log:
            debug_log.info(
                "PARQUET_WRITE_RETURNED | table=%s | staging=%s | elapsed_seconds=%.3f",
                table, tmp, time.perf_counter() - write_started,
            )
            _debug_path_state(debug_log, tmp, "STAGING_AFTER_WRITE")

    if not any(
        tmp.glob("*.parquet")
    ):
        if debug_log:
            debug_log.error(
                "PARQUET_OUTPUT_EMPTY | table=%s | staging=%s | entries=%s",
                table, tmp, [p.name for p in list(tmp.iterdir())[:25]] if tmp.exists() else "<missing>",
            )

        raise RuntimeError(
            "Spark completed without parquet "
            f"data: {tmp}"
        )

    if debug_log:
        debug_log.info("STAGE_START | stage=rename_staging_to_final | table=%s | staging=%s | final=%s", table, tmp, final)

    try:
        tmp.rename(final)
    except Exception:
        if debug_log:
            debug_log.exception("OUTPUT_RENAME_FAILED | table=%s | staging=%s | final=%s", table, tmp, final)
            _debug_path_state(debug_log, tmp, "STAGING_AFTER_RENAME_FAILURE")
            _debug_path_state(debug_log, final, "FINAL_AFTER_RENAME_FAILURE")
        raise

    if debug_log:
        debug_log.info("STAGE_COMPLETE | stage=rename_staging_to_final | table=%s | final=%s", table, final)

    if debug_log:
        debug_log.info("STAGE_START | stage=readback_row_count | table=%s | final=%s", table, final)

    try:
        rows = (
            spark.read
            .parquet(
                str(final)
            )
            .count()
        )
    except Exception:
        if debug_log:
            debug_log.exception("PARQUET_READBACK_FAILED | table=%s | final=%s", table, final)
        raise

    if debug_log:
        debug_log.info("STAGE_COMPLETE | stage=readback_row_count | table=%s | rows=%s", table, rows)

    logger_obj.info(
        "SUCCESS   : %s rows=%s",
        final,
        rows,
    )

    if (
        settings.get(
            "DELETE_LOB_FILES",
            "false",
        ).lower()
        == "true"
        and cleanup_cols
    ):

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

def run(
    settings,
    base_dir,
):

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

    log = logger(
        log_dir,
        out,
    )

    debug_dir_value = settings.get("DATETIME_DEBUG_LOG_DIR", "./datetimelogs")
    debug_dir = Path(debug_dir_value).expanduser()
    if not debug_dir.is_absolute():
        debug_dir = Path(base_dir) / debug_dir
    debug_log = create_debug_logger(debug_dir, out)
    debug_file_path = next(
        (Path(handler.baseFilename) for handler in debug_log.handlers
         if isinstance(handler, logging.FileHandler)),
        debug_dir,
    )
    debug_log.info(
        "RUN_START | input=%s | output=%s | datatype_json=%s | debug_log=%s",
        inp, out, dtype, debug_file_path,
    )

    spark_types = json.loads(
        (
            base_dir
            / "config"
            / "spark_datatypes.json"
        ).read_text(
            encoding="utf-8"
        )
    )

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

    previous = latest_by_input(
        manifest
    )

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

        "debug_log_path": str(debug_file_path.resolve()),

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

    debug_log.info("STAGE_START | stage=create_spark_session")
    try:
        spark = spark_session(
            settings,
            base_dir,
        )
    except Exception:
        debug_log.exception("SPARK_SESSION_START_FAILED")
        raise

    debug_log.info("STAGE_COMPLETE | stage=create_spark_session")
    _debug_spark_configuration(debug_log, spark)
    _debug_path_state(debug_log, out, "OUTPUT_ROOT_AT_RUN_START")

    try:

        for csv in files:

            previous_file = previous.get(
                str(csv),
                {},
            )

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

            schema, table = (
                fetch_schema_and_table(
                    csv
                )
            )

            csv_output = out

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

                final = process_csv(
                    spark,
                    csv,
                    csv_output,
                    dtype,
                    settings,
                    spark_types,
                    log,
                    manifest,
                    debug_log,
                )

                parquet_files = list(
                    final.glob(
                        "*.parquet"
                    )
                )

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

                debug_log.exception(
                    "CSV_CONVERSION_FAILED | csv=%s | table=%s | exception_type=%s | exception=%s",
                    csv, table, type(e).__name__, e,
                )
                error_text = (
                    traceback.format_exc()
                )

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

            write_current_run(
                current_run_path,
                current_run,
            )

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
        debug_log.info(
            "RUN_FINISHED | status=%s | report=%s | error_log=%s",
            final_status, report.get("path"), report.get("error_path"),
        )

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

        current_run[
            "status"
        ] = final_status

        write_current_run(
            current_run_path,
            current_run,
        )

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
        print(
            "DEBUG LOG    :",
            current_run.get(
                "debug_log_path",
                "",
            ),
        )

        print()

        print("=" * 60)

    finally:

        spark.stop()
        debug_log.info("SPARK_SESSION_STOPPED")
        for handler in list(debug_log.handlers):
            try:
                handler.flush()
                handler.close()
            finally:
                debug_log.removeHandler(handler)

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
