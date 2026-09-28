import json
import logging
import re
import shutil
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import rtrim, udf
from pyspark.sql.types import BinaryType, StringType

from .table import fetch_tablename_csv, fetch_schema_and_table, resolve_guid_and_table, load_datatypes, column_types, normalize_column_lookup
from .lob import read_clob, read_native_text, read_blob, lob_extension, lob_filename, resolve_lob_path
from .manifest import append_record, latest_by_input, now
from .run_report import start_report, update_report, add_file_result, finish_report, append_error


def logger(log_dir, output_path=None):
    Path(log_dir).mkdir(parents=True,exist_ok=True)
    l=logging.getLogger("csv_to_parquet")
    l.setLevel(logging.INFO)
    if not l.handlers:
        app = Path(output_path).resolve().parent.name if output_path and Path(output_path).resolve().name.lower() == "parquet" else (Path(output_path).resolve().name if output_path else "csv_to_parquet")
        date = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
        h=logging.FileHandler(Path(log_dir)/f"{app}_{date}.log")
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s")); l.addHandler(h)
        sh=logging.StreamHandler(); sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s")); l.addHandler(sh)
    return l


def spark_session(settings):
    b=SparkSession.builder.appName("ArchiveViewer-CSV-to-Parquet-Standalone")
    for k,v in {
      "spark.driver.memory":settings.get("SPARK_DRIVER_MEMORY","4g"),
      "spark.executor.memory":settings.get("SPARK_EXECUTOR_MEMORY","4g"),
      "spark.sql.shuffle.partitions":settings.get("SPARK_SQL_SHUFFLE_PARTITIONS","200"),
      "spark.network.timeout":settings.get("SPARK_NETWORK_TIMEOUT","800s"),
      "spark.executor.heartbeatInterval":settings.get("SPARK_EXECUTOR_HEARTBEAT_INTERVAL","60s"),
    }.items(): b=b.config(k,v)
    return b.getOrCreate()


def cast_column(df, col, source_type, spark_types):
    st=source_type
    if st is None: return df, "unmapped"
    if st == "NUMBER": return df.withColumn(col,F.col(col).cast(spark_types["sparkDatatypes"]["NUMBER"])), "double"
    if re.match(r"NUMBER\(\d+,\d+\)", st): return df.withColumn(col,F.col(col).cast(spark_types["sparkDatatypes"]["NUMBER"])), "double"
    if st.startswith("DECIMAL"):
        # Exact original branch: DECIMAL and DECIMAL(p,s) are cast using the configured DECIMAL mapping.
        target=spark_types["sparkDatatypes"]["DECIMAL"]
        return df.withColumn(col,F.col(col).cast(target)), target
    mapped=spark_types["sparkDatatypes"].get(st)
    if mapped is None: return df, "unmapped"
    if mapped == "timestamp":
        return df.withColumn(col,F.to_timestamp(F.col(col),spark_types["datetimeformat"]["timestamp"])),mapped
    if mapped == "date":
        return df.withColumn(col,F.when(F.col(col).rlike(r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{1,2} (AM|PM)$"),F.to_timestamp(F.col(col),"M/d/yyyy hh:mm:ss a"))          .when(F.col(col).rlike(r".*\d{2}:\d{2}:\d{2}.*"),F.to_date(F.col(col),"M/d/yyyy hh:mm:ss a"))          .when(F.col(col).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),F.to_date(F.col(col),"M/d/yyyy"))          .otherwise(F.col(col).cast("date"))),mapped
    return df.withColumn(col,F.col(col).cast(mapped)),mapped


def process_csv(spark, csv_path, output_root, datatype_json, settings, spark_types, logger_obj, manifest_path):
    data=load_datatypes(datatype_json)
    schema, table = fetch_schema_and_table(csv_path)
    qualified_table = fetch_tablename_csv(csv_path)
    guid, metadata_table = resolve_guid_and_table(
        data, table, qualified_name=qualified_table, schema=schema, table_info_key="TABLE_INFO"
    )
    tinfo=column_types(data,guid,metadata_table)
    native=bool(data[guid].get("NATIVE_LOB_PRESENT",False))
    native_clob=bool(data[guid].get("SQL_SERVER_DATABASE",False))
    lob_columns=[]
    if isinstance(tinfo,dict):
        lob_columns=tinfo.get("LOB_COLUMNS",[])
    logger_obj.info("CSV       : %s",csv_path)
    logger_obj.info("SCHEMA    : %s",schema if schema else "(default/no schema prefix)")
    logger_obj.info("TABLE     : %s",table)
    logger_obj.info("METADATA TABLE KEY: %s",metadata_table)
    logger_obj.info("GUID      : %s",guid)
    logger_obj.info("NATIVE    : %s",native)
    logger_obj.info("NATIVE CLOB/VARCHAR: %s",native_clob)
    logger_obj.info("LOB cols  : %s",lob_columns)

    df=(spark.read.option("escape",settings.get("ESCAPE","\\"))
        .option("header",True).option("nullValue",settings.get("NULL_VALUE","null"))
        .option("quote",settings.get("QUOTE","₠")).option("delimiter",settings.get("DELIMITER",","))
        .option("multiLine",True).csv(str(csv_path),encoding=settings.get("ENCODING","utf-8")))
    df=df.na.replace("Φ",None)

    with_clob=settings.get("WITH_CLOB","true").lower()=="true"
    with_blob=settings.get("WITH_BLOB","true").lower()=="true"
    cleanup_manifest=Path(manifest_path).with_suffix(".lobpaths")
    cleanup_manifest.parent.mkdir(parents=True,exist_ok=True)
    if cleanup_manifest.exists(): cleanup_manifest.unlink()
    cleanup_cols=[]

    def path_udf(func, return_type): return udf(func,return_type)
    lob_base_dir=str(Path(csv_path).parent)
    clob_udf=path_udf(lambda p: read_clob(p,lob_base_dir),StringType())
    native_text_udf=path_udf(lambda p: read_native_text(p,lob_base_dir),StringType())
    blob_udf=path_udf(lambda p: read_blob(p,lob_base_dir),BinaryType())
    ext_udf=path_udf(lambda p: lob_extension(p,spark_types,settings,lob_base_dir),StringType())
    fn_udf=path_udf(lambda p: lob_filename(p,spark_types,settings,lob_base_dir),StringType())

    for col in list(df.columns):
        st=normalize_column_lookup(tinfo,col)
        if st is None:
            logger_obj.warning("%s: no datatype mapping",col); continue
        if st == "CHAR": df=df.withColumn(col,rtrim(F.col(col)))

        is_clob=st in spark_types["clobdatatype"]
        is_blob=st in spark_types["blobdatatype"]
        # Original converter excludes CLOB/BLOB columns when the corresponding switch is false.
        if (is_clob and not with_clob) or (is_blob and not with_blob):
            cleanup_cols.append(col)
            logger_obj.info("  %s: source='%s' -> excluded (switch disabled)",col,st)
            df=df.drop(col)
            continue
        # Native SQL Server text can be ordinary text or a sidecar reference; dereference only when the value resolves to a real file.
        if native:
            if native_clob and st in ["VARCHAR","NVARCHAR"] and with_clob:
                cleanup_cols.append(col); df=df.withColumn(col,native_text_udf(F.col(col))); mapped="CLOB/TEXT"
            elif is_blob and with_blob:
                cleanup_cols.append(col); df=df.withColumn(col+"_ext",ext_udf(F.col(col))).withColumn(col+"_filename",fn_udf(F.col(col))).withColumn(col,blob_udf(F.col(col))); mapped="BLOB"
            elif is_clob and with_clob:
                cleanup_cols.append(col); df=df.withColumn(col,clob_udf(F.col(col))); mapped="CLOB"
            else:
                df,mapped=cast_column(df,col,st,spark_types)
        else:
            in_lob=bool(lob_columns) and col in lob_columns
            if st in ["VARCHAR","NVARCHAR"] and native_clob and with_clob:
                df=df.withColumn(col,F.col(col).cast(spark_types["sparkDatatypes"]["VARCHAR2"])); mapped="string"
            elif is_blob and in_lob and with_blob:
                cleanup_cols.append(col); df=df.withColumn(col+"_ext",ext_udf(F.col(col))).withColumn(col+"_filename",fn_udf(F.col(col))).withColumn(col,blob_udf(F.col(col))); mapped="BLOB"
            elif is_clob and in_lob and with_clob:
                cleanup_cols.append(col); df=df.withColumn(col,clob_udf(F.col(col))); mapped="CLOB"
            else:
                df,mapped=cast_column(df,col,st,spark_types)
        logger_obj.info("  %s: source='%s' -> %s",col,st,mapped)

    # No driver-side list of all LOB paths: path cleanup is optional and can be done by streaming the CSV later.
    tmp=Path(output_root)/(table+".parquet.__inprogress__")
    final=Path(output_root)/(table+".parquet")
    if tmp.exists(): shutil.rmtree(tmp)
    if final.exists() and settings.get("OVERWRITE","false").lower()!="true":
        raise FileExistsError(f"Output already exists: {final}. Set OVERWRITE=true to replace it.")
    if final.exists(): shutil.rmtree(final)
    tmp.mkdir(parents=True,exist_ok=True)

    logger_obj.info("OUTPUT    : %s",final)
    df.printSchema()
    df.write.mode("overwrite").option("compression",settings.get("COMPRESSION","zstd")).parquet(str(tmp))
    if not any(tmp.glob("*.parquet")):
        raise RuntimeError(f"Spark completed without parquet data: {tmp}")
    tmp.rename(final)
    # Row count is an action, but does not collect rows to the driver.
    rows=spark.read.parquet(str(final)).count()
    logger_obj.info("SUCCESS   : %s rows=%s",final,rows)

    if settings.get("DELETE_LOB_FILES","false").lower()=="true" and cleanup_cols:
        # Stream only the path values from the CSV. Never accumulate all paths in memory.
        for row in spark.read.option("escape",settings.get("ESCAPE","\\")).option("header",True).option("nullValue",settings.get("NULL_VALUE","null")).option("quote",settings.get("QUOTE","₠")).option("delimiter",settings.get("DELIMITER",",")).option("multiLine",True).csv(str(csv_path),encoding=settings.get("ENCODING","utf-8")).select(*cleanup_cols).toLocalIterator():
            for value in row:
                if value:
                    p=resolve_lob_path(value, str(Path(csv_path).parent))
                    if p is not None:
                        try: p.unlink()
                        except OSError as e: logger_obj.warning("Could not delete LOB %s: %s",p,e)
    if settings.get("DELETE_CSV","false").lower()=="true": Path(csv_path).unlink()

    append_record(manifest_path,{"timestamp":now(),"status":"SUCCESS","input":str(csv_path),"table":table,"guid":guid,"output":str(final),"rows":rows,"parquet_file":str(next(final.glob("*.parquet"))),"error":""})
    return final


def _report_failed(report):
    import json
    from pathlib import Path
    try:
        data=json.loads(Path(report["path"]).read_text(encoding="utf-8"))
        for run in reversed(data.get("runs", [])):
            if run.get("run_id")==report["run_id"]:
                return bool(run.get("failed", 0))
    except Exception:
        pass
    return False


def run(settings, base_dir):
    log_dir=Path(settings.get("LOG_DIR",str(base_dir/"logs")))
    status_dir=Path(settings.get("STATUS_DIR",str(base_dir/"status")))
    manifest=status_dir/"conversion_status.csv"
    # Resolve output before creating the application/date-specific logger.
    # The previous build called logger(..., out) before assigning out, which
    # caused UnboundLocalError when main.py was run directly.
    inp=Path(settings["INPUT_PATH"]); out=Path(settings["OUTPUT_PATH"]); dtype=Path(settings["DATATYPE_JSON"])
    log=logger(log_dir, out)
    spark_types=json.loads((base_dir/"config"/"spark_datatypes.json").read_text(encoding="utf-8"))
    out.mkdir(parents=True,exist_ok=True); status_dir.mkdir(parents=True,exist_ok=True)
    if inp.is_file(): files=[inp]
    else: files=sorted([p for p in inp.rglob("*.CSV")]+[p for p in inp.rglob("*.csv")])
    log.info("Found %d CSV file(s)",len(files))
    previous=latest_by_input(manifest)
    report = start_report(out, inp, len(files))
    log.info("APPLICATION: %s", report["app"])
    log.info("RUN REPORT : %s", report["path"])
    log.info("ERROR LOG  : %s", report["error_path"])
    spark=spark_session(settings)
    try:
        for csv in files:
            if previous.get(str(csv),{}).get("status")=="SUCCESS" and Path(previous[str(csv)].get("output","")).exists():
                log.info("SKIP already successful: %s",csv); continue
            schema, table = fetch_schema_and_table(csv)
            # Archive Viewer keeps the normal parquet directory for unqualified
            # CSVs. When a CSV filename is schema-qualified (for example
            # OPTIMSRC130.OPTIM_ORDERS2-...), it creates a sibling schema-specific
            # root: <base>_<schema>/parquet/. This prevents different schemas from
            # mixing tables while preserving the normal path for unqualified files.
            csv_output = out
            if schema:
                parent = out.parent
                csv_output = parent / f"{parent.name}_{schema}" / out.name
            csv_output.mkdir(parents=True, exist_ok=True)
            log.info("SCHEMA OUTPUT: %s", csv_output)
            append_record(manifest,{"timestamp":now(),"status":"RUNNING","input":str(csv),"table":table,"guid":"","output":str(csv_output),"rows":"","parquet_file":"","error":""})
            try:
                final = process_csv(spark,csv,csv_output,dtype,settings,spark_types,log,manifest)
                parquet_files = list(final.glob("*.parquet"))
                rows = 0
                latest = latest_by_input(manifest).get(str(csv), {})
                try:
                    rows = int(latest.get("rows") or 0)
                except (TypeError, ValueError):
                    rows = 0
                add_file_result(report, {
                    "status":"SUCCESS", "csv":str(csv), "schema":schema or "",
                    "table":table, "output":str(final), "rows":rows,
                    "parquet_file":str(parquet_files[0]) if parquet_files else ""
                })
                log.info("FILE SUCCESS: %s | rows=%s | output=%s", table, rows, final)
            except Exception as e:
                log.exception("FAILED: %s",csv)
                error_text = __import__("traceback").format_exc()
                append_record(manifest,{"timestamp":now(),"status":"FAILED","input":str(csv),"table":table,"guid":"","output":"","rows":"","parquet_file":"","error":repr(e)})
                append_error(report, csv, error_text)
                add_file_result(report, {
                    "status":"FAILED", "csv":str(csv), "schema":schema or "",
                    "table":table, "output":"", "rows":0, "error":repr(e)
                })
                log.error("FILE FAILED: %s | error log=%s", table, report["error_path"])
        final_status = "FAILED" if _report_failed(report) else "SUCCESS"
        finish_report(report, final_status)
        log.info("Conversion run finished. Application=%s Status=%s Report=%s", report["app"], final_status, report["path"])
    finally:
        spark.stop()
