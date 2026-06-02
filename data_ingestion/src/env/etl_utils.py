"""
ETL Utilities — `etl_utils`
**Description:** Centralized ETL utility library shared across all ingestion pipelines. Contains reusable functions for:
- Execution logging via `@log_execution` decorator
- ETL audit log table management (DDL + DML)
- Post-ingestion file archiving (date-partitioned)
"""




import time
import traceback
from datetime import datetime
from functools import wraps

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType, LongType
)

print("[INFO] etl_utils: library imports loaded successfully.")


def log_execution(func):
    """Decorator: logs start/end time, elapsed duration, and full traceback on failure."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        func_name = func.__name__
        try:
            print("---------------------------------------------")
            print(f"[START] Executing '{func_name}' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            result = func(*args, **kwargs)
            elapsed = time.time() - start
            print(f"[END]   '{func_name}' completed in {elapsed:.4f}s")
            print("---------------------------------------------")
            return result
        except Exception as e:
            elapsed = time.time() - start
            print("---------------------------------------------")
            print(f"[ERROR] '{func_name}' FAILED after {elapsed:.4f}s")
            print(f"        {type(e).__name__}: {e}")
            print(f"        Traceback:\n{traceback.format_exc()}")
            print("---------------------------------------------")
            raise
    return wrapper

print("[INFO] etl_utils: log_execution decorator defined.")


@log_execution
def ensure_etl_log_table_exists(catalog: str, schema: str, env: str):
    """Creates the ETL audit log Delta table if it does not exist (idempotent)."""

    full_table_name = f"`{catalog}`.`{schema}`.`etl_log`"
    print(f"[INFO] Ensuring ETL log table exists: {full_table_name}  env={env}")

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            job_id            STRING,
            job_run_id        STRING,
            task_id           STRING,
            task_run_id       STRING,
            start_time        TIMESTAMP,
            end_time          TIMESTAMP,
            source_file_path  STRING,
            source_table      STRING,
            target_table      STRING,
            success           INT,
            failure_reason    STRING,
            record_count      LONG
        )
        USING DELTA
    """

    spark.sql(ddl)
    print(f"[INFO] ETL log table ready: {full_table_name}")


@log_execution
def write_etl_log(
    catalog, schema, job_id, job_run_id, task_id, task_run_id,
    start_time, end_time, source_file_path, source_table,
    target_table, success, failure_reason, record_count
):
    """Appends one audit row to the ETL log Delta table."""

    full_log_table = f"`{catalog}`.`{schema}`.`etl_log`"
    print(f"[INFO] Writing ETL log to {full_log_table} | success={success} records={record_count}")

    log_data = [(
        str(job_id), str(job_run_id), str(task_id), str(task_run_id),
        start_time, end_time, str(source_file_path),
        str(source_table) if source_table else None,
        str(target_table), int(success),
        str(failure_reason) if failure_reason else None,
        int(record_count)
    )]

    log_schema = StructType([
        StructField("job_id",           StringType(),    True),
        StructField("job_run_id",       StringType(),    True),
        StructField("task_id",          StringType(),    True),
        StructField("task_run_id",      StringType(),    True),
        StructField("start_time",       TimestampType(), True),
        StructField("end_time",         TimestampType(), True),
        StructField("source_file_path", StringType(),    True),
        StructField("source_table",     StringType(),    True),
        StructField("target_table",     StringType(),    True),
        StructField("success",          IntegerType(),   True),
        StructField("failure_reason",   StringType(),    True),
        StructField("record_count",     LongType(),      True),
    ])

    log_df = spark.createDataFrame(log_data, schema=log_schema)
    log_df.write.format("delta").mode("append").saveAsTable(full_log_table)
    print("[INFO] ETL log entry written.")


@log_execution
def archive_file(source_file_path: str, archive_base_path: str, env: str):
    """Moves an ingested source file to a dated archive subfolder."""

    try:
        file_name = source_file_path.split("/")[-1]
        date_partition = datetime.now().strftime("%Y-%m-%d")
        archive_dest_path = f"{archive_base_path}/{env}/{date_partition}/{file_name}"

        print(f"[INFO] Archiving: {source_file_path} -> {archive_dest_path}")
        dbutils.fs.mv(source_file_path, archive_dest_path, recurse=False)
        print(f"[INFO] Archived to: {archive_dest_path}")
        return archive_dest_path

    except Exception as e:
        error_msg = f"Failed to archive. Source: '{source_file_path}' | Error: {e}"
        print(f"[ERROR] {error_msg}")
        raise Exception(error_msg) from e


print("[INFO] etl_utils: all functions loaded.")
print("[INFO] Available: log_execution | ensure_etl_log_table_exists | write_etl_log | archive_file")
