# =============================================================================
# Module: etl_utils.py
# Version: 0.1
# Developed by: Adu Erena
# Date: 2025-06-05
# Description: Centralized ETL utility library shared across all ingestion
#              pipelines. Contains reusable functions for:
#              - Execution logging via @log_execution decorator
#              - ETL audit log table management (DDL + DML)
#              - Post-ingestion file archiving (date-partitioned)
# =============================================================================
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
    """Decorator: logs start/end time, elapsed duration, and full traceback on failure.

    Usage:
        @log_execution
        def my_function(...):
            ...

    This decorator wraps any function to automatically:
    - Print a START banner with the function name and current timestamp
    - Execute the wrapped function and capture its return value
    - Print an END banner with elapsed time on success
    - Print an ERROR banner with exception details and full traceback on failure
    - Re-raise the original exception so upstream callers can handle it
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Record the start time to calculate elapsed duration later
        start = time.time()
        func_name = func.__name__
        try:
            print("---------------------------------------------")
            print(f"[START] Executing '{func_name}' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            # Call the actual function being decorated
            result = func(*args, **kwargs)
            # Calculate how long the function took to run
            elapsed = time.time() - start
            print(f"[END]   '{func_name}' completed in {elapsed:.4f}s")
            print("---------------------------------------------")
            return result
        except Exception as e:
            # On failure, still report the elapsed time and full error details
            elapsed = time.time() - start
            print("---------------------------------------------")
            print(f"[ERROR] '{func_name}' FAILED after {elapsed:.4f}s")
            print(f"        {type(e).__name__}: {e}")
            print(f"        Traceback:\n{traceback.format_exc()}")
            print("---------------------------------------------")
            # Re-raise so the calling code can catch or propagate the error
            raise
    return wrapper

print("[INFO] etl_utils: log_execution decorator defined.")


@log_execution
def ensure_etl_log_table_exists(catalog: str, schema: str, env: str):
    """Creates the ETL audit log Delta table if it does not exist (idempotent).

    This function runs a CREATE TABLE IF NOT EXISTS DDL statement to guarantee
    the etl_log table is available before any ingestion task attempts to write
    audit records. It is safe to call multiple times — subsequent calls are no-ops.

    Args:
        catalog: Unity Catalog name (e.g., 'foia_tst')
        schema:  Schema/database name where the log table lives (e.g., 'bronze')
        env:     Environment identifier (dev/test/prod) — used for logging only
    """

    # Build the fully qualified three-part table name with backtick quoting
    full_table_name = f"`{catalog}`.`{schema}`.`etl_log`"
    print(f"[INFO] Ensuring ETL log table exists: {full_table_name}  env={env}")

    # DDL defines all columns needed to track each ingestion run's audit trail
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

    # Execute the DDL via the active SparkSession
    spark.sql(ddl)
    print(f"[INFO] ETL log table ready: {full_table_name}")


@log_execution
def write_etl_log(
    catalog, schema, job_id, job_run_id, task_id, task_run_id,
    start_time, end_time, source_file_path, source_table,
    target_table, success, failure_reason, record_count
):
    """Appends one audit row to the ETL log Delta table.

    Called at the end of every ingestion task (both on success and failure)
    to maintain a full audit trail of all pipeline activity.

    Args:
        catalog:          Unity Catalog name
        schema:           Schema where etl_log lives
        job_id:           Databricks job ID (from {{job.id}})
        job_run_id:       Databricks job run ID (from {{job.run_id}})
        task_id:          Databricks task name (from {{task.name}})
        task_run_id:      Databricks task run ID (from {{task.run_id}})
        start_time:       datetime when the ingestion started
        end_time:         datetime when the ingestion ended
        source_file_path: Path to the source file that was ingested (or None)
        source_table:     Source table name for Bronze-to-Silver flows (or None)
        target_table:     Fully qualified target table name
        success:          1 if ingestion succeeded, 0 if it failed
        failure_reason:   Error message string on failure (or None on success)
        record_count:     Number of records written to the target table
    """

    # Build the fully qualified ETL log table name
    full_log_table = f"`{catalog}`.`{schema}`.`etl_log`"
    print(f"[INFO] Writing ETL log to {full_log_table} | success={success} records={record_count}")

    # Construct a single-row tuple with all audit fields, converting to appropriate types
    log_data = [(
        str(job_id), str(job_run_id), str(task_id), str(task_run_id),
        start_time, end_time, str(source_file_path),
        str(source_table) if source_table else None,
        str(target_table), int(success),
        str(failure_reason) if failure_reason else None,
        int(record_count)
    )]

    # Define the schema explicitly to match the etl_log table DDL
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

    # Create a DataFrame from the single audit row and append it to the log table
    log_df = spark.createDataFrame(log_data, schema=log_schema)
    log_df.write.format("delta").mode("append").saveAsTable(full_log_table)
    print("[INFO] ETL log entry written.")


@log_execution
def archive_file(source_file_path: str, archive_base_path: str, env: str):
    """Moves an ingested source file to a dated archive subfolder.

    After a source file is successfully ingested into the Bronze layer,
    this function relocates it to a date-partitioned archive directory
    to prevent re-ingestion and maintain an organized file history.

    Archive path pattern:
        {archive_base_path}/{env}/{YYYY-MM-DD}/{original_filename}

    Args:
        source_file_path:  Full path to the source file that was just ingested
        archive_base_path: Root archive directory (e.g., OUTBOUND_ROOT path)
        env:               Environment identifier used as a subfolder
    """

    try:
        # Extract just the filename from the full source path
        file_name = source_file_path.split("/")[-1]
        # Create a date-based partition folder using today's date
        date_partition = datetime.now().strftime("%Y-%m-%d")
        # Construct the full destination path: base/env/date/filename
        archive_dest_path = f"{archive_base_path}/{env}/{date_partition}/{file_name}"

        print(f"[INFO] Archiving: {source_file_path} -> {archive_dest_path}")
        # Use dbutils.fs.mv to atomically move the file to the archive location
        dbutils.fs.mv(source_file_path, archive_dest_path, recurse=False)
        print(f"[INFO] Archived to: {archive_dest_path}")
        return archive_dest_path

    except Exception as e:
        # Wrap the error with context about which file failed to archive
        error_msg = f"Failed to archive. Source: '{source_file_path}' | Error: {e}"
        print(f"[ERROR] {error_msg}")
        raise Exception(error_msg) from e


print("[INFO] etl_utils: all functions loaded.")
print("[INFO] Available: log_execution | ensure_etl_log_table_exists | write_etl_log | archive_file")
