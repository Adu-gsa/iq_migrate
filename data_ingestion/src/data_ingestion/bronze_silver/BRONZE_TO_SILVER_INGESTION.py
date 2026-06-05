# =============================================================================
# Module: BRONZE_TO_SILVER_INGESTION.py
# Version: 0.1
# Developed by: Adu Erena
# Date: 2025-06-05
# Description: Bronze to Silver Sequential Ingestion module. Reads Bronze tables
#              (all STRING columns), casts each column to its Silver-layer target
#              type using safe parsing/casting logic, validates data quality, and
#              writes the result to Silver Delta tables.
# =============================================================================
"""
Bronze to Silver Sequential Ingestion
This notebook runs a direct Bronze -> Silver transformation flow using the Silver table datatypes as the target contract.
Ingestion strategy:
1. Execute `SILVER_LAYER_DDL.ipynb` so Silver table definitions are created/refreshed first.
2. Read each Bronze table (Bronze values are currently stored as STRING).
"""


from data_ingestion.env.environment_config import *  # noqa: F403 (was: %run ../src/env/environment_config)
from data_ingestion.env.etl_utils import *  # noqa: F403 (was: %run ../src/env/etl_utils)
from data_ingestion.bronze_silver.SILVER_LAYER_DDL import *  # noqa: F403 (was: %run ./SILVER_LAYER_DDL)




import argparse
import time
from datetime import datetime
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, BooleanType, TimestampType, DateType, DecimalType,
    ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
    StructType, StructField
 )

# Runtime-resolved database objects for Silver processing.
# These are set dynamically in the __main__ block based on job parameters.
catalog = ""
bronze_schema = "bronze"
silver_schema = "silver"
etl_log_schema = "silver"


def _get_spark():
    """Resolve spark in both notebook and wheel/serverless task contexts.

    In notebook context, 'spark' is injected as a global variable.
    In wheel task context, we retrieve the active SparkSession programmatically.
    """
    try:
        return spark
    except NameError:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()


def _get_dbutils():
    """Resolve dbutils in both notebook and wheel/serverless task contexts.

    In notebook context, 'dbutils' is injected as a global variable.
    In wheel task context, we import it from the Databricks SDK runtime.
    """
    try:
        return dbutils
    except NameError:
        from databricks.sdk.runtime import dbutils as _dbutils
        return _dbutils


def _str_to_bool(value: str) -> bool:
    """Convert a string value to a Python boolean.

    Recognizes common truthy representations: '1', 'true', 't', 'yes', 'y'.
    Everything else (including None, empty string) returns False.
    """
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _safe_widget_get(name: str, default: str) -> str:
    """Safely retrieve a Databricks widget value with a fallback default.

    Used to read notebook widgets when running in notebook context.
    Returns the default value if the widget doesn't exist or any error occurs.
    """
    try:
        return _get_dbutils().widgets.get(name).strip()
    except Exception:
        return default


def _parse_runtime_args():
    """Parse command-line arguments for wheel task execution.

    When the module runs as a wheel entry point, Databricks passes named_parameters
    as command-line arguments. This function defines all accepted parameters with
    their defaults and returns the parsed namespace.

    Returns:
        argparse.Namespace with all runtime configuration values
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env", default="")
    parser.add_argument("--catalog", default="")
    parser.add_argument("--bronze_schema", default="bronze")
    parser.add_argument("--silver_schema", default="silver")
    parser.add_argument("--etl_log_schema", default="")
    parser.add_argument("--run_mode", default="all", choices=["single", "all"])
    parser.add_argument("--table_name", default="")
    parser.add_argument("--job_id", default="")
    parser.add_argument("--job_run_id", default="")
    parser.add_argument("--task_id", default="")
    parser.add_argument("--task_run_id", default="")
    parser.add_argument("--fail_on_cast_errors", default="false")
    parser.add_argument("--cast_error_max_rows", type=int, default=0)
    parser.add_argument("--cast_error_max_pct", type=float, default=0.0)
    args, _ = parser.parse_known_args()
    return args

def resolve_tables_for_ingestion(selected_table_name: str = ""):
    """Determine which tables to process in this Silver ingestion run.

    Discovers all tables in the Silver schema (from SILVER_LAYER_DDL definitions),
    excludes internal tables (etl_log, *_dq_quarantine), and filters to only those
    that have a corresponding Bronze source table.

    Args:
        selected_table_name: If provided, restricts ingestion to this single table.
                            If empty, all qualifying tables are returned.

    Returns:
        Sorted list of table names to ingest from Bronze to Silver.

    Raises:
        ValueError: If the requested table doesn't exist or no tables are resolved.
    """
    # Resolve tables directly from Silver schema so ingestion follows SILVER_LAYER_DDL definitions.
    silver_tables = [
        r.tableName for r in _get_spark().sql(f"SHOW TABLES IN `{catalog}`.`{silver_schema}`").collect()
    ]

    # Exclude internal/system tables from the ingestion candidate list
    excluded = {"etl_log"}
    candidate_tables = sorted([t for t in silver_tables if t not in excluded and not t.endswith("_dq_quarantine")])

    # If a specific table was requested, validate it exists in the candidate list
    requested = (selected_table_name or "").strip().lower()
    if requested:
        if requested not in candidate_tables:
            raise ValueError(
                f"Requested table '{requested}' not found in {catalog}.{silver_schema}. "
                f"Available tables: {candidate_tables}"
            )
        candidate_tables = [requested]

    # Only ingest tables that exist in Bronze source schema (skip if Bronze source missing).
    tables = [
        t for t in candidate_tables
        if _get_spark().catalog.tableExists(f"`{catalog}`.`{bronze_schema}`.`{t}`")
    ]

    if not tables:
        raise ValueError(
            f"No Silver tables resolved for ingestion in {catalog}.{silver_schema}. "
            f"Run SILVER_LAYER_DDL.ipynb first."
        )

    return tables

def ensure_etl_log_table_exists(catalog: str, schema: str, env: str):
    """Create the ETL audit log table if it doesn't already exist.

    This is a local definition (guaranteed available regardless of %run state)
    that creates the etl_log Delta table for tracking ingestion audit records.
    """
    # Guaranteed local definition to avoid NameError regardless of %run state.
    full_table_name = f"`{catalog}`.`{schema}`.`etl_log`"
    _get_spark().sql(f"""
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
    """)

def write_etl_log(
    catalog, schema, job_id, job_run_id, task_id, task_run_id,
    start_time, end_time, source_file_path, source_table,
    target_table, success, failure_reason, record_count
 ):
    """Append an audit record to the ETL log table.

    This is a local definition (guaranteed available regardless of %run state)
    that writes a single row documenting the outcome of an ingestion operation.
    """
    # Guaranteed local definition to avoid NameError regardless of %run state.
    full_log_table = f"`{catalog}`.`{schema}`.`etl_log`"
    log_data = [(
        str(job_id), str(job_run_id), str(task_id), str(task_run_id),
        start_time, end_time,
        str(source_file_path) if source_file_path else None,
        str(source_table) if source_table else None,
        str(target_table) if target_table else None,
        int(success),
        str(failure_reason) if failure_reason else None,
        int(record_count) if record_count is not None else 0
    )]

    log_schema = StructType([
        StructField("job_id", StringType(), True),
        StructField("job_run_id", StringType(), True),
        StructField("task_id", StringType(), True),
        StructField("task_run_id", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("source_file_path", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("success", IntegerType(), True),
        StructField("failure_reason", StringType(), True),
        StructField("record_count", LongType(), True),
    ])

    _get_spark().createDataFrame(log_data, schema=log_schema).write.format("delta").mode("append").saveAsTable(full_log_table)

def _empty_to_null(col_expr):
    """Convert empty/blank strings from Bronze into SQL NULL before casting.

    Bronze data is stored as STRING, so empty values appear as '' or whitespace.
    This normalizes them to NULL so downstream casts produce NULL instead of errors.
    """
    # Standardize blank strings from Bronze into NULL before casting.
    as_string = col_expr.cast("string")
    return F.when(F.trim(as_string) == "", F.lit(None)).otherwise(as_string)

def _ts_parse_safe(col_expr, fmt: str):
    """Attempt to parse a timestamp using try_to_timestamp (if available) for safety.

    try_to_timestamp returns NULL on parse failure instead of raising an error.
    Falls back to to_timestamp on older runtimes where try_to_timestamp is unavailable.
    """
    # Use try_to_timestamp when available to avoid hard failures on malformed values.
    if hasattr(F, "try_to_timestamp"):
        return F.try_to_timestamp(col_expr, F.lit(fmt))
    return F.to_timestamp(col_expr, fmt)

def _safe_numeric_cast(cleaned_col, target_type):
    """Cast a cleaned string column to a numeric type with regex-gated safety.

    Handles common formatting issues in source data:
    - Removes commas (thousands separator)
    - Removes dollar signs
    - Converts parenthesized negatives: (123) → -123
    - Returns NULL for values that don't match expected numeric patterns
    """
    # Apply regex-gated numeric casting so malformed strings become NULL (instead of raising CAST_INVALID_INPUT).
    numeric = F.regexp_replace(cleaned_col, ",", "")
    numeric = F.regexp_replace(numeric, "\\$", "")
    numeric = F.regexp_replace(numeric, r"^\((.*)\\)$", r"-$1")
    numeric = F.trim(numeric)

    # Regex patterns for validation before casting
    int_pattern = r"^[+-]?\d+$"
    whole_decimal_pattern = r"^[+-]?\d+\.0+$"
    dec_pattern = r"^[+-]?(\d+\.?\d*|\.\d+)$"

    if isinstance(target_type, (ByteType, ShortType, IntegerType, LongType)):
        # Accept integer-like decimals such as 123.0 for integer targets.
        return (
            F.when(numeric.rlike(int_pattern), numeric.cast(target_type))
             .when(numeric.rlike(whole_decimal_pattern), numeric.cast("decimal(38,10)").cast(target_type))
             .otherwise(F.lit(None).cast(target_type))
        )

    if isinstance(target_type, (FloatType, DoubleType, DecimalType)):
        # For floating-point/decimal targets, validate against decimal pattern
        return F.when(numeric.rlike(dec_pattern), numeric.cast(target_type)).otherwise(F.lit(None).cast(target_type))

    return numeric.cast(target_type)

def _safe_boolean_cast(cleaned_col):
    """Map common boolean encodings to Spark BooleanType safely.

    Handles various truthy/falsy representations found in Bronze source data,
    including decimal-like flags (1.0/0.0) and text representations.
    Returns NULL for values that don't match any recognized pattern.
    """
    # Map common boolean encodings, including decimal-like flags 1.0/0.0.
    v = F.lower(F.trim(cleaned_col))
    return (
        F.when(v.isin("1", "1.0", "true", "t", "y", "yes"), F.lit(True))
         .when(v.isin("0", "0.0", "false", "f", "n", "no"), F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )

def _safe_date_parse(cleaned_col):
    """Parse DATE values safely from both date-only and datetime-like Bronze strings.

    Tries multiple date format patterns commonly found in FAS Advantage source data.
    Returns NULL if none of the known patterns match.
    """
    # Parse DATE safely from both date-only and datetime-like Bronze strings.
    d_clean = F.regexp_replace(cleaned_col, r"\s+", " ")
    # First try parsing as timestamp and extracting the date portion
    ts_candidate = F.coalesce(
        _ts_parse_safe(d_clean, "MMM dd yyyy h:mm:ss:SSSa"),
        _ts_parse_safe(d_clean, "MMM d yyyy h:mm:ss:SSSa"),
        _ts_parse_safe(d_clean, "MMM dd yyyy hh:mm:ss:SSSa"),
        _ts_parse_safe(d_clean, "MMM d yyyy hh:mm:ss:SSSa"),
        _ts_parse_safe(d_clean, "yyyy-MM-dd HH:mm:ss"),
        _ts_parse_safe(d_clean, "yyyy-MM-dd HH:mm"),
        _ts_parse_safe(d_clean, "MM/dd/yyyy HH:mm:ss"),
        _ts_parse_safe(d_clean, "MM/dd/yyyy HH:mm"),
        _ts_parse_safe(d_clean, "yyyy-MM-dd'T'HH:mm:ss"),
        _ts_parse_safe(d_clean, "yyyy-MM-dd'T'HH:mm:ss.SSS")
    )

    # Then try direct date-only patterns as fallback
    return F.coalesce(
        F.to_date(ts_candidate),
        F.to_date(d_clean, "yyyy-MM-dd"),
        F.to_date(d_clean, "MM/dd/yyyy"),
        F.to_date(d_clean, "MM-dd-yyyy"),
        F.to_date(d_clean, "yyyyMMdd"),
        F.to_date(d_clean, "MMM dd yyyy"),
        F.to_date(d_clean, "MMM d yyyy")
    )

def _cast_expr(source_column_name, target_type):
    """Build the appropriate Spark column expression to cast a Bronze STRING column to its Silver target type.

    This is the core casting dispatcher that routes each column to the appropriate
    safe-casting function based on the target Silver datatype. All Bronze values
    start as STRING, so every non-string target requires explicit transformation.

    Args:
        source_column_name: Name of the column in the Bronze DataFrame
        target_type:        PySpark DataType instance from the Silver schema

    Returns:
        Spark Column expression that safely casts the source to the target type
    """
    # Bronze values are strings; any non-string Silver target type must be explicitly transformed/cast.
    source_col = F.col(source_column_name)
    cleaned = _empty_to_null(source_col)

    if isinstance(target_type, StringType):
        # String-to-string: just trim whitespace
        return F.trim(cleaned).cast("string")

    if isinstance(target_type, BooleanType):
        # Restrict to explicit boolean encodings to avoid ANSI cast failures on arbitrary strings.
        return _safe_boolean_cast(cleaned)

    if isinstance(target_type, (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)):
        # Numeric types: use regex-gated safe casting
        return _safe_numeric_cast(cleaned, target_type)

    if isinstance(target_type, TimestampType):
        # Normalize repeated spaces, then try known timestamp patterns from Bronze text files.
        ts_clean = F.regexp_replace(cleaned, r"\s+", " ")
        return F.coalesce(
            _ts_parse_safe(ts_clean, "MMM dd yyyy h:mm:ss:SSSa"),
            _ts_parse_safe(ts_clean, "MMM d yyyy h:mm:ss:SSSa"),
            _ts_parse_safe(ts_clean, "MMM dd yyyy hh:mm:ss:SSSa"),
            _ts_parse_safe(ts_clean, "MMM d yyyy hh:mm:ss:SSSa"),
            _ts_parse_safe(ts_clean, "yyyy-MM-dd HH:mm:ss"),
            _ts_parse_safe(ts_clean, "yyyy-MM-dd HH:mm"),
            _ts_parse_safe(ts_clean, "MM/dd/yyyy HH:mm:ss"),
            _ts_parse_safe(ts_clean, "MM/dd/yyyy HH:mm"),
            _ts_parse_safe(ts_clean, "yyyy-MM-dd'T'HH:mm:ss"),
            _ts_parse_safe(ts_clean, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            _ts_parse_safe(ts_clean, "yyyy-MM-dd")
        )

    if isinstance(target_type, DateType):
        # Parse date safely from both date-only and datetime-like values.
        return _safe_date_parse(cleaned)

    # Fallback for any additional Silver datatypes: cast from Bronze string value to target datatype.
    return cleaned.cast(target_type)

def cast_to_silver_schema(source_df, silver_table_name):
    """Transform a Bronze DataFrame to match the Silver table schema with data quality tracking.

    This function is the main Bronze-to-Silver transformation engine:
    1. Reads the Silver table's schema (from SILVER_LAYER_DDL) as the target contract
    2. For each Silver column, builds a safe cast expression from the Bronze source
    3. Tracks data quality by detecting rows where non-empty values failed to cast
    4. Returns both the transformed DataFrame and aggregated DQ metrics

    Args:
        source_df:        Bronze DataFrame (all STRING columns)
        silver_table_name: Fully qualified Silver table name (used to read target schema)

    Returns:
        Tuple of (silver_df, dq_metrics):
        - silver_df: DataFrame with columns cast to Silver types
        - dq_metrics: Dict with total_rows, cast_error_rows, and per-column failure counts
    """
    # Build transformed Silver dataframe and cast quality metrics using Silver DDL as the datatype contract.
    target_schema = _get_spark().table(silver_table_name).schema
    # Create case-insensitive lookup for matching Bronze columns to Silver fields
    source_lookup = {c.lower(): c for c in source_df.columns}
    projected_columns = []
    dq_checks = []

    for field in target_schema.fields:
        source_col = source_lookup.get(field.name.lower())
        if source_col:
            # Column exists in Bronze — build the safe cast expression
            cast_expr = _cast_expr(source_col, field.dataType)
            projected_columns.append(cast_expr.alias(field.name))

            # Flag rows where a non-empty source value failed to cast for non-string targets.
            if not isinstance(field.dataType, StringType):
                cleaned = _empty_to_null(F.col(source_col))
                dq_checks.append((field.name, cleaned.isNotNull() & cast_expr.isNull()))
        else:
            # Column missing in Bronze — fill with NULL of the target type
            projected_columns.append(F.lit(None).cast(field.dataType).alias(field.name))
            if not field.nullable:
                # Non-nullable field missing from source is always a DQ issue
                dq_checks.append((field.name, F.lit(True)))

    # Apply all column projections to produce the Silver DataFrame
    silver_df = source_df.select(*projected_columns)

    # Build a combined row-level failure condition (any column cast failed = row failure)
    if dq_checks:
        row_fail_cond = reduce(lambda a, b: a | b, [cond for _, cond in dq_checks])
    else:
        row_fail_cond = F.lit(False)

    # Compute aggregate DQ metrics: total rows, total cast errors, per-column failures
    dq_agg_exprs = [
        F.count("*").alias("total_rows"),
        F.sum(F.when(row_fail_cond, 1).otherwise(0)).alias("cast_error_rows")
    ]

    for col_name, cond in dq_checks:
        dq_agg_exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"{col_name}__cast_fail"))

    # Collect DQ metrics by scanning the source DataFrame once
    dq_metrics = source_df.select(*dq_agg_exprs).collect()[0].asDict()
    return silver_df, dq_metrics



if __name__ == '__main__':
    # ---- Widget / Parameter Setup ----
    # Define notebook widgets for interactive use; silently skip if running as wheel task
    try:
        dbutils.widgets.text("env", "test", "Environment")
        dbutils.widgets.text("job_id", "", "Databricks Job ID")
        dbutils.widgets.text("job_run_id", "", "Databricks Job Run ID")
        dbutils.widgets.text("task_id", "", "Databricks Task ID")
        dbutils.widgets.text("task_run_id", "", "Databricks Task Run ID")
        dbutils.widgets.dropdown("run_mode", "all", ["single", "all"], "Run Mode")
        dbutils.widgets.text("table_name", "", "Table Name (single mode)")
        dbutils.widgets.dropdown("fail_on_cast_errors", "false", ["true", "false"], "Fail On Cast Errors")
        dbutils.widgets.text("cast_error_max_rows", "0", "Cast Error Max Rows")
        dbutils.widgets.text("cast_error_max_pct", "0", "Cast Error Max Percent")
    except Exception:
        pass

    # Parse runtime arguments (from wheel named_parameters or command line)
    args = _parse_runtime_args()

    # Resolve environment and catalog from args or widget fallbacks
    env = EnvironmentConfig.get_environment(args.env or _safe_widget_get("env", "test"))

    catalog = (args.catalog or EnvironmentConfig.get_catalog(env)).strip()
    bronze_schema = (args.bronze_schema or "bronze").strip()
    silver_schema = (args.silver_schema or "silver").strip()
    etl_log_schema = (args.etl_log_schema or silver_schema).strip()

    # Determine run mode: 'single' processes one table, 'all' processes every available table
    run_mode = (args.run_mode or _safe_widget_get("run_mode", "all")).strip().lower()
    selected_table_name = (args.table_name or _safe_widget_get("table_name", "")).strip().lower()
    if run_mode == "single" and not selected_table_name:
        raise ValueError("table_name is required when run_mode=single")

    # Collect job/task identifiers for ETL audit logging
    job_ids = {
        "job_id": args.job_id or _safe_widget_get("job_id", ""),
        "job_run_id": args.job_run_id or _safe_widget_get("job_run_id", ""),
        "task_id": args.task_id or _safe_widget_get("task_id", ""),
        "task_run_id": args.task_run_id or _safe_widget_get("task_run_id", ""),
    }

    # Data quality threshold configuration
    fail_on_cast_errors = _str_to_bool(args.fail_on_cast_errors or _safe_widget_get("fail_on_cast_errors", "false"))
    cast_error_max_rows = int(args.cast_error_max_rows)
    cast_error_max_pct = float(args.cast_error_max_pct)

    # Resolve which tables to process based on run_mode
    tables = resolve_tables_for_ingestion(selected_table_name if run_mode == "single" else "")

    print(
        f"[INFO] env={env} catalog={catalog} source_schema={bronze_schema} target_schema={silver_schema} "
        f"run_mode={run_mode} "
        f"fail_on_cast_errors={fail_on_cast_errors} cast_error_max_rows={cast_error_max_rows} "
        f"cast_error_max_pct={cast_error_max_pct} tables={len(tables)}"
    )

    # Ensure ETL log table exists in Silver schema.
    ensure_etl_log_table_exists(catalog, etl_log_schema, env=env)

    pipeline_start = time.perf_counter()

    # ---- Main Processing Loop ----
    # Iterate over each resolved table and perform Bronze → Silver transformation
    for table_name in tables:
        table_start = time.perf_counter()
        bronze_table = f"`{catalog}`.`{bronze_schema}`.`{table_name}`"
        silver_table = f"`{catalog}`.`{silver_schema}`.`{table_name}`"

        start_time = datetime.now()
        success = 0
        failure_reason = None
        record_count = 0

        print(f"\n[START] {table_name}")

        try:
            # Read the Bronze source table (all columns are STRING)
            bronze_df = _get_spark().table(bronze_table)
            # Cast to Silver schema and collect data quality metrics
            silver_df, dq_metrics = cast_to_silver_schema(bronze_df, silver_table)

            src_count = int(dq_metrics.get("total_rows") or 0)
            cast_error_rows = int(dq_metrics.get("cast_error_rows") or 0)
            cast_error_pct = (cast_error_rows / src_count * 100.0) if src_count > 0 else 0.0

            # Extract per-column failure counts for reporting
            column_failures = {
                k.replace("__cast_fail", ""): int(v or 0)
                for k, v in dq_metrics.items()
                if k.endswith("__cast_fail") and int(v or 0) > 0
            }

            # Report and optionally fail on data quality issues
            if cast_error_rows > 0:
                top_cols = sorted(column_failures.items(), key=lambda x: x[1], reverse=True)[:10]
                message = (
                    f"Cast validation issue for {table_name}: "
                    f"cast_error_rows={cast_error_rows} ({cast_error_pct:.4f}%), "
                    f"top_columns={top_cols}"
                )
                print(f"[WARN] {message}")

                # Check if error thresholds are exceeded
                threshold_exceeded = (
                    (cast_error_max_rows > 0 and cast_error_rows > cast_error_max_rows)
                    or (cast_error_max_pct > 0 and cast_error_pct > cast_error_max_pct)
                    or ((cast_error_max_rows == 0 and cast_error_max_pct == 0) and fail_on_cast_errors)
                )
                if fail_on_cast_errors and threshold_exceeded:
                    raise ValueError(message)

            # Direct Bronze -> Silver write with no staging/quarantine tables.
            (
                silver_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(silver_table)
            )

            record_count = src_count
            success = 1
            print(
                f"[DONE] {table_name} | bronze_rows={src_count:,} | silver_rows={record_count:,} "
                f"| elapsed_s={time.perf_counter() - table_start:.2f}"
            )

        except Exception as e:
            failure_reason = str(e)
            print(f"[FAILURE] {table_name} | elapsed_s={time.perf_counter() - table_start:.2f} | {failure_reason}")
            raise

        finally:
            # Always write an ETL log entry regardless of success or failure
            write_etl_log(
                catalog, silver_schema,
                job_ids["job_id"], job_ids["job_run_id"],
                job_ids["task_id"], job_ids["task_run_id"],
                start_time, datetime.now(),
                source_file_path=None,
                source_table=bronze_table,
                target_table=silver_table,
                success=success,
                failure_reason=failure_reason,
                record_count=record_count
            )

    print(f"\n[COMPLETE] Bronze-to-Silver direct ingestion finished for all tables in {time.perf_counter() - pipeline_start:.2f}s.")
