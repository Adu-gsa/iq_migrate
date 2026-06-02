# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Sequential Ingestion
# MAGIC
# MAGIC This notebook runs a direct Bronze -> Silver transformation flow using the Silver table datatypes as the target contract.
# MAGIC
# MAGIC Ingestion strategy:
# MAGIC 1. Execute `SILVER_LAYER_DDL.ipynb` so Silver table definitions are created/refreshed first.
# MAGIC 2. Read each Bronze table (Bronze values are currently stored as STRING).
# MAGIC 3. Cast columns to Silver target datatypes (INT, DECIMAL, TIMESTAMP, DOUBLE, TINYINT, BOOLEAN, DATE, etc.).
# MAGIC 4. Validate cast quality and detect rows where a non-empty source value became NULL after casting.
# MAGIC 5. Write transformed data directly to `foia_tst.silver.<table>` (no staging table, no quarantine table).
# MAGIC 6. Log success/failure and row counts into `foia_tst.silver.etl_log`.

# COMMAND ----------

# MAGIC %run ../src/env/environment_config

# COMMAND ----------

# MAGIC %run ../src/env/etl_utils

# COMMAND ----------

# MAGIC %run ./SILVER_LAYER_DDL

# COMMAND ----------

from datetime import datetime
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, BooleanType, TimestampType, DateType, DecimalType,
    ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
    StructType, StructField
 )

# Project-required database objects for Silver processing.
catalog = "foia_tst"
bronze_schema = "bronze"
silver_schema = "silver"
etl_log_schema = silver_schema  # Silver jobs must log to foia_tst.silver.etl_log

dbutils.widgets.text("job_id", "", "Databricks Job ID")
dbutils.widgets.text("job_run_id", "", "Databricks Job Run ID")
dbutils.widgets.text("task_id", "", "Databricks Task ID")
dbutils.widgets.text("task_run_id", "", "Databricks Task Run ID")
dbutils.widgets.dropdown("fail_on_cast_errors", "false", ["true", "false"], "Fail On Cast Errors")
dbutils.widgets.text("cast_error_max_rows", "0", "Cast Error Max Rows")
dbutils.widgets.text("cast_error_max_pct", "0", "Cast Error Max Percent")

job_ids = {
    "job_id": dbutils.widgets.get("job_id").strip(),
    "job_run_id": dbutils.widgets.get("job_run_id").strip(),
    "task_id": dbutils.widgets.get("task_id").strip(),
    "task_run_id": dbutils.widgets.get("task_run_id").strip(),
}

fail_on_cast_errors = dbutils.widgets.get("fail_on_cast_errors").strip().lower() == "true"
cast_error_max_rows = int(dbutils.widgets.get("cast_error_max_rows").strip() or "0")
cast_error_max_pct = float(dbutils.widgets.get("cast_error_max_pct").strip() or "0")

def resolve_tables_for_ingestion():
    # Resolve tables directly from Silver schema so ingestion follows SILVER_LAYER_DDL definitions.
    silver_tables = [
        r.tableName for r in spark.sql(f"SHOW TABLES IN `{catalog}`.`{silver_schema}`").collect()
    ]

    excluded = {"etl_log"}
    candidate_tables = sorted([t for t in silver_tables if t not in excluded and not t.endswith("_dq_quarantine")])

    # Only ingest tables that exist in Bronze source schema.
    tables = [
        t for t in candidate_tables
        if spark.catalog.tableExists(f"`{catalog}`.`{bronze_schema}`.`{t}`")
    ]

    if not tables:
        raise ValueError(
            f"No Silver tables resolved for ingestion in {catalog}.{silver_schema}. "
            f"Run SILVER_LAYER_DDL.ipynb first."
        )

    return tables

def ensure_etl_log_table_exists(catalog: str, schema: str, env: str):
    # Guaranteed local definition to avoid NameError regardless of %run state.
    full_table_name = f"`{catalog}`.`{schema}`.`etl_log`"
    spark.sql(f"""
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

    spark.createDataFrame(log_data, schema=log_schema).write.format("delta").mode("append").saveAsTable(full_log_table)

tables = resolve_tables_for_ingestion()

print(
    f"[INFO] catalog={catalog} source_schema={bronze_schema} target_schema={silver_schema} "
    f"fail_on_cast_errors={fail_on_cast_errors} cast_error_max_rows={cast_error_max_rows} "
    f"cast_error_max_pct={cast_error_max_pct} tables={len(tables)}"
)

# Ensure ETL log table exists in Silver schema.
ensure_etl_log_table_exists(catalog, etl_log_schema, env="test")

# COMMAND ----------

def _empty_to_null(col_expr):
    # Standardize blank strings from Bronze into NULL before casting.
    as_string = col_expr.cast("string")
    return F.when(F.trim(as_string) == "", F.lit(None)).otherwise(as_string)

def _ts_parse_safe(col_expr, fmt: str):
    # Use try_to_timestamp when available to avoid hard failures on malformed values.
    if hasattr(F, "try_to_timestamp"):
        return F.try_to_timestamp(col_expr, F.lit(fmt))
    return F.to_timestamp(col_expr, fmt)

def _safe_numeric_cast(cleaned_col, target_type):
    # Apply regex-gated numeric casting so malformed strings become NULL (instead of raising CAST_INVALID_INPUT).
    numeric = F.regexp_replace(cleaned_col, ",", "")
    numeric = F.regexp_replace(numeric, "\\$", "")
    numeric = F.regexp_replace(numeric, r"^\((.*)\)$", r"-$1")
    numeric = F.trim(numeric)

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
        return F.when(numeric.rlike(dec_pattern), numeric.cast(target_type)).otherwise(F.lit(None).cast(target_type))

    return numeric.cast(target_type)

def _safe_boolean_cast(cleaned_col):
    # Map common boolean encodings, including decimal-like flags 1.0/0.0.
    v = F.lower(F.trim(cleaned_col))
    return (
        F.when(v.isin("1", "1.0", "true", "t", "y", "yes"), F.lit(True))
         .when(v.isin("0", "0.0", "false", "f", "n", "no"), F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )

def _safe_date_parse(cleaned_col):
    # Parse DATE safely from both date-only and datetime-like Bronze strings.
    d_clean = F.regexp_replace(cleaned_col, r"\s+", " ")
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
    # Bronze values are strings; any non-string Silver target type must be explicitly transformed/cast.
    source_col = F.col(source_column_name)
    cleaned = _empty_to_null(source_col)

    if isinstance(target_type, StringType):
        return F.trim(cleaned).cast("string")

    if isinstance(target_type, BooleanType):
        # Restrict to explicit boolean encodings to avoid ANSI cast failures on arbitrary strings.
        return _safe_boolean_cast(cleaned)

    if isinstance(target_type, (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)):
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
    # Build transformed Silver dataframe and cast quality metrics using Silver DDL as the datatype contract.
    target_schema = spark.table(silver_table_name).schema
    source_lookup = {c.lower(): c for c in source_df.columns}
    projected_columns = []
    dq_checks = []

    for field in target_schema.fields:
        source_col = source_lookup.get(field.name.lower())
        if source_col:
            cast_expr = _cast_expr(source_col, field.dataType)
            projected_columns.append(cast_expr.alias(field.name))

            # Flag rows where a non-empty source value failed to cast for non-string targets.
            if not isinstance(field.dataType, StringType):
                cleaned = _empty_to_null(F.col(source_col))
                dq_checks.append((field.name, cleaned.isNotNull() & cast_expr.isNull()))
        else:
            projected_columns.append(F.lit(None).cast(field.dataType).alias(field.name))
            if not field.nullable:
                dq_checks.append((field.name, F.lit(True)))

    silver_df = source_df.select(*projected_columns)

    if dq_checks:
        row_fail_cond = reduce(lambda a, b: a | b, [cond for _, cond in dq_checks])
    else:
        row_fail_cond = F.lit(False)

    dq_agg_exprs = [
        F.count("*").alias("total_rows"),
        F.sum(F.when(row_fail_cond, 1).otherwise(0)).alias("cast_error_rows")
    ]

    for col_name, cond in dq_checks:
        dq_agg_exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"{col_name}__cast_fail"))

    dq_metrics = source_df.select(*dq_agg_exprs).collect()[0].asDict()
    return silver_df, dq_metrics

# COMMAND ----------

for table_name in tables:
    bronze_table = f"`{catalog}`.`{bronze_schema}`.`{table_name}`"
    silver_table = f"`{catalog}`.`{silver_schema}`.`{table_name}`"

    start_time = datetime.now()
    success = 0
    failure_reason = None
    record_count = 0

    print(f"\n[START] {table_name}")

    try:
        bronze_df = spark.table(bronze_table)
        silver_df, dq_metrics = cast_to_silver_schema(bronze_df, silver_table)

        src_count = int(dq_metrics.get("total_rows") or 0)
        cast_error_rows = int(dq_metrics.get("cast_error_rows") or 0)
        cast_error_pct = (cast_error_rows / src_count * 100.0) if src_count > 0 else 0.0

        column_failures = {
            k.replace("__cast_fail", ""): int(v or 0)
            for k, v in dq_metrics.items()
            if k.endswith("__cast_fail") and int(v or 0) > 0
        }

        if cast_error_rows > 0:
            top_cols = sorted(column_failures.items(), key=lambda x: x[1], reverse=True)[:10]
            message = (
                f"Cast validation issue for {table_name}: "
                f"cast_error_rows={cast_error_rows} ({cast_error_pct:.4f}%), "
                f"top_columns={top_cols}"
            )
            print(f"[WARN] {message}")

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
        print(f"[DONE] {table_name} | bronze_rows={src_count:,} | silver_rows={record_count:,}")

    except Exception as e:
        failure_reason = str(e)
        print(f"[FAILURE] {table_name} | {failure_reason}")
        raise

    finally:
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

print("\n[COMPLETE] Bronze-to-Silver direct ingestion finished for all tables.")