# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Log Table DDL
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Developed by** | Adu Erena |
# MAGIC | **Date** | 2026-05-11 |
# MAGIC | **Version** | 1.0 |
# MAGIC
# MAGIC **Description:** Creates the `etl_log` Delta table used for audit logging across all Bronze ingestion pipelines. Each ingestion run appends a row tracking job metadata, timing, record counts, and success/failure status.
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | **Table** | `etl_log` |
# MAGIC | **Columns** | 12 |
# MAGIC | **Format** | Delta |
# MAGIC
# MAGIC **Columns:**
# MAGIC
# MAGIC | # | Column | Type | Description |
# MAGIC |---|--------|------|-------------|
# MAGIC | 1 | `job_id` | STRING | Databricks Job ID |
# MAGIC | 2 | `job_run_id` | STRING | Databricks Job Run ID |
# MAGIC | 3 | `task_id` | STRING | Databricks Task ID |
# MAGIC | 4 | `task_run_id` | STRING | Databricks Task Run ID |
# MAGIC | 5 | `start_time` | TIMESTAMP | Ingestion start time |
# MAGIC | 6 | `end_time` | TIMESTAMP | Ingestion end time |
# MAGIC | 7 | `source_file_path` | STRING | Path to the source file ingested |
# MAGIC | 8 | `source_table` | STRING | Source table name |
# MAGIC | 9 | `target_table` | STRING | Fully qualified target table name |
# MAGIC | 10 | `success` | INT | 1 = success, 0 = failure |
# MAGIC | 11 | `failure_reason` | STRING | Error message on failure |
# MAGIC | 12 | `record_count` | LONG | Number of records written |
# MAGIC
# MAGIC **Widget inputs:** `catalog` (Unity Catalog name), `schema` (target schema, default `bronze`).
# MAGIC
# MAGIC > Run this notebook once per environment to provision the ETL log table before ingestion.

# COMMAND ----------

# --- Widget Setup ---
dbutils.widgets.text("catalog", "foia_tst")
dbutils.widgets.text("schema", "silver")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"[INFO] DDL target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL: `etl_log`

# COMMAND ----------

# Create ETL_LOG table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.etl_log (
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
COMMENT 'ETL audit log for Bronze ingestion pipelines'
""")

print(f"[INFO] ETL log table created: {catalog}.{schema}.etl_log")