# =============================================================================
# Module: ETL_LOG_DDL_BRONZE.py
# Version: 0.1
# Developed by: Adu Erena
# Date: 2025-06-05
# Description: Creates the `etl_log` Delta table used for audit logging across
#              all Bronze ingestion pipelines. Each ingestion run appends a row
#              tracking job metadata, timing, record counts, and success/failure.
# =============================================================================
"""
ETL Log Table DDL
**Description:** Creates the `etl_log` Delta table used for audit logging across all Bronze ingestion pipelines. Each ingestion run appends a row tracking job metadata, timing, record counts, and success/failure status.
**Columns:**
**Widget inputs:** `catalog` (Unity Catalog name), `schema` (target schema, default `bronze`).
"""




# --- Widget Setup ---



if __name__ == '__main__':
    dbutils.widgets.text("catalog", "foia_tst")
    dbutils.widgets.text("schema", "bronze")

    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")

    print(f"[INFO] DDL target: {catalog}.{schema}")




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
