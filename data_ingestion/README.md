<!-- =============================================================================
  Project: foia_data_ingestion
  Version: 0.1
  Developed by: Adu Erena
  Date: 2025-06-05
  Description: FOIA Data Ingestion Project — Bronze and Silver layer pipeline
               packaged as a Python wheel for Databricks jobs.
============================================================================= -->

# foia_data_ingestion

FOIA Data Ingestion Project packaged and deployed as a Python wheel for Databricks jobs and pipelines.

## End-to-end process flow

### Overview

This pipeline ingests FAS Advantage FOIA data from flat files (CSV/TXT) into a Databricks Lakehouse using a medallion architecture (Bronze → Silver). The entire process is packaged as a Python wheel and orchestrated via Databricks job tasks.

```
  Source Files (.txt)          Bronze Layer (Delta)         Silver Layer (Delta)
  ┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
  │ ^|~ delimited    │────────>│ Raw data as-is   │────────>│ Typed & cleaned  │
  │ files on Volumes │  Read   │ + ingest metadata │  Cast   │ production-ready │
  └──────────────────┘         └──────────────────┘         └──────────────────┘
         │                              │
         │                              │
         v                              v
  ┌──────────────────┐         ┌──────────────────┐
  │ Archived to      │         │ ETL log table    │
  │ outbound folder  │         │ (audit trail)    │
  └──────────────────┘         └──────────────────┘
```

### Step-by-step flow

**1. Source files land on Databricks Volumes**
- Flat files (`.txt`, `^|~` delimited) are placed in `/Volumes/fas_advantage_np/bronze/fas_advantage_s3_np/IQ_RAW_FILES/<table_name>/`
- One file per table per run

**2. Bronze ingestion (`ingest_table_dispatcher` entry point)**
- The Databricks job triggers one task per table, each calling the `ingest_table_dispatcher` entry point
- For each table, the dispatcher:
  1. Reads the source `.txt` file using the table-specific schema
  2. Adds metadata columns (`ingest_ts`, `input_file_name`)
  3. Writes to the Bronze Delta table (full overwrite)
  4. Archives the source file to the outbound folder with a date partition
  5. Logs success/failure to the `etl_log` audit table
- Tables are ingested in a dependency chain (19 tables total):
  `gsin_hide_remove` → `gsin_hide_remove_hist` → `catalog_832` → ... → `item_xref`

**3. Bronze-to-Silver transformation (`bronze_to_silver_ingestion` entry point)**
- Runs after all Bronze tables are loaded
- Reads each Bronze table (all STRING columns)
- Casts columns to their Silver-layer target types (INT, TIMESTAMP, DECIMAL, etc.)
- Writes typed data to Silver Delta tables

**4. Environment & catalog resolution**
- Environment (`dev`, `test`, `prod`) is passed as a job parameter
- The `EnvironmentConfig` class maps each environment to its Unity Catalog:
  - `dev` → `foia_dev`
  - `test` → `foia_tst`
  - `prod` → `foia_prod`

### Tables ingested

| # | Table                  | # | Table              |
|---|------------------------|---|--------------------| 
| 1 | gsin_hide_remove       | 11 | suspend_contract  |
| 2 | gsin_hide_remove_hist  | 12 | bpa_header        |
| 3 | catalog_832            | 13 | bpa_item          |
| 4 | sin_limit              | 14 | bpa_item_price    |
| 5 | contracts              | 15 | price_discount    |
| 6 | mp_product             | 16 | zone_price        |
| 7 | product_file           | 17 | adv_product       |
| 8 | order_status           | 18 | item_xref_attributes |
| 9 | sin                    | 19 | item_xref         |
| 10 | contract_zone         |    |                   |

## Build the wheel

```sh
python -m build --wheel
```

The wheel file will be created in the `dist/` directory.

If `build` is not installed:

```sh
pip install build
python -m build --wheel
```

## Install and validate the wheel locally

```sh
pip install dist/data_ingestion-0.1.0-py3-none-any.whl --force-reinstall
```

To validate the build, open a Python shell and import your package:

```python
from data_ingestion.env import environment_config
from data_ingestion.ingest_table import wheel_entrypoints
```

## Project structure

```
src/
  data_ingestion/          # Root Python package
    __init__.py
    bronze_silver/         # Bronze → Silver transformation
    ddl/                   # Table DDL definitions
    env/                   # Environment config & ETL utilities
    ingest_table/          # Ingestion dispatcher & entry points
```

## Deploy the wheel to Databricks

### Step 1: Build the wheel

```sh
# Clean previous build artifacts
rm -rf build/ dist/ src/data_ingestion.egg-info/

# Build the wheel
python -m build --wheel
```

The output wheel will be at `dist/data_ingestion-0.1.0-py3-none-any.whl`.

### Step 2: Upload the wheel to Databricks Workspace

**Option A — Using Databricks CLI:**

```sh
databricks workspace mkdirs /Workspace/Shared/iq_migrate/data_ingestion/dist

databricks workspace import \
  dist/data_ingestion-0.1.0-py3-none-any.whl \
  /Workspace/Shared/iq_migrate/data_ingestion/dist/data_ingestion-0.1.0-py3-none-any.whl \
  --format AUTO --overwrite
```

**Option B — Using Databricks UI:**

1. Navigate to **Workspace > Shared > iq_migrate > data_ingestion > dist**
2. Click **Import** and upload `data_ingestion-0.1.0-py3-none-any.whl`

### Step 3: Reference the wheel in your job YAML

```yaml
python_wheel_task:
  package_name: data_ingestion
  entry_point: ingest_table_dispatcher    # or bronze_to_silver_ingestion
  named_parameters:
    env: '{{job.parameters.env}}'
    schema: bronze
    run_mode: single
    table_name: <table_name>
    fail_fast: 'true'
    job_id: '{{job.id}}'
    task_id: '{{task.name}}'
    task_run_id: '{{task.run_id}}'
    job_run_id: '{{job.run_id}}'
libraries:
  - whl: /Workspace/Shared/iq_migrate/data_ingestion/dist/data_ingestion-0.1.0-py3-none-any.whl
```

### Available entry points

| Entry point                  | Description                          |
|------------------------------|--------------------------------------|
| `ingest_table_dispatcher`    | Ingests a single table to Bronze     |
| `bronze_to_silver_ingestion` | Runs Bronze → Silver transformation  |
| `wheel_smoke_test`           | Validates wheel installation         |

## Notes
- Always rebuild the wheel after code changes and upload the latest version to Databricks.
- Use environment variables in your YAML for dev/prod switching.
- The wheel path in the job YAML must match the upload destination exactly.
