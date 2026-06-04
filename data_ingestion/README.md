
# foia_data_ingestion

FOIA Data Ingestion Project packaged and deployed as a Python wheel for Databricks jobs and pipelines.

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
