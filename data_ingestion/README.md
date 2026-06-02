
# foia_data_ingestion

FOIA Data Ingestion Project packaged and deployed as a Python wheel for Databricks jobs and pipelines.

## Build the wheel

```sh
python setup.py bdist_wheel
```

The wheel file will be created in the `dist/` directory.

## Install and validate the wheel locally

```sh
pip install dist/data_ingestion-0.1.0-py3-none-any.whl --force-reinstall
```

To validate the build, open a Python shell and import your package:

```python
import bronze_silver  # or any module from your package
# Run a function to verify installation
```

## Deploy the wheel to Databricks

1. Upload the wheel file (`dist/data_ingestion-0.1.0-py3-none-any.whl`) to your Databricks workspace or DBFS.
2. Reference the wheel in your job or pipeline YAML:

```yaml
libraries:
	- whl: file:/Workspace/path/to/data_ingestion-0.1.0-py3-none-any.whl
source: python_wheel
python_wheel_task:
	package_name: data_ingestion
	entry_point: your_entry_point
	parameters:
		--env "${job.parameters.env}"
		--schema "${bundle.variables.bronze_schema}"
		# ...other parameters...
```

## Notes
- Always rebuild the wheel after code changes and upload the latest version to Databricks.
- Use environment variables in your YAML for dev/prod switching.
