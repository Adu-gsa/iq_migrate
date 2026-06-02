import runpy


def ingest_table_dispatcher():
    # Execute the dispatcher notebook-script module as a console entry point.
    runpy.run_module("ingest_table.INGEST_TABLE_DISPATCHER", run_name="__main__")


def bronze_to_silver_ingestion():
    # Execute bronze-to-silver notebook-script module as a console entry point.
    runpy.run_module("bronze_silver.BRONZE_TO_SILVER_INGESTION", run_name="__main__")
