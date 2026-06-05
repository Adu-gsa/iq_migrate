import os
import platform
import runpy
import sys
from datetime import datetime


def wheel_smoke_test():
    """Minimal wheel entry point to validate Python wheel task bootstrap only."""
    print("[SMOKE] wheel_smoke_test started")
    print(f"[SMOKE] utc_ts={datetime.utcnow().isoformat()}Z")
    print(f"[SMOKE] python={sys.version.split()[0]}")
    print(f"[SMOKE] executable={sys.executable}")
    print(f"[SMOKE] platform={platform.platform()}")
    print(f"[SMOKE] cwd={os.getcwd()}")
    print(f"[SMOKE] sys_path_entries={len(sys.path)}")
    print("[SMOKE] wheel_smoke_test completed")


def ingest_table_dispatcher():
    # Execute the dispatcher notebook-script module as a console entry point.
    runpy.run_module("data_ingestion.ingest_table.INGEST_TABLE_DISPATCHER", run_name="__main__")


def bronze_to_silver_ingestion():
    # Execute bronze-to-silver notebook-script module as a console entry point.
    runpy.run_module("data_ingestion.bronze_silver.BRONZE_TO_SILVER_INGESTION", run_name="__main__")


def send_email_notification():
    # Execute email notification module as a console entry point.
    runpy.run_module("data_ingestion.notification.EMAIL_NOTIFICATION", run_name="__main__")
