# =============================================================================
# Module: EMAIL_NOTIFICATION.py
# Version: 0.1
# Developed by: Adu Erena
# Date: 2025-06-05
# Description: End-of-workflow email notification task. Queries the ETL audit log
#              for the current job run and sends an HTML email summary with
#              per-task status, timing, record counts, and failure reasons.
#              Runs with run_if: ALL_DONE to execute regardless of upstream outcome.
# =============================================================================
"""
Email Notification — `EMAIL_NOTIFICATION`
**Description:** End-of-workflow task that queries the ETL audit log for the
current job run and sends an email summary with:
  - Overall job status (SUCCESS / FAILED)
  - Per-task status with start time, end time, and failure reason (if any)
  - Job-level start and end timestamps
Runs with `run_if: ALL_DONE` so it executes regardless of upstream outcome.
"""

import argparse
import sys
import traceback
from datetime import datetime

from pyspark.sql import SparkSession

from data_ingestion.env.environment_config import EnvironmentConfig

print("[INFO] EMAIL_NOTIFICATION: module imports loaded.")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(description="Send email notification on job completion")
    parser.add_argument("--env",          required=True, help="Environment: dev | test | prod")
    parser.add_argument("--job_id",       required=True, help="Databricks job ID")
    parser.add_argument("--job_run_id",   required=True, help="Databricks job run ID")
    parser.add_argument("--task_id",      required=True, help="Current task name")
    parser.add_argument("--task_run_id",  required=True, help="Current task run ID")
    parser.add_argument("--recipients",   required=True, help="Comma-separated list of email recipients")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# ETL log query
# ---------------------------------------------------------------------------

def _get_task_statuses(spark, catalog, schema, job_run_id):
    """Query the etl_log table for all tasks in this job run."""
    log_table = f"`{catalog}`.`{schema}`.`etl_log`"
    print(f"[INFO] Querying task statuses from {log_table} for job_run_id={job_run_id}")

    query = f"""
        SELECT task_id,
               source_table,
               target_table,
               success,
               failure_reason,
               start_time,
               end_time,
               record_count
        FROM   {log_table}
        WHERE  job_run_id = '{job_run_id}'
        ORDER BY start_time ASC
    """
    return spark.sql(query).collect()


# ---------------------------------------------------------------------------
# Email body builder
# ---------------------------------------------------------------------------

def _build_email_body(env, job_id, job_run_id, task_rows):
    """Build an HTML email body from the ETL log rows."""

    has_failure = any(row["success"] == 0 for row in task_rows)
    overall_status = "FAILED" if has_failure else "SUCCESS"
    status_color = "#dc3545" if has_failure else "#28a745"

    # Determine job-level start / end
    start_times = [row["start_time"] for row in task_rows if row["start_time"]]
    end_times   = [row["end_time"]   for row in task_rows if row["end_time"]]
    job_start = min(start_times).strftime("%Y-%m-%d %H:%M:%S") if start_times else "N/A"
    job_end   = max(end_times).strftime("%Y-%m-%d %H:%M:%S")   if end_times   else "N/A"

    total_tasks  = len(task_rows)
    passed_tasks = sum(1 for r in task_rows if r["success"] == 1)
    failed_tasks = total_tasks - passed_tasks

    # Build per-task table rows
    task_html_rows = ""
    for row in task_rows:
        st = row["start_time"].strftime("%Y-%m-%d %H:%M:%S") if row["start_time"] else "N/A"
        et = row["end_time"].strftime("%Y-%m-%d %H:%M:%S")   if row["end_time"]   else "N/A"
        status_text  = "SUCCESS" if row["success"] == 1 else "FAILED"
        row_color    = "#28a745" if row["success"] == 1 else "#dc3545"
        fail_reason  = row["failure_reason"] if row["failure_reason"] else ""
        record_count = row["record_count"]   if row["record_count"]  else 0

        task_html_rows += f"""
        <tr>
            <td style="padding:6px 12px; border:1px solid #dee2e6;">{row["task_id"]}</td>
            <td style="padding:6px 12px; border:1px solid #dee2e6;">{row["target_table"]}</td>
            <td style="padding:6px 12px; border:1px solid #dee2e6; color:{row_color}; font-weight:bold;">{status_text}</td>
            <td style="padding:6px 12px; border:1px solid #dee2e6;">{st}</td>
            <td style="padding:6px 12px; border:1px solid #dee2e6;">{et}</td>
            <td style="padding:6px 12px; border:1px solid #dee2e6; text-align:right;">{record_count}</td>
            <td style="padding:6px 12px; border:1px solid #dee2e6; color:#dc3545;">{fail_reason}</td>
        </tr>"""

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333;">
        <h2 style="color:{status_color};">FOIA Data Ingestion Job — {overall_status}</h2>
        <table style="border-collapse:collapse; margin-bottom:16px;">
            <tr><td style="padding:4px 12px; font-weight:bold;">Environment</td><td style="padding:4px 12px;">{env.upper()}</td></tr>
            <tr><td style="padding:4px 12px; font-weight:bold;">Job ID</td><td style="padding:4px 12px;">{job_id}</td></tr>
            <tr><td style="padding:4px 12px; font-weight:bold;">Job Run ID</td><td style="padding:4px 12px;">{job_run_id}</td></tr>
            <tr><td style="padding:4px 12px; font-weight:bold;">Job Start Time</td><td style="padding:4px 12px;">{job_start}</td></tr>
            <tr><td style="padding:4px 12px; font-weight:bold;">Job End Time</td><td style="padding:4px 12px;">{job_end}</td></tr>
            <tr><td style="padding:4px 12px; font-weight:bold;">Total Tasks</td><td style="padding:4px 12px;">{total_tasks}</td></tr>
            <tr><td style="padding:4px 12px; font-weight:bold;">Passed</td><td style="padding:4px 12px; color:#28a745;">{passed_tasks}</td></tr>
            <tr><td style="padding:4px 12px; font-weight:bold;">Failed</td><td style="padding:4px 12px; color:#dc3545;">{failed_tasks}</td></tr>
        </table>

        <h3>Task Details</h3>
        <table style="border-collapse:collapse; width:100%;">
            <thead>
                <tr style="background:#f8f9fa;">
                    <th style="padding:8px 12px; border:1px solid #dee2e6; text-align:left;">Task</th>
                    <th style="padding:8px 12px; border:1px solid #dee2e6; text-align:left;">Target Table</th>
                    <th style="padding:8px 12px; border:1px solid #dee2e6; text-align:left;">Status</th>
                    <th style="padding:8px 12px; border:1px solid #dee2e6; text-align:left;">Start Time</th>
                    <th style="padding:8px 12px; border:1px solid #dee2e6; text-align:left;">End Time</th>
                    <th style="padding:8px 12px; border:1px solid #dee2e6; text-align:right;">Records</th>
                    <th style="padding:8px 12px; border:1px solid #dee2e6; text-align:left;">Failure Reason</th>
                </tr>
            </thead>
            <tbody>
                {task_html_rows}
            </tbody>
        </table>

        <p style="margin-top:16px; font-size:12px; color:#888;">
            This is an automated notification from the FOIA Data Ingestion pipeline.
        </p>
    </body>
    </html>
    """
    return html, overall_status


# ---------------------------------------------------------------------------
# Email sender (Databricks SDK workspace email)
# ---------------------------------------------------------------------------

def _send_email(recipients, subject, html_body):
    """Send notification email using the Databricks SDK."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    from databricks.sdk.service.jobs import ViewItem, ViewsToExport

    # Use the Databricks workspace notification endpoint via SDK
    import requests
    import json

    host = w.config.host.rstrip("/")
    token = w.config.token

    payload = {
        "to": recipients,
        "subject": subject,
        "body": html_body,
        "content_type": "text/html",
    }

    # Attempt to use the internal email endpoint
    # If not available, fall back to SMTP
    try:
        response = requests.post(
            f"{host}/api/2.0/preview/email/send",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        if response.status_code == 200:
            print("[INFO] Email sent via Databricks email API.")
            return True
        else:
            print(f"[WARN] Databricks email API returned {response.status_code}: {response.text}")
            print("[INFO] Falling back to SMTP...")
    except Exception as e:
        print(f"[WARN] Databricks email API unavailable: {e}")
        print("[INFO] Falling back to SMTP...")

    # Fallback: send via SMTP
    return _send_email_smtp(recipients, subject, html_body)


def _send_email_smtp(recipients, subject, html_body):
    """Fallback: send email via SMTP (unauthenticated relay or environment-configured)."""
    import smtplib
    import os
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    smtp_host = os.environ.get("SMTP_HOST", "smtp.gsa.gov")
    smtp_port = int(os.environ.get("SMTP_PORT", "25"))
    from_addr = os.environ.get("SMTP_FROM", "noreply-foia-ingestion@gsa.gov")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    print(f"[INFO] Sending email via SMTP {smtp_host}:{smtp_port} to {recipients}")
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.sendmail(from_addr, recipients, msg.as_string())

    print("[INFO] Email sent via SMTP.")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("[INFO] EMAIL_NOTIFICATION: Starting job status email notification")
    print("=" * 60)

    args = _parse_args()
    env        = EnvironmentConfig.get_environment(args.env)
    catalog    = EnvironmentConfig.get_catalog(env)
    job_id     = args.job_id
    job_run_id = args.job_run_id
    recipients = [r.strip() for r in args.recipients.split(",") if r.strip()]

    print(f"[INFO] env={env}  catalog={catalog}  job_id={job_id}  job_run_id={job_run_id}")
    print(f"[INFO] recipients={recipients}")

    spark = SparkSession.builder.getOrCreate()

    # Query ETL log from both bronze and silver schemas
    task_rows = []
    for schema in ("bronze", "silver"):
        rows = _get_task_statuses(spark, catalog, schema, job_run_id)
        if rows:
            print(f"[INFO] Found {len(rows)} ETL log entries in {catalog}.{schema}.etl_log")
            task_rows.extend(rows)
    # Sort combined results by start_time
    task_rows.sort(key=lambda r: r["start_time"] or datetime.min)

    if not task_rows:
        print("[WARN] No ETL log entries found in any schema. Sending notification with minimal info.")
        html_body = f"""
        <html><body style="font-family:Arial,sans-serif;">
        <h2 style="color:#ffc107;">FOIA Data Ingestion Job — UNKNOWN STATUS</h2>
        <p>No ETL log entries were found for job run <b>{job_run_id}</b> (job {job_id}).</p>
        <p>Environment: <b>{env.upper()}</b></p>
        <p>Please check the Databricks workspace for details.</p>
        </body></html>
        """
        overall_status = "UNKNOWN"
    else:
        html_body, overall_status = _build_email_body(env, job_id, job_run_id, task_rows)
        print(f"[INFO] Overall status: {overall_status}  |  {len(task_rows)} tasks found in ETL log")

    subject = f"FOIA Ingestion [{env.upper()}] — {overall_status} — Job Run {job_run_id}"

    try:
        _send_email(recipients, subject, html_body)
        print("[INFO] EMAIL_NOTIFICATION: completed successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")
        print(traceback.format_exc())
        # Don't fail the whole job just because email couldn't be sent
        print("[WARN] Email notification failed but will not raise to avoid masking job status.")
        sys.exit(0)


if __name__ == "__main__":
    main()
