from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_egirard1_spark import run_daily

DEFAULT_ARGS = {
    "owner": "team_egirard1",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="team_egirard1",
    description="Capstone retail KPI pipeline",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone", "egirard1"],
) as dag:

    wait_csv = FileSensor(
        task_id="wait_for_vendor_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
    )

    @task
    def ingest(ds: str) -> dict:
        return ingest_day(ds)

    @task
    def validate(ds: str) -> dict:
        return validate_silver(ds)

    @task
    def compute_kpis(ds: str) -> dict:
        return run_daily(ds)

    @task
    def publish_report(ds: str) -> dict:
        path = report_json(ds)
        if not path.exists():
            raise FileNotFoundError(f"Report not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            report = json.load(f)

        return {
            "status": report.get("status", "ready"),
            "report_path": str(path),
            "curated_path": report.get("curated_path"),
            "total_revenue": report.get("total_revenue"),
            "total_transactions": report.get("total_transactions"),
        }

    @task
    def export_summary(ds: str) -> dict:
        source = report_json(ds)
        backup_dir = Path("/opt/airflow/data/reports/backup")
        backup_dir.mkdir(parents=True, exist_ok=True)

        target = backup_dir / f"dashboard_{ds}_backup.json"

        if not source.exists():
            raise FileNotFoundError(f"Report not found: {source}")

        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

        return {
            "status": "exported",
            "backup_path": str(target),
        }

    ingested = ingest()
    validated = validate()
    kpis = compute_kpis()
    published = publish_report()
    exported = export_summary()

    wait_csv >> ingested >> validated >> kpis >> published >> exported
