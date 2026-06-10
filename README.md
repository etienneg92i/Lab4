# Lab 4 – Big Data Processing Capstone

**Team:** Etienne Girard & Enzo Augie  
**Course:** Big Data Processing – EPF (Data/IA)  
**DAG id:** `team_egirard1`  
**Spark module:** `include/team_egirard1_spark.py`

---

## Overview

A retail partner drops one CSV file per day containing store transactions. This pipeline automates the full daily flow:

1. Wait for the vendor CSV file to arrive
2. Ingest the CSV into typed Silver Parquet (DuckDB)
3. Validate the Silver data before triggering analytics
4. Compute KPI aggregates with PySpark
5. Publish a JSON dashboard report
6. Export a backup copy for the demo

The pipeline runs on Apache Airflow 2.10 with CeleryExecutor, PySpark 3.5, and DuckDB, all packaged inside Docker.

---

## Project Structure

```
.
├── dags/
│   ├── team_egirard1.py          # Team DAG
│   └── starter_pipeline.py       # Reference starter DAG
├── include/
│   ├── ingest.py                 # Bronze → Silver (DuckDB)
│   ├── paths.py                  # Medallion path helpers
│   ├── team_egirard1_spark.py    # PySpark KPI transforms
│   └── __init__.py
├── spark_jobs/
│   └── daily_kpis.py             # Standalone Spark script (reference)
├── scripts/
│   ├── vendor_drop.py            # Simulate vendor CSV delivery
│   └── smoke_test.py             # End-to-end smoke test
├── lib/
│   └── lab4_starter/             # Starter library provided by the course
├── data/
│   ├── incoming/                 # Vendor CSVs (git-ignored)
│   ├── raw/                      # Silver Parquet (git-ignored)
│   ├── curated/                  # KPI Parquet (git-ignored)
│   ├── reports/                  # Dashboard JSON (git-ignored)
│   └── reference/                # Dimension tables
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── .env.example
```

---

## Architecture

### DAG Flow

```
wait_for_vendor_csv
        │
      ingest
        │
     validate
        │
   compute_kpis
        │
  publish_report
        │
  export_summary
```

### Task descriptions

| task_id | Tool | Role |
|---|---|---|
| `wait_for_vendor_csv` | `FileSensor` | Waits for `data/incoming/transactions_<ds>.csv` |
| `ingest` | DuckDB | Converts the CSV into Silver Parquet under `data/raw/dt=<ds>/` |
| `validate` | DuckDB | Checks row count and total revenue; fails fast on corrupt data |
| `compute_kpis` | PySpark | Runs three transforms and writes curated KPI Parquet |
| `publish_report` | Python | Reads the dashboard JSON and surfaces key metrics to Airflow |
| `export_summary` | Python | Copies the dashboard JSON to a backup folder |

### Medallion Layers

| Layer | Path | Format |
|---|---|---|
| Incoming | `data/incoming/transactions_<ds>.csv` | CSV |
| Silver (raw) | `data/raw/dt=<ds>/transactions.parquet` | Parquet |
| Gold (curated) | `data/curated/dt=<ds>/kpis_by_category_country.parquet` | Parquet |
| Report | `data/reports/dashboard_<ds>.json` | JSON |

---

## Spark Transformations

All transforms are in `include/team_egirard1_spark.py`.

| Step | Function | Description |
|---|---|---|
| 1 | `transform_1` | Reads Silver Parquet; drops rows with null `tx_id`, `category`, `country`, or non-positive `amount_eur` |
| 2 | `transform_2` | Enriches each transaction with `logical_date`, `amount_segment` (low / medium / high), and `processed_at` |
| 3 | `transform_3` | Aggregates `revenue_eur` and `transaction_count` by `(logical_date, category, country)` |

The dashboard JSON output contains: `logical_date`, `total_revenue`, `total_transactions`, `kpi_groups`, `curated_path`.

---

## Getting Started

### Prerequisites

- Docker Desktop (8 GB RAM minimum, 12 GB free disk)
- Python 3.11+ on the host (for utility scripts only)

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/etienneg92i/Lab4.git
cd Lab4

# 2. Configure environment
cp .env.example .env
# On Linux/macOS: set AIRFLOW_UID=$(id -u) in .env

# 3. Pull base images and build the custom image
docker compose pull postgres redis
docker compose build airflow-init

# 4. Initialize the Airflow database and start all services
docker compose up airflow-init
docker compose up -d
```

Wait 60–90 seconds, then check that all services are healthy:

```bash
docker compose ps
```

Open the Airflow UI at [http://localhost:8080](http://localhost:8080) (`airflow` / `airflow`).

### Verify Java and PySpark

```bash
docker compose exec airflow-worker java -version
docker compose exec airflow-worker python -c \
  "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').getOrCreate(); print('Spark', s.version); s.stop()"
```

---

## Running the Pipeline

### 1. Seed vendor data

```bash
# Generate 14 days of transaction CSVs (June 1–14, 2026)
python scripts/vendor_drop.py --seed-pack --volume small
# Generate the reference dimension table
python scripts/vendor_drop.py --reference
```

### 2. Trigger the DAG

In the Airflow UI, unpause `team_egirard1` and trigger a manual run for a date of your choice, or use the CLI:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger team_egirard1 -e 2026-06-03T00:00:00+00:00
```

### 3. Check the output

```bash
cat data/reports/dashboard_2026-06-03.json
ls data/curated/dt=2026-06-03/
```

### Smoke test (end-to-end)

```bash
python scripts/smoke_test.py
```

This seeds data, unpauses `lab4_starter`, triggers `2026-06-01`, and waits for the dashboard JSON.

---

## Backfill

```bash
python scripts/vendor_drop.py --seed-pack --volume small
docker compose exec airflow-scheduler \
  airflow dags backfill team_egirard1 -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## Failure Demo

Generate a corrupt vendor file (all amounts set to zero):

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

Trigger the DAG for that date. The `validate` task will fail with a `RuntimeError` on the revenue check, leaving downstream tasks unexecuted. The Airflow UI shows a visible red task for that run.

---

## Idempotence

Re-running the DAG for the same logical date is safe. Each stage overwrites its output:

- Silver Parquet: deleted and recreated by DuckDB
- Curated Parquet: written with `mode("overwrite")` by Spark
- Dashboard JSON and backup JSON: overwritten on each run

---

## Exploration Tracks

| Track | Status | Notes |
|---|---|---|
| R – Reliability | ✅ Done | Retry policy, sensor timeout, validation before Spark, documented backfill |
| S – Spark depth | ⚠️ Partial | Three-step transform pipeline; reference-data join (`category_targets.csv`) not implemented |
| O – Orchestration | ✅ Done | Six tasks with explicit linear dependencies |
| Q – Data quality | ✅ Done | Row-count and revenue checks in `validate`; `--corrupt` demo supported |
| P – Custom | ✅ Done | `export_summary` task backs up the dashboard JSON |
| X – SparkSubmit | ❌ Not implemented | Pipeline uses an in-process `local[*]` SparkSession for the laptop demo |

---

## Production Discussion

The project uses `SparkSession.builder.master("local[*]")` because the lab runs on a laptop inside the Airflow Docker environment. In a production setting:

- Spark jobs would be submitted to a remote cluster with `SparkSubmitOperator`
- Curated data and reports would be stored in object storage (S3, GCS)
- Schema validation would be strengthened (e.g. Great Expectations)
- Failed tasks would trigger alerting (Slack, PagerDuty)
- Reference-data joins would be added for business KPI targets
- `JAVA_HOME` would be resolved dynamically to support both ARM64 and AMD64 builds

---

## Common Pitfalls

| Symptom | Fix |
|---|---|
| Port 8080 already in use | Set `AIRFLOW_UI_PORT=8081` in `.env`, then `docker compose up -d --force-recreate` |
| `Java not found` in Spark task | Rebuild the image; verify with `docker compose exec airflow-worker java -version` |
| `FileSensor` never succeeds | Run `vendor_drop.py` for that `ds` date before triggering the DAG |
| DAG import error | `docker compose logs airflow-scheduler \| tail -50` |
| Linux permission errors on logs | Set `AIRFLOW_UID=$(id -u)` in `.env` |
| `No module named 'pyspark'` | Rebuild with `docker compose build --no-cache airflow-init` |

---

## Stop / Reset

```bash
docker compose stop      # pause, keep all data
docker compose start     # resume
docker compose down -v   # full reset (deletes database volume)
```
