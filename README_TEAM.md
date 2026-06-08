# Team: Etienne Girard & Enzo Augie

**DAG id:** `team_egirard1`  
**Git repo:** `https://github.com/etienneg92i/Lab4`  
**Spark module:** `include/team_egirard1_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

## 1. Business Problem

A retail partner drops one CSV file per day with store transactions. Operations needs a daily dashboard to follow revenue and transaction volume by product category and country.

Our pipeline automates the full daily flow:

1. Wait for the vendor CSV file.
2. Ingest the CSV into typed Silver Parquet.
3. Validate the Silver data before analytics.
4. Compute KPI aggregates with PySpark.
5. Publish a JSON dashboard report.
6. Export a backup copy for the demo.

If the pipeline fails, business users lose visibility on daily sales performance and cannot trust the dashboard for that logical day.

## 2. Architecture

### Airflow Tasks

| task_id | Role |
|---|---|
| `wait_for_vendor_csv` | Waits for `data/incoming/transactions_<ds>.csv` using a `FileSensor`. |
| `ingest` | Converts the vendor CSV into Silver Parquet under `data/raw/dt=<ds>/`. |
| `validate` | Checks row count and total revenue; fails visibly if the Silver data is corrupt. |
| `compute_kpis` | Runs the team PySpark module and writes curated KPI Parquet. |
| `publish_report` | Reads the dashboard JSON and returns the main metrics to Airflow. |
| `export_summary` | Copies the dashboard JSON to a backup folder for the demo. |

### DAG Flow

```text
wait_for_vendor_csv
        |
      ingest
        |
     validate
        |
   compute_kpis
        |
  publish_report
        |
  export_summary
```

## 3. Spark Transformations

The Spark job is implemented in `include/team_egirard1_spark.py`.

| Step | Function | Description |
|---|---|---|
| 1 | `transform_1` | Reads the Silver Parquet file and filters invalid transaction rows. |
| 2 | `transform_2` | Adds the logical date, an `amount_segment`, and processing metadata. |
| 3 | `transform_3` | Aggregates revenue and transaction count by category and country. |

The Spark job writes:

- Curated KPI Parquet: `data/curated/dt=<ds>/kpis_by_category_country.parquet`
- Dashboard JSON: `data/reports/dashboard_<ds>.json`

The dashboard JSON contains the logical date, output paths, total revenue, total transaction count, and number of KPI groups.

## 4. Idempotence

The pipeline is idempotent for one Airflow logical date (`ds`).

When the same date is executed multiple times:

- the Silver Parquet output is overwritten;
- the curated KPI Parquet output is overwritten;
- the dashboard JSON is overwritten;
- the backup JSON is regenerated.

This prevents duplicate KPI rows and keeps one deterministic output per logical day.

## 5. Backfill

Before a backfill, generate the vendor files:

```bash
python scripts/vendor_drop.py --seed-pack --volume small
python scripts/vendor_drop.py --reference
```

Then run the Airflow backfill:

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_egirard1 -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

## 6. Failure Demo

A corrupt vendor file can be generated with:

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

The `validate` task checks that the Silver data has enough rows and a positive total revenue. With a corrupt file, the validation fails in Airflow and downstream tasks do not run. This gives a visible red task in the Airflow UI.

## 7. Exploration Tracks

| Track | Status | Implementation |
|---|---|---|
| R - Reliability | Done | Retry policy, sensor timeout, validation before Spark, and documented backfill. |
| S - Spark depth | Partial | Three Spark transformations with filtering, enrichment, and aggregation. No reference-data join was added. |
| O - Orchestration | Done | Six Airflow tasks with explicit dependencies. |
| Q - Data quality | Done | Row-count and revenue checks in the `validate` task; supports the `--corrupt` demo. |
| P - Custom | Done | Backup export of the dashboard JSON through `export_summary`. |
| X - SparkSubmit | Not implemented | The project keeps an in-process local SparkSession for the laptop demo. |

## 8. Demo Script

1. Generate vendor files with `scripts/vendor_drop.py`.
2. Open the Airflow UI and unpause `team_egirard1`.
3. Trigger the DAG for one logical date, for example `2026-06-03`.
4. Show the green Airflow run and the six task instances.
5. Show the curated Parquet path under `data/curated/dt=2026-06-03/`.
6. Show `data/reports/dashboard_2026-06-03.json`.
7. Optional: generate a corrupt file and show the `validate` task failing.

## 9. Demo Backup

If the live demo fails, we can use backup evidence:

- Airflow graph screenshot with a successful run.
- `dashboard_<ds>.json` showing `status: "kpi_ok"`.
- Curated Parquet folder under `data/curated/dt=<ds>/`.
- Optional screenshot of a failed `validate` task after using `--corrupt`.

## 10. Production Discussion

The project uses `SparkSession.builder.master("local[*]")` because the lab runs on a laptop inside the Airflow Docker environment. In production, the Spark job would be submitted to a remote Spark cluster with `SparkSubmitOperator`, while Airflow would only orchestrate and monitor the job.

Possible production improvements:

- store curated data and reports in object storage;
- add stronger schema validation;
- add alerting on failed tasks;
- add reference-data joins for business targets;
- run Spark on a distributed cluster instead of local mode.
