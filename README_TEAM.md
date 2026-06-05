# Team: Etienne Girard & Enzo Augie

**DAG id:** `team_egirard1`  
**Git repo:** `https://github.com/etienneg92i/Lab4`  
**Spark module:** `include/team_egirard1_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

## 1. Business problem

<Who needs the dashboard? What breaks if the pipeline fails?>

The dashboard is intended for a retail company that wants to monitor daily sales activity.

The pipeline automatically ingests vendor transaction files, validates data quality, computes KPIs using Spark, and publishes a dashboard.

If the pipeline fails, business users lose visibility on daily revenue, transaction volume, and category/country performance.

---

## 2. Architecture

| task_id             | Role                         |
| ------------------- | ---------------------------- |
| wait_for_vendor_csv | Wait for daily vendor file   |
| ingest              | Load CSV into Silver Parquet |
| validate            | Verify Silver data quality   |
| compute_kpis        | Run Spark KPI aggregation    |
| publish_report      | Publish dashboard JSON       |
| export_summary      | Backup dashboard report      |



wait_for_vendor_csv
        ↓
      ingest
        ↓
     validate
        ↓
   compute_kpis
        ↓
  publish_report
        ↓
   export_summary

## 3. Spark transformations (≥3 - your code)

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Read Silver Parquet and filter invalid records |
| 2 | `transform_2` | Enrich data with processing metadata and amount segments |
| 3 | `transform_3` | Aggregate revenue and transaction count by category and country |

## 4. Idempotence

The pipeline is idempotent.

When the same logical date is executed multiple times:
- Silver Parquet is overwritten.
- Curated KPI Parquet is overwritten.
- Dashboard JSON is overwritten.
- Backup report is regenerated.

No duplicate data is created.

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_<shortname> -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

A corrupt vendor file can be generated using:

python scripts/vendor_drop.py --date 2026-06-03 --corrupt

The validation task checks row count and revenue consistency.

If validation fails, the validate task fails in Airflow and downstream tasks are not executed.
---

## 7. Exploration tracks

| Track           | Done? | Describe your implementation                    |
| --------------- | ----- | ----------------------------------------------- |
| R Reliability   | Yes   | Retry policy and validation checks              |
| S Spark depth   | Yes   | Three Spark transformations and KPI aggregation |
| O Orchestration | Yes   | Six Airflow tasks with dependencies             |
| Q Data quality  | Yes   | Validation of Silver layer before Spark         |
| P Custom        | Yes   | Dashboard backup generation                     |
| X SparkSubmit   | No    | Not implemented                                 |


## 8. Demo script & backup

1. Generate vendor file.
2. Trigger DAG.
3. Show Airflow execution.
4. Show generated Parquet file.
5. Show dashboard JSON.
6. Show backup JSON.
---

## 9. Production next steps

- Add monitoring and alerting.
- Store reports in object storage.
- Add historical KPI tracking.
- Add reference data joins.
- Deploy on a distributed Spark cluster.