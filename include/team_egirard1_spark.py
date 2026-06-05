from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, current_timestamp, lit, round, sum as spark_sum, when

from include.paths import curated_kpis, raw_parquet, report_json


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """Read silver Parquet and keep valid transaction rows."""
    input_path = str(raw_parquet(logical_date))

    df = spark.read.parquet(input_path)

    return (
        df
        .filter(col("tx_id").isNotNull())
        .filter(col("category").isNotNull())
        .filter(col("country").isNotNull())
        .filter(col("amount_eur").isNotNull())
        .filter(col("amount_eur") > 0)
    )


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """Enrich transactions with date and amount segment."""
    return (
        df
        .withColumn("logical_date", lit(logical_date))
        .withColumn(
            "amount_segment",
            when(col("amount_eur") >= 100, lit("high"))
            .when(col("amount_eur") >= 50, lit("medium"))
            .otherwise(lit("low"))
        )
        .withColumn("processed_at", current_timestamp())
    )


def transform_3(df: DataFrame) -> DataFrame:
    """Aggregate revenue and transaction count by category and country."""
    return (
        df
        .groupBy("logical_date", "category", "country")
        .agg(
            round(spark_sum("amount_eur"), 2).alias("revenue_eur"),
            count("*").alias("transaction_count"),
        )
        .orderBy("category", "country")
    )


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """Run Spark KPI job and write curated Parquet plus dashboard JSON."""
    spark = (
        SparkSession.builder
        .appName(f"team_egirard1_kpis_{logical_date}")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        clean_df = transform_1(spark, logical_date)
        enriched_df = transform_2(spark, clean_df, logical_date)
        kpi_df = transform_3(enriched_df)

        curated_path = curated_kpis(logical_date)
        report_path = report_json(logical_date)

        Path(str(curated_path)).parent.mkdir(parents=True, exist_ok=True)
        Path(str(report_path)).parent.mkdir(parents=True, exist_ok=True)

        # Idempotence: overwrite the same logical date output.
        kpi_df.write.mode("overwrite").parquet(str(curated_path))

        total_row = kpi_df.agg(
            round(spark_sum("revenue_eur"), 2).alias("total_revenue"),
            spark_sum("transaction_count").alias("total_transactions"),
        ).collect()[0]

        group_count = kpi_df.count()

        report = {
            "logical_date": logical_date,
            "status": "kpi_ok",
            "curated_path": str(curated_path),
            "report_path": str(report_path),
            "total_revenue": float(total_row["total_revenue"] or 0),
            "total_transactions": int(total_row["total_transactions"] or 0),
            "kpi_groups": int(group_count),
        }

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report

    finally:
        spark.stop()
