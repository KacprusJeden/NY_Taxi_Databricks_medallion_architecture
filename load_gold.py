# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from datetime import date, datetime

# ============================================================
# PARAMS
# ============================================================
date_of_data = dbutils.widgets.get("date_of_data")

try:
    date_of_data = datetime.strptime(date_of_data, "%Y-%m-%d")
except ValueError:
    raise ValueError(
        f"Uncorrect date: '{date_of_data}'. "
        "Expected format: YYYY-MM-DD."
    )

# COMMAND ----------

from pyspark.sql import functions as F

source_table = "silver.dbo.ny_taxi"

df_silver = spark.table(source_table).filter(F.col("date_of_data") == F.lit(date_of_data))

# COMMAND ----------

def save(df, table_name, date_of_data):
    df.write.format("delta")\
    .mode("overwrite")\
    .option("replaceWhere", f"date_of_data = '{date_of_data}'")\
    .option("mergeSchema", "true")\
    .saveAsTable(table_name)

# COMMAND ----------

destination_table = "gold.dbo.ny_taxi_daily"

df_prepared = (
    df_silver
    .withColumn("trip_date", F.to_date(F.col("pickup_datetime")))
    .withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_datetime") - F.col("pickup_datetime")).cast("long") / 60.0
    )
)

gold_taxi_daily = (
    df_prepared
    .groupBy("date_of_data", "taxi_type", "trip_date")
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("passenger_count").alias("total_passengers"),
        F.round(F.sum("trip_distance"), 2).alias("total_distance"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.sum("tip_amount"), 2).alias("total_tip"),
        F.round(F.sum("total_amount"), 2).alias("total_amount"),
        F.round(F.avg("total_amount"), 2).alias("avg_trip_amount"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
    )
    .select(
        "date_of_data",
        "taxi_type",
        "trip_date",
        "trip_count",
        "total_passengers",
        "total_distance",
        "avg_distance",
        "total_fare",
        "total_tip",
        "total_amount",
        "avg_trip_amount",
        "avg_trip_duration_minutes"
    )
)

save(gold_taxi_daily, destination_table, date_of_data)

# COMMAND ----------

destination_table = "gold.dbo.ny_taxi_zone_daily"

df_prepared = (
    df_silver
    .withColumn("trip_date", F.to_date(F.col("pickup_datetime")))
    .withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_datetime") - F.col("pickup_datetime")).cast("long") / 60.0
    )
)

gold_zone_daily = (
    df_prepared
    .groupBy(
        "date_of_data",
        "trip_date",
        "taxi_type",
        "pulocationid",
        "dolocationid"
    )
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_amount"),
        F.round(F.avg("total_amount"), 2).alias("avg_amount"),
        F.round(F.sum("trip_distance"), 2).alias("total_distance"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
    )
    .select(
        "date_of_data",
        "trip_date",
        "taxi_type",
        "pulocationid",
        "dolocationid",
        "trip_count",
        "total_amount",
        "avg_amount",
        "total_distance",
        "avg_distance",
        "avg_trip_duration_minutes"
    )
)

save(gold_zone_daily, destination_table, date_of_data)