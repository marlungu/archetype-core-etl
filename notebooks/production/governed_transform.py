# Databricks notebook source
"""Governed PySpark transformation reference.

The Airflow pipeline uses parameterized SQL MERGE for operational writes.
This notebook demonstrates the Spark-native transformation pattern used when
processing runs on Databricks compute.
"""
from pyspark.sql import functions as F

CATALOG = "archetype_core"
SCHEMA = "governed"
BRONZE = f"{CATALOG}.{SCHEMA}.classifications_bronze"
GOLD = f"{CATALOG}.{SCHEMA}.classifications_gold"

bronze = spark.table(BRONZE)
columns_for_hash = [F.col(c) for c in sorted(bronze.columns)]

governed = (
    bronze
    .withColumn("processed_at", F.current_timestamp())
    .withColumn("record_hash", F.sha2(F.to_json(F.struct(*columns_for_hash)), 256))
    .dropDuplicates(["record_id", "pipeline_run_id"])
)

governed.createOrReplaceTempView("governed_updates")

spark.sql(f"""
MERGE INTO {GOLD} AS target
USING governed_updates AS source
ON target.record_id = source.record_id
AND target.pipeline_run_id = source.pipeline_run_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
