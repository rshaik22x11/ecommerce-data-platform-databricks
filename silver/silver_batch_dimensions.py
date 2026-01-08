"""
DLT Pipeline: Silver Batch Dimensions

Purpose:
--------
Builds trusted Silver tables for reference datasets using batch processing.

This pipeline demonstrates the standard Silver batch pattern applied to
low-volume, slowly changing reference data.

Key Features:
-------------
- Batch processing (no streaming)
- Deduplication using business keys
- Join-safe filtering (non-null keys)
- Minimal transformation to preserve data usability
"""
import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dlt.table(
    name="silver_olist_customers",
    comment="Cleaned and deduplicated Olist customers (batch)"
)
def silver_olist_customers():

    df = dlt.read("bronze_olist_customers")

    window_spec = (
        Window
        .partitionBy("customer_id")
        .orderBy(col("ingestion_time").desc())
    )

    return (
        df.filter("customer_id IS NOT NULL")
          .withColumn("row_num", row_number().over(window_spec))
          .filter(col("row_num") == 1)
          .drop("row_num")
    )
