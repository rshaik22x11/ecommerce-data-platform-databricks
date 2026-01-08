"""
DLT Pipeline: Bronze Batch Reference Ingestion

Purpose:
--------
Ingest reference and lookup datasets into the Bronze layer using batch processing.

This pipeline represents the standard Bronze batch ingestion pattern used for
low-volume, slowly changing datasets such as customers, sellers, and products.

Design Notes:
-------------
- Batch ingestion (spark.read)
- No CDC or streaming required
- Append-only raw Bronze tables
- Minimal transformation to preserve source fidelity
"""

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_olist_customers",
    comment="Raw batch ingestion of Olist customers CSV"
)
def bronze_olist_customers():
    return (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/projects/olist/datasets_raw/customers/")
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("ingestion_time", current_timestamp())
    )
