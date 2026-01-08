"""
DLT Pipeline: Bronze Incremental Orders

Purpose:
--------
Ingest raw transactional order data incrementally into the Bronze layer
using Databricks Auto Loader.

This pipeline represents the standard pattern used for all incremental
transactional datasets in the project.

Key Features:
-------------
- Incremental file ingestion (Auto Loader)
- Schema inference and evolution
- Rescued data handling for unexpected columns
- Append-only raw Bronze tables
"""
import dlt
from pyspark.sql.functions import *

#used Autoloader for high-volume, append-only datasets to support incremental ingestion

@dlt.table(
    name="bronze_olist_orders",
    comment="Raw incremental ingestion of Olist orders using Auto Loader with schema evolution and rescue"
)
def bronze_olist_orders():
    df=(
        spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","csv")
        .option("cloudFiles.inferColumnTypes", True)
        .option("cloudFiles.schemaEvolutionMode","addNewColumns")
        .option("cloudFiles.rescuedDataColumn","_rescued_data")
        .option("cloudFiles.schemaLocation", "/Volumes/projects/olist/meta_data/schemalocation/bronze_orders/")
        .option("header", True)
        .load("/Volumes/projects/olist/datasets_raw/orders/")
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name")))
    return df

