"""
DLT Pipeline: Silver Orders CDC

Purpose:
--------
Builds a trusted Silver table for orders using CDC-style upserts.
This pipeline cleans Bronze data, enforces data quality rules,
and deterministically handles late-arriving records.

Key Features:
-------------
- CDC-style upserts using dlt.apply_changes
- Deterministic sequencing using ingestion timestamp
- Data quality enforcement via DLT expectations
- Streaming Silver pipeline
- Archived data items with missing business keys
"""

import dlt
from pyspark.sql.functions import *

# -----------------------
# Source View (Validated)
# -----------------------
@dlt.view(
    name="silver_orders_source",
    comment="Validated orders source for CDC-style upsert"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_order_purchase_timestamp", "order_purchase_timestamp IS NOT NULL")
def silver_orders_source():

    return (
        dlt.read_stream("bronze_olist_orders")
        .select(
            "order_id",
            "customer_id",
            "order_purchase_timestamp",
            "ingestion_time"
        )
    )

# -----------------------
# Silver CDC Target Table
# -----------------------

dlt.create_streaming_table(
    name="silver_orders",
    comment="Silver orders table managed via CDC-style upserts"
)

dlt.apply_changes(
    target="silver_orders",
    source="silver_orders_source",
    keys=["order_id"],
    sequence_by="ingestion_time",
    stored_as_scd_type=1
)

# -----------------------
# Silver archive Table
# -----------------------

@dlt.table(
    name="silver_orders_quarantine",
    comment="Orders with null business keys"
)
def silver_orders_quarantine():
    return (
        dlt.read_stream("bronze_olist_orders")
        .filter("order_id IS NULL")
        .withColumn("archieved_at", current_timestamp()))
