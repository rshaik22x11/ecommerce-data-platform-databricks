"""
Notebook: 02_gold_build_fact_order_items

Purpose:
--------
Builds the primary Gold fact table using a star schema design.

Fact Table:
-----------
gold.fact_order_items

Grain:
------
One row per (order_id, product_id, seller_id)

Design Notes:
-------------
- Orders, payments, and reviews are treated as contextual inputs
- Small dimensions are broadcast to reduce shuffle cost
- Table is rebuildable to support backfills
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("USE gold")

# Load Silver sources
order_items = spark.table("silver_olist_order_items")
orders = spark.table("silver_olist_orders")

# Load Gold dimensions
dim_customer = spark.table("gold.dim_customer")
dim_product = spark.table("gold.dim_product")
dim_seller = spark.table("gold.dim_seller")

# -----------------------
# Join order items with order context
# -----------------------
fact_base = (
    order_items
    .join(
        orders.select(
            "order_id",
            "customer_id",
            "order_purchase_timestamp"
        ),
        on="order_id",
        how="left"
    )
)

# -----------------------
# Derive date keys and partitions
# -----------------------
fact_with_date = (
    fact_base
    .withColumn("order_date", to_date("order_purchase_timestamp"))
    .withColumn("order_date_id", date_format(col("order_date"), "yyyyMMdd").cast("int"))
    .withColumn("order_year", year("order_date"))
    .withColumn("order_month", month("order_date"))
)

# -----------------------
# Enrich with dimensions (broadcast joins)
# -----------------------
fact_enriched = fact_with_date.join(
    broadcast(dim_customer), "customer_id", "left"
)

fact_enriched = fact_enriched.join(
    broadcast(dim_product), "product_id", "left"
)

fact_enriched = fact_enriched.join(
    broadcast(dim_seller), "seller_id", "left"
)

# -----------------------
# Derive metrics
# -----------------------
fact_final = (
    fact_enriched
    .withColumn("item_price", col("price"))
    .withColumn("total_item_value", col("price") + col("freight_value"))
)

# -----------------------
# Final schema selection
# -----------------------
fact_order_items = fact_final.select(
    "order_id",
    "product_id",
    "seller_id",
    "customer_id",
    "order_date_id",
    "order_date",
    "order_year",
    "order_month",
    "customer_city",
    "customer_state",
    "product_category_name",
    "seller_city",
    "seller_state",
    "item_price",
    "freight_value",
    "total_item_value"
)

# -----------------------
# Write Gold fact table
# -----------------------
(
    fact_order_items
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("order_year", "order_month")
    .saveAsTable("gold.fact_order_items")
)
