"""
Notebook: 01_gold_build_dimensions

Purpose:
--------
Builds stable, reusable Gold dimension tables from trusted Silver data.
This notebook contains only batch logic and is fully idempotent.

Design Principles:
------------------
- Source only from Silver layer
- No CDC or streaming
- Correct grain per dimension
- Rebuildable via overwrite
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("USE gold")

# -----------------------
# Customer Dimension
# -----------------------
dim_customer = (
    spark.table("silver_olist_customers")
    .select(
        "customer_id",
        "customer_city",
        "customer_state"
    )
)

dim_customer.write.mode("overwrite").format("delta").saveAsTable("gold.dim_customer")

# -----------------------
# Product Dimension
# -----------------------
dim_product = (
    spark.table("silver_olist_products")
    .select(
        "product_id",
        "product_category_name",
        "product_weight_g"
    )
)

dim_product.write.mode("overwrite").format("delta").saveAsTable("gold.dim_product")

# -----------------------
# Seller Dimension
# -----------------------
dim_seller = (
    spark.table("silver_olist_sellers")
    .select(
        "seller_id",
        "seller_city",
        "seller_state"
    )
)

dim_seller.write.mode("overwrite").format("delta").saveAsTable("gold.dim_seller")

# -----------------------
# Date Dimension
# -----------------------
date_df = (
    spark.table("silver_olist_orders")
    .select(to_date("order_purchase_timestamp").alias("date"))
    .distinct()
)

dim_date = (
    date_df
    .withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("week", weekofyear("date"))
)

dim_date.write.mode("overwrite").format("delta").saveAsTable("gold.dim_date")
