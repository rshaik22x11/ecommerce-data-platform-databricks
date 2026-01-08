# Olist Databricks Data Platform

## Overview
This project demonstrates a production-grade **end-to-end data engineering platform** built on **Databricks** using a **Bronze–Silver–Gold architecture**.

The platform supports **both batch and incremental ingestion**, CDC-style upserts, late-arriving data handling, and optimized analytical querying using Delta Lake.

---

## Architecture

### Bronze Layer (Raw Ingestion)
- Ingests raw CSV datasets into Delta tables
- Supports both batch and incremental ingestion:
  - Incremental ingestion using Databricks Auto Loader for transactional datasets
  - Batch ingestion for reference and lookup datasets
- Handles schema evolution and rescued data
- Append-only raw Delta tables preserving source fidelity

> **Note:** All transactional datasets follow the same Bronze ingestion pattern.
> This repository includes one representative Auto Loader pipeline to demonstrate
> the approach without duplicating boilerplate code.


### Silver Layer (Trusted Data)
- Built using **Delta Live Tables (DLT)**
- Implements **CDC-style upserts** using `apply_changes`
- Handles late-arriving data deterministically using ingestion timestamps
- Enforces data quality through:
  - Dropping irrecoverable records
  - Archiving partially invalid records for audit
- Uses batch ingestion for reference datasets

### Gold Layer (Analytics)
- Built using **Databricks notebooks**
- Star schema design optimized for analytics
- One primary fact table at the lowest useful grain
- Supports:
  - Partitioning
  - Broadcast joins
  - Z-ordering
  - Backfills
  - Schema evolution
  - Rollback using Delta Lake time travel

---

## Gold Data Model

### Fact Table
**gold.fact_order_items**
- Grain: `(order_id, product_id, seller_id)`
- Measures:
  - item_price
  - freight_value
  - total_item_value
  - net_item_value (added via backfill)

### Dimension Tables
- gold.dim_customer
- gold.dim_product
- gold.dim_seller
- gold.dim_date

---

## Performance Optimizations
- Partitioning by year and month
- Broadcast joins for small dimensions
- Z-ordering on frequently filtered columns
- Cost-aware OPTIMIZE strategy

---

## Data Quality & Observability
- DLT expectations for data validation
- Quarantine tables for auditability
- Schema evolution handled explicitly during controlled backfills

---

## Tech Stack
- Databricks
- Delta Lake
- PySpark
- Delta Live Tables (DLT)
- Unity Catalog

---

## Future Enhancements
- Incremental Gold refresh strategy
- Data quality metrics dashboards
- Job parameterization
- CI/CD integration
- Cost-based optimization automation

---

## Author
Built as a job-ready data engineering portfolio project.
