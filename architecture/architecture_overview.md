Olist CSV Files
      |
      v
+------------------------------+
| Bronze Layer (DLT)           |
| - Batch ingestion            |
| - Auto Loader (incremental)  |
| - Schema evolution & rescue  |
+------------------------------+
      |
      v
+------------------------------+
| Silver Layer (DLT)           |
| - CDC via apply_changes      |
| - Late data handling         |
| - Data quality rules         |
+------------------------------+
      |
      v
+------------------------------+
| Gold Layer (Notebooks)       |
| - Star schema                |
| - fact_order_items           |
| - dimensions                 |
| - Optimizations              |
+------------------------------+
      |
      v
BI / Analytics / SQL
