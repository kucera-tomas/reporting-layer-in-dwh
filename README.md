# CRM Data Pipeline & Analytics (Task #2)

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline and analytical suite for CRM data. It processes raw subscription, customer, and transaction data into a structured Data Warehouse and calculates key business metrics like MRR and Cumulative LTV.

## 📁 Project Structure

```text
project-root/
├── data_gen/
│   └── generate_sample_data.py    # Python script to generate mock CSVs/inserts
├── ddl/
│   ├── raw_schema.sql             # Table definitions for raw source data
│   └── dwh_schema.sql             # Table definitions for DWH and Data Marts
├── etl/
│   ├── stg_cleaning.sql           # Logic for deduplication and type casting
│   └── mart_sales_performance.sql # Building the dm_sales_performance view
├── quality_checks/
│   └── dq_alerts.sql              # SQL tests for nulls, orphans, and logic gaps
├── analytics/
│   ├── mrr_calculation.sql        # MRR logic (Annual-to-Monthly spreading)
│   └── cumulative_ltv.sql         # Month-over-month LTV growth per customer
└── README.md                      # Project 
└── requirements.txt               # For mock data generation 
```