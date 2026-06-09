# E-Commerce Sales Analytics Pipeline
# Apache Airflow + AWS S3 + Redshift + dbt

![Python](https://img.shields.io/badge/Python-3.9-blue)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE)
![AWS](https://img.shields.io/badge/AWS-S3_Redshift-FF9900)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF6F3C)
![SQL](https://img.shields.io/badge/SQL-Analytics-green)

## What This Project Does
Builds a production-grade scheduled e-commerce sales analytics
pipeline that extracts order, customer, and product data from
a public dataset, loads it into AWS S3, orchestrates the entire
workflow using Apache Airflow DAGs on a daily schedule, loads
into AWS Redshift, and transforms using dbt into business-ready
customer and sales analytics tables.

## Why I Built This
In my professional work I build and maintain scheduled data
pipelines that run automatically every day. This project
demonstrates production-grade Airflow DAG orchestration with
AWS cloud services — the same scheduling and orchestration
pattern used in enterprise data platforms at scale.

## Architecture
E-Commerce Dataset (Kaggle CSV)
        ↓
Python upload_to_s3.py
        ↓
AWS S3 (raw data lake layer)
        ↓
Apache Airflow DAG (daily schedule — orchestrates all steps)
        ↓
AWS Glue Crawler (auto-catalogs S3 data)
        ↓
AWS Redshift (cloud data warehouse)
        ↓
dbt staging model (cleans and standardizes orders)
        ↓
dbt mart model (customer RFM analytics + sales summary)
        ↓
Analytics-ready tables for BI consumption

## Tech Stack
| Tool | Purpose |
|------|---------|
| Apache Airflow | DAG orchestration and scheduling |
| AWS S3 | Raw data lake storage |
| AWS Redshift | Cloud data warehouse |
| AWS Glue | Data catalog and crawler |
| Python | Data extraction and S3 upload |
| dbt Core | SQL transformations and data quality tests |
| SQL | Advanced analytics and data modeling |

## Project Structure
ecommerce-airflow-aws-pipeline/
├── README.md
├── .gitignore
├── dbt_project.yml
├── airflow/
│   └── dags/
│       └── ecommerce_pipeline_dag.py
├── aws/
│   └── setup.sql
├── scripts/
│   └── upload_to_s3.py
├── models/
│   ├── staging/
│   │   └── stg_orders.sql
│   ├── marts/
│   │   └── mart_sales_analytics.sql
│   └── schema.yml
└── tests/
    └── test_positive_order_value.sql

## Key Features
- Apache Airflow DAG with daily schedule and retry logic
- AWS S3 as raw data lake with partitioned folder structure
- AWS Redshift as scalable cloud data warehouse
- dbt staging model cleans and standardizes raw order data
- dbt mart model calculates RFM scores — Recency, Frequency,
  Monetary — standard customer analytics pattern
- Automated dbt schema tests for data quality on every run
- Full pipeline runs unattended on schedule — zero manual steps

## Data Source
Brazilian E-Commerce Public Dataset by Olist
Available on Kaggle — 100,000+ real orders from 2016 to 2018
Contains orders, customers, products, sellers, payments, reviews
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Pipeline Schedule
The Airflow DAG runs daily at 6:00 AM UTC
Catchup disabled — only processes new data each run
Retry logic — 3 retries with 5 minute delay on failure
Email alerts on failure — production monitoring pattern

## How To Run This Project

Step 1 — Clone the repository
git clone https://github.com/vurugonda-mounika/ecommerce-airflow-aws-pipeline.git

Step 2 — Install dependencies
pip install apache-airflow boto3 awswrangler dbt-redshift pandas kaggle

Step 3 — Configure AWS credentials
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1

Step 4 — Upload data to S3
python scripts/upload_to_s3.py

Step 5 — Start Airflow and trigger DAG
airflow standalone
airflow dags trigger ecommerce_pipeline

Step 6 — Run dbt transformations
dbt run
dbt test

## RFM Analytics Explained
This project implements RFM customer segmentation:
- Recency — how recently did the customer place an order?
- Frequency — how many orders has the customer placed?
- Monetary — how much has the customer spent in total?
RFM is the standard customer analytics framework used by
every major e-commerce and retail company worldwide.

## Results
- Raw order data lands in S3 within seconds of upload
- Airflow DAG orchestrates full pipeline on daily schedule
- Staging layer cleans 100,000+ orders with data type casting
  and null handling
- Mart layer produces customer RFM segments and sales summaries
  by product category, seller, and time period
- All dbt data quality tests passing on every scheduled run

## Author
Mounika Vurugonda
Senior Data Engineer | Snowflake · dbt · Airflow · AWS · Kafka
LinkedIn: https://linkedin.com/in/vurugonda-mounika
GitHub: https://github.com/vurugonda-mounika
