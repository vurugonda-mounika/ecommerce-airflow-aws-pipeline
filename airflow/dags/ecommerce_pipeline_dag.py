# ecommerce_pipeline_dag.py
# PURPOSE: Apache Airflow DAG that orchestrates the complete
#          e-commerce sales analytics pipeline on a daily schedule
#
# This DAG coordinates every step in the correct order:
# 1. Validate source data in S3
# 2. Trigger AWS Glue crawler to catalog new data
# 3. Load data from S3 into Redshift staging tables
# 4. Run dbt transformations (staging + mart models)
# 5. Run dbt data quality tests
# 6. Send success notification
#
# In real work: this exact pattern runs in production at enterprise
# companies. Every step has retry logic, failure alerts, and
# dependency management — the same patterns used at LetQuickly
# and at major retail companies worldwide.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
import boto3
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# DAG CONFIGURATION
# These settings apply to every task in the DAG
# In production: these come from Airflow Variables or environment
# ─────────────────────────────────────────────────────────────────

default_args = {
    "owner":              "mounika.vurugonda",
    "depends_on_past":    False,

    # Retry configuration — production pipelines always retry
    # 3 retries with 5 minute gaps before marking as failed
    "retries":            3,
    "retry_delay":        timedelta(minutes=5),

    # Alert on failure — sends email to data team
    # In production: this triggers PagerDuty or Slack alert
    "email_on_failure":   True,
    "email_on_retry":     False,
    "email":              ["data-team@company.com"],

    # Start date — when the DAG first became active
    "start_date":         days_ago(1),
}

# ─────────────────────────────────────────────────────────────────
# DAG DEFINITION
# schedule_interval: runs every day at 6:00 AM UTC
# catchup=False: does not backfill missed runs
# tags: used to filter DAGs in Airflow UI
# ─────────────────────────────────────────────────────────────────

dag = DAG(
    dag_id="ecommerce_sales_pipeline",
    default_args=default_args,
    description="Daily e-commerce sales analytics pipeline — S3 to Redshift via dbt",
    schedule_interval="0 6 * * *",   # 6:00 AM UTC every day
    catchup=False,                    # do not backfill missed runs
    max_active_runs=1,                # only one run at a time
    tags=["ecommerce", "sales", "analytics", "daily"],
)

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION VARIABLES
# In production: use Airflow Variables (airflow.models.Variable)
# so these can be changed without code deployment
# ─────────────────────────────────────────────────────────────────

S3_BUCKET          = "ecommerce-analytics-raw"
S3_PREFIX          = "olist/orders/"
GLUE_CRAWLER_NAME  = "ecommerce-olist-crawler"
REDSHIFT_CONN_ID   = "redshift_ecommerce"
REDSHIFT_SCHEMA    = "ecommerce_raw"
DBT_PROJECT_DIR    = "/opt/airflow/dbt"


# ─────────────────────────────────────────────────────────────────
# TASK 1: S3 SENSOR
# Waits until today's data file exists in S3 before proceeding
# This is a critical production pattern — never assume data arrived
# poke_interval: checks every 5 minutes
# timeout: fails after 2 hours if file never arrives
# ─────────────────────────────────────────────────────────────────

wait_for_s3_data = S3KeySensor(
    task_id="wait_for_s3_data",
    bucket_name=S3_BUCKET,
    bucket_key=f"{S3_PREFIX}olist_orders_dataset.csv",
    aws_conn_id="aws_default",
    poke_interval=300,         # check every 5 minutes
    timeout=7200,              # fail after 2 hours
    mode="poke",               # keep checking until file arrives
    dag=dag,
)


# ─────────────────────────────────────────────────────────────────
# TASK 2: VALIDATE DATA QUALITY
# Checks the uploaded file has data and meets minimum row count
# Catches issues before they propagate downstream
# In production: this saves hours of debugging bad data
# ─────────────────────────────────────────────────────────────────

def validate_s3_data(**context):
    """
    Validates the S3 data file before processing.
    Checks file exists, is not empty, and meets minimum size.
    Fails the task if validation fails — stops the pipeline early.
    """
    s3 = boto3.client("s3")

    try:
        response = s3.head_object(
            Bucket=S3_BUCKET,
            Key=f"{S3_PREFIX}olist_orders_dataset.csv"
        )

        file_size_mb = response["ContentLength"] / (1024 * 1024)
        logger.info(f"S3 file size: {file_size_mb:.2f} MB")

        # Minimum file size check — protects against empty uploads
        if file_size_mb < 0.1:
            raise ValueError(
                f"File too small: {file_size_mb:.2f} MB. "
                f"Expected at least 0.1 MB. Possible empty upload."
            )

        logger.info("S3 data validation passed")
        return {"file_size_mb": file_size_mb, "status": "valid"}

    except Exception as e:
        logger.error(f"S3 validation failed: {str(e)}")
        raise


validate_data = PythonOperator(
    task_id="validate_s3_data",
    python_callable=validate_s3_data,
    dag=dag,
)


# ─────────────────────────────────────────────────────────────────
# TASK 3: TRIGGER AWS GLUE CRAWLER
# Glue Crawler scans S3 and updates the Glue Data Catalog
# This keeps the schema metadata up to date automatically
# If new columns are added to source data — Glue detects them
# ─────────────────────────────────────────────────────────────────

run_glue_crawler = GlueJobOperator(
    task_id="run_glue_crawler",
    job_name=GLUE_CRAWLER_NAME,
    aws_conn_id="aws_default",
    dag=dag,
)


# ─────────────────────────────────────────────────────────────────
# TASK 4: CREATE REDSHIFT STAGING TABLE
# Creates the raw landing table in Redshift if it does not exist
# COPY command loads data from S3 into Redshift
# This is the standard AWS data lake to warehouse load pattern
# ─────────────────────────────────────────────────────────────────

create_staging_table = RedshiftSQLOperator(
    task_id="create_staging_table",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql="""
        CREATE TABLE IF NOT EXISTS ecommerce_raw.raw_orders (
            order_id                    VARCHAR(50),
            customer_id                 VARCHAR(50),
            order_status                VARCHAR(20),
            order_purchase_timestamp    TIMESTAMP,
            order_approved_at           TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        )
        DISTKEY(customer_id)
        SORTKEY(order_purchase_timestamp);
    """,
    dag=dag,
)


load_s3_to_redshift = RedshiftSQLOperator(
    task_id="load_s3_to_redshift",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql="""
        -- Truncate and reload — full refresh pattern for daily batch
        -- In production with large tables: use incremental UPSERT instead
        TRUNCATE TABLE ecommerce_raw.raw_orders;

        COPY ecommerce_raw.raw_orders
        FROM 's3://{{ var.value.s3_bucket }}/olist/orders/olist_orders_dataset.csv'
        IAM_ROLE '{{ var.value.redshift_iam_role }}'
        FORMAT AS CSV
        IGNOREHEADER 1
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        MAXERROR 100;
    """,
    dag=dag,
)


# ─────────────────────────────────────────────────────────────────
# TASK 5: RUN DBT MODELS
# Runs dbt staging and mart models in sequence
# BashOperator executes dbt CLI commands
# --select flag runs only specific models — faster than full run
# In production: dbt Cloud API is used instead of BashOperator
# ─────────────────────────────────────────────────────────────────

run_dbt_staging = BashOperator(
    task_id="run_dbt_staging",
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --select staging \
            --profiles-dir /opt/airflow/dbt \
            --target prod \
            --vars '{{"execution_date": "{{{{ ds }}}}"}}'
    """,
    dag=dag,
)

run_dbt_marts = BashOperator(
    task_id="run_dbt_marts",
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --select marts \
            --profiles-dir /opt/airflow/dbt \
            --target prod \
            --vars '{{"execution_date": "{{{{ ds }}}}"}}'
    """,
    dag=dag,
)


# ─────────────────────────────────────────────────────────────────
# TASK 6: RUN DBT TESTS
# Runs all data quality tests after transformations
# If any test fails — task fails, pipeline stops
# Prevents bad data from reaching downstream consumers
# ─────────────────────────────────────────────────────────────────

run_dbt_tests = BashOperator(
    task_id="run_dbt_tests",
    bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test \
            --profiles-dir /opt/airflow/dbt \
            --target prod
    """,
    dag=dag,
)


# ─────────────────────────────────────────────────────────────────
# TASK 7: PIPELINE SUCCESS NOTIFICATION
# Logs pipeline completion with summary statistics
# In production: sends Slack message or updates monitoring dashboard
# ─────────────────────────────────────────────────────────────────

def notify_success(**context):
    """
    Logs pipeline completion summary.
    In production: sends Slack notification to data team channel.
    Pattern: always notify on success AND failure in production.
    """
    execution_date = context["ds"]
    dag_run_id     = context["run_id"]

    logger.info("=" * 60)
    logger.info("E-COMMERCE PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"DAG run ID:     {dag_run_id}")
    logger.info("Steps completed:")
    logger.info("  1. S3 data validated")
    logger.info("  2. Glue crawler updated catalog")
    logger.info("  3. Data loaded into Redshift")
    logger.info("  4. dbt staging models run")
    logger.info("  5. dbt mart models run")
    logger.info("  6. All data quality tests passed")
    logger.info("=" * 60)


pipeline_success = PythonOperator(
    task_id="pipeline_success_notification",
    python_callable=notify_success,
    dag=dag,
)


# ─────────────────────────────────────────────────────────────────
# TASK DEPENDENCIES — THE DAG GRAPH
# >> operator sets execution order
# Each task only runs after the previous one succeeds
# This is the core value of Airflow — dependency management
#
# Flow:
# wait_for_s3_data
#       ↓
# validate_data
#       ↓
# run_glue_crawler
#       ↓
# create_staging_table
#       ↓
# load_s3_to_redshift
#       ↓
# run_dbt_staging
#       ↓
# run_dbt_marts
#       ↓
# run_dbt_tests
#       ↓
# pipeline_success
# ─────────────────────────────────────────────────────────────────

(
    wait_for_s3_data
    >> validate_data
    >> run_glue_crawler
    >> create_staging_table
    >> load_s3_to_redshift
    >> run_dbt_staging
    >> run_dbt_marts
    >> run_dbt_tests
    >> pipeline_success
)
