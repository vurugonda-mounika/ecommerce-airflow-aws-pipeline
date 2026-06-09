# upload_to_s3.py
# PURPOSE: Downloads Olist e-commerce dataset and uploads to AWS S3
#          in a production-ready partitioned folder structure
#
# This is the EXTRACT step of our pipeline.
# In real work: this script would pull from an e-commerce API,
# SFTP server, or database export instead of Kaggle.
# The S3 upload pattern and folder structure are identical
# to production enterprise data lake implementations.
#
# Folder structure in S3:
# s3://ecommerce-analytics-raw/
# └── olist/
#     ├── orders/
#     │   └── olist_orders_dataset.csv
#     ├── customers/
#     │   └── olist_customers_dataset.csv
#     ├── products/
#     │   └── olist_products_dataset.csv
#     ├── payments/
#     │   └── olist_order_payments_dataset.csv
#     └── reviews/
#         └── olist_order_reviews_dataset.csv

import boto3
import pandas as pd
import os
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from io import StringIO

# ─────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# S3 configuration
# In production: bucket name from environment variable
# Never hardcode bucket names in code — use os.environ
S3_BUCKET   = os.environ.get("S3_BUCKET", "ecommerce-analytics-raw")
S3_PREFIX   = "olist"
AWS_REGION  = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Data directory — where CSV files are stored locally
DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")


# ─────────────────────────────────────────────────────────────────
# S3 CLIENT
# Uses credentials from environment variables
# AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# In production: uses IAM roles instead — no keys needed
# ─────────────────────────────────────────────────────────────────

def get_s3_client():
    """
    Creates and returns an S3 client.
    In production on AWS: uses IAM instance role automatically.
    Locally: uses environment variable credentials.
    """
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )
    logger.info(f"S3 client created for region: {AWS_REGION}")
    return s3


# ─────────────────────────────────────────────────────────────────
# BUCKET SETUP
# Creates S3 bucket if it does not exist
# In production: bucket already exists — this is for local dev
# ─────────────────────────────────────────────────────────────────

def ensure_bucket_exists(s3_client):
    """
    Creates the S3 bucket if it does not exist.
    Idempotent — safe to run multiple times.
    """
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket already exists: {S3_BUCKET}")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            logger.info(f"Creating bucket: {S3_BUCKET}")
            if AWS_REGION == "us-east-1":
                s3_client.create_bucket(Bucket=S3_BUCKET)
            else:
                s3_client.create_bucket(
                    Bucket=S3_BUCKET,
                    CreateBucketConfiguration={
                        "LocationConstraint": AWS_REGION
                    }
                )
            logger.info(f"Bucket created: {S3_BUCKET}")
        else:
            raise


# ─────────────────────────────────────────────────────────────────
# DATA VALIDATION
# Validates each dataset before uploading to S3
# Catches data issues before they enter the pipeline
# In production: this prevents bad data from reaching Redshift
# ─────────────────────────────────────────────────────────────────

def validate_dataset(df, dataset_name, required_columns):
    """
    Validates a dataset before uploading to S3.
    Checks row count, required columns, and null rates.
    """
    logger.info(f"Validating {dataset_name}...")

    # Check minimum row count
    if len(df) == 0:
        raise ValueError(f"{dataset_name} is empty — no rows found")

    # Check required columns exist
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"{dataset_name} missing required columns: {missing_cols}"
        )

    # Log null rates for key columns
    for col in required_columns:
        null_rate = df[col].isnull().sum() / len(df) * 100
        if null_rate > 10:
            logger.warning(
                f"{dataset_name}.{col} has {null_rate:.1f}% null values"
            )

    logger.info(
        f"{dataset_name} validation passed — "
        f"{len(df):,} rows, {len(df.columns)} columns"
    )
    return True


# ─────────────────────────────────────────────────────────────────
# S3 UPLOAD
# Uploads DataFrame directly to S3 as CSV
# No temp file needed — uploads from memory using StringIO
# In production: large files use multipart upload automatically
# ─────────────────────────────────────────────────────────────────

def upload_dataframe_to_s3(s3_client, df, s3_key, dataset_name):
    """
    Uploads a pandas DataFrame directly to S3 as CSV.
    Uses StringIO buffer — no local temp file needed.
    Adds metadata tags for data lineage tracking.
    """
    logger.info(f"Uploading {dataset_name} to s3://{S3_BUCKET}/{s3_key}")

    # Convert DataFrame to CSV string in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Upload to S3 with metadata
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
        # Metadata for data lineage — tracks when and where data came from
        Metadata={
            "source":          "olist-kaggle-dataset",
            "upload_timestamp": datetime.utcnow().isoformat(),
            "row_count":       str(len(df)),
            "column_count":    str(len(df.columns)),
            "pipeline":        "ecommerce-airflow-aws-pipeline"
        }
    )

    logger.info(
        f"Successfully uploaded {dataset_name} — "
        f"{len(df):,} rows to s3://{S3_BUCKET}/{s3_key}"
    )


# ─────────────────────────────────────────────────────────────────
# DATASET DEFINITIONS
# Each dataset has its name, filename, S3 path, and required columns
# Adding a new dataset only requires adding one entry here
# ─────────────────────────────────────────────────────────────────

DATASETS = [
    {
        "name":             "Orders",
        "filename":         "olist_orders_dataset.csv",
        "s3_folder":        "orders",
        "required_columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp"
        ]
    },
    {
        "name":             "Customers",
        "filename":         "olist_customers_dataset.csv",
        "s3_folder":        "customers",
        "required_columns": [
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        ]
    },
    {
        "name":             "Products",
        "filename":         "olist_products_dataset.csv",
        "s3_folder":        "products",
        "required_columns": [
            "product_id",
            "product_category_name"
        ]
    },
    {
        "name":             "Payments",
        "filename":         "olist_order_payments_dataset.csv",
        "s3_folder":        "payments",
        "required_columns": [
            "order_id",
            "payment_type",
            "payment_value"
        ]
    },
    {
        "name":             "Reviews",
        "filename":         "olist_order_reviews_dataset.csv",
        "s3_folder":        "reviews",
        "required_columns": [
            "review_id",
            "order_id",
            "review_score"
        ]
    },
]


# ─────────────────────────────────────────────────────────────────
# MAIN UPLOAD FUNCTION
# Processes all datasets — validate then upload
# ─────────────────────────────────────────────────────────────────

def upload_all_datasets():
    """
    Main function — uploads all Olist datasets to S3.
    For each dataset: read CSV, validate, upload to S3.
    """
    logger.info("Starting e-commerce data upload to S3...")
    logger.info(f"Target bucket: s3://{S3_BUCKET}/{S3_PREFIX}/")

    s3_client = get_s3_client()
    ensure_bucket_exists(s3_client)

    upload_summary = []

    for dataset in DATASETS:
        try:
            # Read local CSV file
            local_path = os.path.join(DATA_DIR, dataset["filename"])

            if not os.path.exists(local_path):
                logger.warning(
                    f"File not found: {local_path} — skipping. "
                    f"Download from Kaggle first."
                )
                continue

            df = pd.read_csv(local_path)

            # Validate before uploading
            validate_dataset(
                df,
                dataset["name"],
                dataset["required_columns"]
            )

            # Build S3 key — partitioned by dataset type
            s3_key = (
                f"{S3_PREFIX}/"
                f"{dataset['s3_folder']}/"
                f"{dataset['filename']}"
            )

            # Upload to S3
            upload_dataframe_to_s3(
                s3_client,
                df,
                s3_key,
                dataset["name"]
            )

            upload_summary.append({
                "dataset":  dataset["name"],
                "rows":     len(df),
                "s3_path":  f"s3://{S3_BUCKET}/{s3_key}",
                "status":   "success"
            })

        except Exception as e:
            logger.error(
                f"Failed to upload {dataset['name']}: {str(e)}"
            )
            upload_summary.append({
                "dataset": dataset["name"],
                "status":  "failed",
                "error":   str(e)
            })

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("UPLOAD SUMMARY")
    logger.info("=" * 60)
    for item in upload_summary:
        if item["status"] == "success":
            logger.info(
                f"✓ {item['dataset']:15} | "
                f"{item['rows']:>8,} rows | "
                f"{item['s3_path']}"
            )
        else:
            logger.error(
                f"✗ {item['dataset']:15} | FAILED: {item['error']}"
            )

    successful = sum(1 for i in upload_summary if i["status"] == "success")
    logger.info(f"\n{successful}/{len(DATASETS)} datasets uploaded successfully")
    logger.info("=" * 60)


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("E-Commerce S3 Upload Script starting...")
    logger.info(
        "NOTE: Download Olist dataset from Kaggle first:\n"
        "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
    )
    upload_all_datasets()
