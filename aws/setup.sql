-- setup.sql
-- PURPOSE: Complete AWS Redshift infrastructure setup for the
--          e-commerce sales analytics pipeline
--
-- Run this ONCE before the Airflow DAG executes for the first time
-- All statements use IF NOT EXISTS -- safe to run multiple times
--
-- Redshift is different from standard SQL databases:
-- DISTKEY controls how data is distributed across compute nodes
-- SORTKEY controls how data is sorted on disk for fast queries
-- These settings dramatically affect query performance at scale
-- In production: wrong DISTKEY = 10x slower queries

-- ─────────────────────────────────────────────────────────────────
-- STEP 1: DATABASE AND SCHEMA SETUP
-- ─────────────────────────────────────────────────────────────────

-- Create dedicated database for e-commerce pipeline
CREATE DATABASE IF NOT EXISTS ecommerce_analytics;

-- Raw layer -- exact copy of S3 data, no transformations
CREATE SCHEMA IF NOT EXISTS ecommerce_raw;

-- Staging layer -- cleaned data from dbt staging models
CREATE SCHEMA IF NOT EXISTS ecommerce_staging;

-- Marts layer -- business-ready analytics from dbt mart models
CREATE SCHEMA IF NOT EXISTS ecommerce_marts;


-- ─────────────────────────────────────────────────────────────────
-- STEP 2: RAW TABLES
-- One table per source dataset
-- All columns VARCHAR to accept any raw data -- dbt cleans types
-- DISTKEY on join columns for query performance
-- SORTKEY on timestamp columns for time-range query performance
-- ─────────────────────────────────────────────────────────────────

-- Raw orders table
-- DISTKEY on customer_id: queries joining orders + customers are fast
-- SORTKEY on order_purchase_timestamp: time-range queries are fast
CREATE TABLE IF NOT EXISTS ecommerce_raw.raw_orders (
    order_id                        VARCHAR(50),
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(20),
    order_purchase_timestamp        VARCHAR(30),
    order_approved_at               VARCHAR(30),
    order_delivered_carrier_date    VARCHAR(30),
    order_delivered_customer_date   VARCHAR(30),
    order_estimated_delivery_date   VARCHAR(30),
    -- Pipeline audit columns
    _loaded_at                      TIMESTAMP DEFAULT GETDATE(),
    _source_file                    VARCHAR(200)
)
DISTKEY(customer_id)
SORTKEY(order_purchase_timestamp);


-- Raw customers table
-- DISTKEY on customer_id: matches orders DISTKEY -- co-located joins
-- Co-location means Redshift does NOT shuffle data across nodes
-- This is the most important Redshift optimization concept
CREATE TABLE IF NOT EXISTS ecommerce_raw.raw_customers (
    customer_id             VARCHAR(50),
    customer_unique_id      VARCHAR(50),
    customer_zip_code       VARCHAR(10),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(5),
    _loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(customer_id)
SORTKEY(customer_unique_id);


-- Raw products table
-- DISTSTYLE ALL: small dimension table replicated on all nodes
-- Small lookup tables should use DISTSTYLE ALL for fast joins
CREATE TABLE IF NOT EXISTS ecommerce_raw.raw_products (
    product_id                  VARCHAR(50),
    product_category_name       VARCHAR(100),
    product_name_length         INTEGER,
    product_description_length  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            FLOAT,
    product_length_cm           FLOAT,
    product_height_cm           FLOAT,
    product_width_cm            FLOAT,
    _loaded_at                  TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL;


-- Raw payments table
CREATE TABLE IF NOT EXISTS ecommerce_raw.raw_payments (
    order_id                VARCHAR(50),
    payment_sequential      INTEGER,
    payment_type            VARCHAR(30),
    payment_installments    INTEGER,
    payment_value           FLOAT,
    _loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(order_id)
SORTKEY(order_id);


-- Raw reviews table
CREATE TABLE IF NOT EXISTS ecommerce_raw.raw_reviews (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            INTEGER,
    review_comment_title    VARCHAR(500),
    review_comment_message  VARCHAR(5000),
    review_creation_date    VARCHAR(30),
    review_answer_timestamp VARCHAR(30),
    _loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(order_id)
SORTKEY(order_id);


-- ─────────────────────────────────────────────────────────────────
-- STEP 3: REDSHIFT SPECTRUM EXTERNAL SCHEMA
-- Spectrum lets Redshift query S3 files directly via SQL
-- No COPY command needed -- query raw S3 data like a table
-- In production: used for historical data that does not
-- need to live in Redshift permanently
-- ─────────────────────────────────────────────────────────────────

-- Create external schema pointing to AWS Glue Data Catalog
-- This connects Redshift to our Glue crawler output
-- Uncomment when AWS Glue catalog is set up
/*
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_ecommerce
FROM DATA CATALOG
DATABASE 'ecommerce_glue_db'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
*/


-- ─────────────────────────────────────────────────────────────────
-- STEP 4: MONITORING AND MAINTENANCE QUERIES
-- Use these to monitor pipeline health in production
-- ─────────────────────────────────────────────────────────────────

-- Check row counts across all raw tables
-- Run after every pipeline execution to verify data loaded
/*
SELECT 'raw_orders'    AS table_name, COUNT(*) AS row_count, MAX(_loaded_at) AS last_load FROM ecommerce_raw.raw_orders
UNION ALL
SELECT 'raw_customers' AS table_name, COUNT(*) AS row_count, MAX(_loaded_at) AS last_load FROM ecommerce_raw.raw_customers
UNION ALL
SELECT 'raw_products'  AS table_name, COUNT(*) AS row_count, MAX(_loaded_at) AS last_load FROM ecommerce_raw.raw_products
UNION ALL
SELECT 'raw_payments'  AS table_name, COUNT(*) AS row_count, MAX(_loaded_at) AS last_load FROM ecommerce_raw.raw_payments
UNION ALL
SELECT 'raw_reviews'   AS table_name, COUNT(*) AS row_count, MAX(_loaded_at) AS last_load FROM ecommerce_raw.raw_reviews
ORDER BY table_name;
*/

-- Check Redshift query performance
-- Identifies slow queries that need optimization
/*
SELECT
    query,
    TRIM(querytxt)      AS query_text,
    starttime,
    endtime,
    DATEDIFF(seconds, starttime, endtime) AS duration_seconds
FROM stl_query
WHERE starttime >= DATEADD(day, -1, GETDATE())
ORDER BY duration_seconds DESC
LIMIT 20;
*/

-- Check table distribution health
-- Identifies tables with data skew across nodes
/*
SELECT
    tablename,
    SUM(rows)           AS total_rows,
    COUNT(DISTINCT slice) AS node_slices,
    MAX(rows)           AS max_rows_per_slice,
    MIN(rows)           AS min_rows_per_slice,
    ROUND(
        100.0 * (MAX(rows) - MIN(rows)) / NULLIF(MAX(rows), 0),
        1
    )                   AS skew_pct
FROM svv_diskusage
WHERE name NOT LIKE 'pg_%'
GROUP BY tablename
ORDER BY skew_pct DESC;
*/

-- ─────────────────────────────────────────────────────────────────
-- STEP 5: GRANTS
-- Role-based access control
-- Data engineers read/write, analysts read only
-- ─────────────────────────────────────────────────────────────────

-- Uncomment in team environment
-- GRANT USAGE ON SCHEMA ecommerce_raw      TO GROUP data_engineers;
-- GRANT USAGE ON SCHEMA ecommerce_staging  TO GROUP data_engineers;
-- GRANT USAGE ON SCHEMA ecommerce_marts    TO GROUP data_engineers;
-- GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce_marts TO GROUP analysts;
