-- stg_orders.sql
-- PURPOSE: Clean and standardize raw e-commerce orders data
--          from Redshift raw layer loaded via S3 and Airflow
--
-- This is the STAGING layer -- first dbt transformation step
-- Raw data from S3/Redshift has no type guarantees
-- We enforce data types, clean nulls, and add business flags here
-- before any RFM or sales analytics logic runs in the mart layer
--
-- In real e-commerce work: same pattern used at every major
-- retailer -- raw orders land in warehouse, staging cleans them,
-- mart layer builds customer analytics on top

WITH raw_orders AS (

    SELECT * FROM {{ source('ecommerce_raw', 'raw_orders') }}

),

raw_customers AS (

    SELECT * FROM {{ source('ecommerce_raw', 'raw_customers') }}

),

raw_payments AS (

    SELECT * FROM {{ source('ecommerce_raw', 'raw_payments') }}

),

-- ─────────────────────────────────────────────────────────────────
-- CLEAN ORDERS
-- Cast VARCHAR columns to proper types
-- Redshift raw table stores everything as VARCHAR
-- because we do not know source data quality upfront
-- ─────────────────────────────────────────────────────────────────

cleaned_orders AS (

    SELECT
        -- Order identifiers
        TRIM(order_id)                                      AS order_id,
        TRIM(customer_id)                                   AS customer_id,

        -- Order status -- standardize to uppercase
        UPPER(TRIM(order_status))                           AS order_status,

        -- Cast timestamp strings to proper TIMESTAMP type
        -- TRY_TO_TIMESTAMP is Redshift-safe -- returns NULL on bad values
        -- instead of crashing the entire pipeline
        TRY_TO_TIMESTAMP(
            order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS'
        )                                                   AS order_purchase_timestamp,

        TRY_TO_TIMESTAMP(
            order_approved_at, 'YYYY-MM-DD HH24:MI:SS'
        )                                                   AS order_approved_at,

        TRY_TO_TIMESTAMP(
            order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS'
        )                                                   AS order_delivered_carrier_date,

        TRY_TO_TIMESTAMP(
            order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS'
        )                                                   AS order_delivered_customer_date,

        TRY_TO_TIMESTAMP(
            order_estimated_delivery_date, 'YYYY-MM-DD HH24:MI:SS'
        )                                                   AS order_estimated_delivery_date,

        -- Derived date dimensions for analytics
        DATE_TRUNC('day',
            TRY_TO_TIMESTAMP(order_purchase_timestamp,
            'YYYY-MM-DD HH24:MI:SS')
        )                                                   AS order_date,

        DATE_TRUNC('month',
            TRY_TO_TIMESTAMP(order_purchase_timestamp,
            'YYYY-MM-DD HH24:MI:SS')
        )                                                   AS order_month,

        DATE_TRUNC('year',
            TRY_TO_TIMESTAMP(order_purchase_timestamp,
            'YYYY-MM-DD HH24:MI:SS')
        )                                                   AS order_year,

        -- Delivery performance flags
        -- Was the order delivered on time?
        CASE
            WHEN order_delivered_customer_date IS NOT NULL
             AND order_estimated_delivery_date IS NOT NULL
             AND TRY_TO_TIMESTAMP(order_delivered_customer_date,
                 'YYYY-MM-DD HH24:MI:SS')
                 <= TRY_TO_TIMESTAMP(order_estimated_delivery_date,
                    'YYYY-MM-DD HH24:MI:SS')
            THEN TRUE
            ELSE FALSE
        END                                                 AS delivered_on_time,

        -- How many days did delivery take?
        DATEDIFF(
            'day',
            TRY_TO_TIMESTAMP(order_purchase_timestamp,
                'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP(order_delivered_customer_date,
                'YYYY-MM-DD HH24:MI:SS')
        )                                                   AS delivery_days,

        -- Order approval lag -- how long to approve after purchase?
        DATEDIFF(
            'hour',
            TRY_TO_TIMESTAMP(order_purchase_timestamp,
                'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP(order_approved_at,
                'YYYY-MM-DD HH24:MI:SS')
        )                                                   AS approval_lag_hours,

        -- Pipeline metadata
        _loaded_at                                          AS raw_loaded_at

    FROM raw_orders

    -- Remove records with no order ID -- unusable
    WHERE TRIM(order_id) IS NOT NULL
    AND   TRIM(order_id) != ''

    -- Remove records with no customer ID -- unusable
    AND   TRIM(customer_id) IS NOT NULL
    AND   TRIM(customer_id) != ''

    -- Remove records with no purchase timestamp -- cannot analyze
    AND   order_purchase_timestamp IS NOT NULL
    AND   TRIM(order_purchase_timestamp) != ''

),

-- ─────────────────────────────────────────────────────────────────
-- CLEAN CUSTOMERS
-- Add customer geographic dimensions
-- ─────────────────────────────────────────────────────────────────

cleaned_customers AS (

    SELECT
        TRIM(customer_id)           AS customer_id,
        TRIM(customer_unique_id)    AS customer_unique_id,
        TRIM(customer_city)         AS customer_city,
        UPPER(TRIM(customer_state)) AS customer_state,
        TRIM(customer_zip_code)     AS customer_zip_code
    FROM raw_customers
    WHERE TRIM(customer_id) IS NOT NULL

),

-- ─────────────────────────────────────────────────────────────────
-- AGGREGATE PAYMENTS PER ORDER
-- One row per order with total payment value
-- Raw payments has multiple rows per order
-- (installments, multiple payment methods)
-- ─────────────────────────────────────────────────────────────────

order_payments AS (

    SELECT
        TRIM(order_id)                  AS order_id,
        SUM(payment_value)              AS total_payment_value,
        MAX(payment_installments)       AS max_installments,
        COUNT(DISTINCT payment_type)    AS payment_methods_used,

        -- Most used payment type per order
        -- In real work: helps fraud detection and customer segmentation
        MAX(CASE
            WHEN payment_value = (
                SELECT MAX(p2.payment_value)
                FROM ecommerce_raw.raw_payments p2
                WHERE p2.order_id = raw_payments.order_id
            )
            THEN payment_type
        END)                            AS primary_payment_type

    FROM raw_payments
    WHERE TRIM(order_id) IS NOT NULL
    AND   payment_value > 0
    GROUP BY TRIM(order_id)

),

-- ─────────────────────────────────────────────────────────────────
-- FINAL JOIN
-- Bring together orders + customers + payments
-- One row per order with all relevant dimensions
-- ─────────────────────────────────────────────────────────────────

final AS (

    SELECT
        -- Order details
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.order_date,
        o.order_month,
        o.order_year,
        o.delivered_on_time,
        o.delivery_days,
        o.approval_lag_hours,

        -- Customer dimensions
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        c.customer_zip_code,

        -- Payment details
        COALESCE(p.total_payment_value, 0)  AS order_value,
        p.max_installments,
        p.payment_methods_used,
        p.primary_payment_type,

        -- Order value tier -- used for customer segmentation
        CASE
            WHEN COALESCE(p.total_payment_value, 0) >= 500
                THEN 'HIGH_VALUE'
            WHEN COALESCE(p.total_payment_value, 0) >= 100
                THEN 'MEDIUM_VALUE'
            WHEN COALESCE(p.total_payment_value, 0) > 0
                THEN 'LOW_VALUE'
            ELSE 'ZERO_VALUE'
        END                                 AS order_value_tier,

        -- Pipeline audit
        o.raw_loaded_at

    FROM cleaned_orders o
    LEFT JOIN cleaned_customers c
        ON o.customer_id = c.customer_id
    LEFT JOIN order_payments p
        ON o.order_id = p.order_id

)

SELECT * FROM final
