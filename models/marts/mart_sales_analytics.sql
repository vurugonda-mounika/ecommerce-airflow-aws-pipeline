-- mart_sales_analytics.sql
-- PURPOSE: Business-ready e-commerce sales analytics with
--          RFM customer segmentation and sales performance metrics
--
-- RFM = Recency, Frequency, Monetary
-- The gold standard customer analytics framework used by
-- Amazon, Walmart, Target, and every major retailer worldwide
--
-- Creates two analytical outputs:
-- 1. Customer RFM scores and segments
-- 2. Sales performance by category, state, and time period
--
-- In real e-commerce work: this exact layer feeds marketing
-- dashboards, customer retention programs, and executive reports
-- Same staging to mart pattern used at LetQuickly for property
-- analytics -- applied here to retail customer data

WITH staged_orders AS (

    -- Read from staging model
    SELECT * FROM {{ ref('stg_orders') }}

),

-- ─────────────────────────────────────────────────────────────────
-- PART 1: RFM CALCULATION
-- For each customer calculate R, F, M values
-- ─────────────────────────────────────────────────────────────────

-- Only delivered orders count for RFM
-- Cancelled or pending orders do not reflect real customer behavior
delivered_orders AS (

    SELECT *
    FROM staged_orders
    WHERE order_status = 'DELIVERED'
    AND   order_value > 0
    AND   order_purchase_timestamp IS NOT NULL

),

-- Calculate raw RFM values per customer
rfm_raw AS (

    SELECT
        customer_unique_id,
        customer_state,
        customer_city,

        -- RECENCY: days since last purchase
        -- Lower number = more recent = better customer
        DATEDIFF(
            'day',
            MAX(order_purchase_timestamp),
            GETDATE()
        )                                       AS recency_days,

        -- FREQUENCY: total number of orders placed
        -- Higher number = more loyal customer
        COUNT(DISTINCT order_id)                AS frequency_orders,

        -- MONETARY: total amount spent
        -- Higher number = more valuable customer
        SUM(order_value)                        AS monetary_total,
        AVG(order_value)                        AS monetary_avg,

        -- Additional customer metrics
        MAX(order_purchase_timestamp)           AS last_order_date,
        MIN(order_purchase_timestamp)           AS first_order_date,
        DATEDIFF(
            'day',
            MIN(order_purchase_timestamp),
            MAX(order_purchase_timestamp)
        )                                       AS customer_lifespan_days,

        -- Delivery experience
        ROUND(
            100.0 * SUM(CASE WHEN delivered_on_time THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0), 1
        )                                       AS on_time_delivery_rate,

        AVG(delivery_days)                      AS avg_delivery_days,
        COUNT(DISTINCT primary_payment_type)    AS payment_types_used

    FROM delivered_orders
    GROUP BY
        customer_unique_id,
        customer_state,
        customer_city

),

-- ─────────────────────────────────────────────────────────────────
-- RFM SCORING
-- Score each customer 1-5 on each dimension
-- 5 = best, 1 = worst
-- Uses NTILE(5) window function to create equal-sized buckets
-- ─────────────────────────────────────────────────────────────────

rfm_scored AS (

    SELECT
        customer_unique_id,
        customer_state,
        customer_city,
        recency_days,
        frequency_orders,
        monetary_total,
        monetary_avg,
        last_order_date,
        first_order_date,
        customer_lifespan_days,
        on_time_delivery_rate,
        avg_delivery_days,
        payment_types_used,

        -- RECENCY SCORE: recent buyers get 5, oldest buyers get 1
        -- NOTE: lower recency_days = more recent = better score
        -- So we reverse the NTILE order for recency
        6 - NTILE(5) OVER (ORDER BY recency_days ASC)   AS recency_score,

        -- FREQUENCY SCORE: more orders = higher score
        NTILE(5) OVER (ORDER BY frequency_orders ASC)   AS frequency_score,

        -- MONETARY SCORE: higher spend = higher score
        NTILE(5) OVER (ORDER BY monetary_total ASC)     AS monetary_score

    FROM rfm_raw

),

-- ─────────────────────────────────────────────────────────────────
-- RFM SEGMENTATION
-- Combine scores to classify customers into business segments
-- These segments directly drive marketing and retention decisions
-- ─────────────────────────────────────────────────────────────────

rfm_segmented AS (

    SELECT
        *,

        -- Combined RFM score -- max 15, min 3
        (recency_score + frequency_score + monetary_score) AS rfm_total_score,

        -- Customer segment classification
        -- Based on industry-standard RFM segmentation rules
        CASE
            -- Champions: bought recently, buy often, spend the most
            WHEN recency_score >= 4
             AND frequency_score >= 4
             AND monetary_score >= 4
            THEN 'CHAMPION'

            -- Loyal customers: buy often but not always recent
            WHEN frequency_score >= 4
             AND monetary_score >= 3
            THEN 'LOYAL_CUSTOMER'

            -- Potential loyalists: recent buyers with average frequency
            WHEN recency_score >= 4
             AND frequency_score BETWEEN 2 AND 3
            THEN 'POTENTIAL_LOYALIST'

            -- Recent customers: bought recently but not often yet
            WHEN recency_score >= 4
             AND frequency_score <= 2
            THEN 'RECENT_CUSTOMER'

            -- At risk: used to buy often but not recently
            WHEN recency_score <= 2
             AND frequency_score >= 3
            THEN 'AT_RISK'

            -- Cannot lose them: high value but not buying recently
            WHEN recency_score <= 2
             AND monetary_score >= 4
            THEN 'CANNOT_LOSE'

            -- Hibernating: low recency, low frequency
            WHEN recency_score <= 2
             AND frequency_score <= 2
            THEN 'HIBERNATING'

            -- Promising: recent but low spend so far
            WHEN recency_score >= 3
             AND monetary_score <= 2
            THEN 'PROMISING'

            ELSE 'NEEDS_ATTENTION'
        END                                             AS customer_segment

    FROM rfm_scored

),

-- ─────────────────────────────────────────────────────────────────
-- PART 2: SALES PERFORMANCE SUMMARY
-- Aggregated by state and month for trend analysis
-- ─────────────────────────────────────────────────────────────────

sales_by_state_month AS (

    SELECT
        customer_state,
        order_month,
        order_year,

        -- Volume metrics
        COUNT(DISTINCT order_id)            AS total_orders,
        COUNT(DISTINCT customer_unique_id)  AS unique_customers,

        -- Revenue metrics
        SUM(order_value)                    AS total_revenue,
        AVG(order_value)                    AS avg_order_value,
        MEDIAN(order_value)                 AS median_order_value,

        -- Order value distribution
        SUM(CASE WHEN order_value_tier = 'HIGH_VALUE'
            THEN 1 ELSE 0 END)             AS high_value_orders,
        SUM(CASE WHEN order_value_tier = 'MEDIUM_VALUE'
            THEN 1 ELSE 0 END)             AS medium_value_orders,
        SUM(CASE WHEN order_value_tier = 'LOW_VALUE'
            THEN 1 ELSE 0 END)             AS low_value_orders,

        -- Delivery performance
        ROUND(
            100.0 * SUM(CASE WHEN delivered_on_time
                THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0), 1
        )                                   AS on_time_delivery_pct,

        AVG(delivery_days)                  AS avg_delivery_days,

        -- Payment insights
        SUM(CASE WHEN primary_payment_type = 'credit_card'
            THEN 1 ELSE 0 END)             AS credit_card_orders,
        SUM(CASE WHEN primary_payment_type = 'boleto'
            THEN 1 ELSE 0 END)             AS boleto_orders

    FROM staged_orders
    WHERE order_status = 'DELIVERED'
    GROUP BY
        customer_state,
        order_month,
        order_year

),

-- ─────────────────────────────────────────────────────────────────
-- FINAL OUTPUT
-- Join customer RFM with their state-level sales context
-- ─────────────────────────────────────────────────────────────────

final AS (

    SELECT
        -- Customer identity
        r.customer_unique_id,
        r.customer_state,
        r.customer_city,

        -- RFM values
        r.recency_days,
        r.frequency_orders,
        r.monetary_total,
        r.monetary_avg,

        -- RFM scores
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total_score,

        -- Customer segment
        r.customer_segment,

        -- Customer timeline
        r.first_order_date,
        r.last_order_date,
        r.customer_lifespan_days,

        -- Experience metrics
        r.on_time_delivery_rate,
        r.avg_delivery_days,
        r.payment_types_used,

        -- State context this month
        s.total_orders          AS state_orders_this_month,
        s.total_revenue         AS state_revenue_this_month,
        s.on_time_delivery_pct  AS state_on_time_pct

    FROM rfm_segmented r
    LEFT JOIN sales_by_state_month s
        ON  r.customer_state = s.customer_state
        AND s.order_month = DATE_TRUNC(
                'month', GETDATE()
            )

    -- Champions and at-risk customers first
    ORDER BY
        CASE r.customer_segment
            WHEN 'CHAMPION'           THEN 1
            WHEN 'CANNOT_LOSE'        THEN 2
            WHEN 'AT_RISK'            THEN 3
            WHEN 'LOYAL_CUSTOMER'     THEN 4
            WHEN 'POTENTIAL_LOYALIST' THEN 5
            WHEN 'RECENT_CUSTOMER'    THEN 6
            WHEN 'PROMISING'          THEN 7
            WHEN 'NEEDS_ATTENTION'    THEN 8
            WHEN 'HIBERNATING'        THEN 9
            ELSE 10
        END,
        r.rfm_total_score DESC

)

SELECT * FROM final
