{{ config(materialized='view', schema='gold_customer') }}

/*
Gold dimension: one current descriptive record per canonical SwiftRoute client.

Key design:
  - customer_key is a deterministic surrogate derived from canonical client_id.
  - client_id is retained as the business key.
  - The suffix _c avoids colliding with dim_customer_l in the last-mile mart.
  - Descriptive attributes come from the most recent customer-period row.
  - Historical service flags summarize activity represented in the Silver Business
    table; they are not a claim about activity outside the available source history.
*/

WITH customer_period_source AS (
    SELECT
        client_id,
        period_month,
        client_name,
        has_salesforce_account,
        account_type,
        industry,
        account_tier,
        primary_city,
        account_owner_name,
        is_fulfillment_client_configured,
        has_fulfillment_activity,
        has_last_mile_activity
    FROM {{ ref('silver_business_customer_period_integrated') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
),

latest_customer_context AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY client_id
            ORDER BY period_month DESC
        ) AS customer_row_num
    FROM customer_period_source
),

customer_service_history AS (
    SELECT
        client_id,
        MAX(CASE WHEN COALESCE(has_fulfillment_activity, FALSE) THEN 1 ELSE 0 END)
            AS ever_used_fulfillment_flag,
        MAX(CASE WHEN COALESCE(has_last_mile_activity, FALSE) THEN 1 ELSE 0 END)
            AS ever_used_last_mile_flag
    FROM customer_period_source
    GROUP BY client_id
),

customer_dimension AS (
    SELECT
        c.client_id,
        CASE
            WHEN c.client_name IS NULL
              OR LOWER(TRIM(c.client_name)) IN ('', 'n/a', 'null')
                THEN 'N/A'
            ELSE TRIM(c.client_name)
        END AS customer_name,
        COALESCE(c.has_salesforce_account, FALSE) AS has_salesforce_account,
        COALESCE(NULLIF(TRIM(c.account_type), ''), 'N/A') AS account_type,
        COALESCE(NULLIF(TRIM(c.industry), ''), 'N/A') AS industry,
        COALESCE(NULLIF(TRIM(c.account_tier), ''), 'N/A') AS account_tier,
        COALESCE(NULLIF(TRIM(c.primary_city), ''), 'N/A') AS primary_city,
        COALESCE(NULLIF(TRIM(c.account_owner_name), ''), 'N/A') AS account_owner_name,
        c.is_fulfillment_client_configured,
        CASE WHEN h.ever_used_fulfillment_flag = 1 THEN TRUE ELSE FALSE END
            AS has_ever_used_fulfillment,
        CASE WHEN h.ever_used_last_mile_flag = 1 THEN TRUE ELSE FALSE END
            AS has_ever_used_last_mile,
        CASE
            WHEN h.ever_used_fulfillment_flag = 1
             AND h.ever_used_last_mile_flag = 1
                THEN 'fulfillment_and_last_mile_observed'
            WHEN h.ever_used_fulfillment_flag = 1
                THEN 'fulfillment_only_observed'
            WHEN h.ever_used_last_mile_flag = 1
                THEN 'last_mile_only_observed'
            ELSE 'no_observed_operational_activity'
        END AS observed_service_relationship
    FROM latest_customer_context c
    INNER JOIN customer_service_history h
        ON c.client_id = h.client_id
    WHERE c.customer_row_num = 1
)

SELECT
    SHA2(client_id, 256) AS customer_key,
    client_id,
    customer_name,
    has_salesforce_account,
    account_type,
    industry,
    account_tier,
    primary_city,
    account_owner_name,
    is_fulfillment_client_configured,
    has_ever_used_fulfillment,
    has_ever_used_last_mile,
    observed_service_relationship
FROM customer_dimension
