{{
    config(
        schema = 'gold_fulfillment',
        alias = 'dim_customer'
    )
}}

/*
    Customer dimension keyed by SwiftRoute's shared client_id.
    Includes Salesforce accounts even when they have no fulfillment movement,
    plus transaction-observed client IDs that do not match a Salesforce account.
    N/A is an explicit unknown member so every fact row can join to the dimension.
*/

WITH account_ranked AS (
    SELECT
        TRIM(CAST(client_id AS STRING)) AS customer_key,
        TRIM(CAST(client_id AS STRING)) AS client_id,
        CAST(account_id AS STRING) AS account_id,
        CAST(account_name AS STRING) AS customer_name,
        CAST(account_type AS STRING) AS account_type,
        CAST(industry AS STRING) AS industry,
        CAST(account_tier AS STRING) AS customer_tier,
        CAST(primary_city AS STRING) AS primary_city,
        TRY_CAST(is_fulfillment_client AS BOOLEAN) AS is_fulfillment_client,
        TRY_CAST(contracted_monthly_volume AS DECIMAL(18, 2)) AS contracted_monthly_volume,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(source_updated_at_utc AS TIMESTAMP) AS source_updated_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(CAST(client_id AS STRING))
            ORDER BY source_updated_at_utc DESC NULLS LAST, account_id ASC
        ) AS account_rank
    FROM {{ ref('salesforce_accounts_t') }}
    WHERE client_id IS NOT NULL
      AND TRIM(CAST(client_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(client_id AS STRING))) <> 'N/A'
),
account_one_row AS (
    SELECT *
    FROM account_ranked
    WHERE account_rank = 1
),

observed_clients AS (
    SELECT
        TRIM(CAST(client_id AS STRING)) AS customer_key,
        MAX(NULLIF(TRIM(CAST(client_name AS STRING)), 'N/A')) AS customer_name,
        MAX(NULLIF(TRIM(CAST(account_id AS STRING)), 'N/A')) AS account_id,
        MAX(NULLIF(TRIM(CAST(account_type AS STRING)), 'N/A')) AS account_type,
        MAX(NULLIF(TRIM(CAST(client_industry AS STRING)), 'N/A')) AS industry,
        MAX(NULLIF(TRIM(CAST(client_tier AS STRING)), 'N/A')) AS customer_tier,
        MAX(NULLIF(TRIM(CAST(client_primary_city AS STRING)), 'N/A')) AS primary_city,
        MAX(CASE
                WHEN is_fulfillment_client = TRUE THEN 1
                WHEN is_fulfillment_client = FALSE THEN 0
                ELSE NULL
            END) AS fulfillment_client_int,
        MAX(TRY_CAST(account_contracted_monthly_volume AS DECIMAL(18, 2)))
            AS contracted_monthly_volume,
        MAX(NULLIF(TRIM(CAST(account_contract_type AS STRING)), 'N/A')) AS contract_type
    FROM {{ ref('silver_business_fufillment') }}
    WHERE client_id IS NOT NULL
      AND TRIM(CAST(client_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(client_id AS STRING))) <> 'N/A'
    GROUP BY TRIM(CAST(client_id AS STRING))
),

all_customer_keys AS (
    SELECT customer_key FROM account_one_row
    UNION
    SELECT customer_key FROM observed_clients
),
resolved_customers AS (
    SELECT
        k.customer_key,
        k.customer_key AS client_id,
        COALESCE(NULLIF(a.account_id, 'N/A'), o.account_id, 'N/A') AS account_id,
        COALESCE(NULLIF(a.customer_name, ''), o.customer_name, 'N/A') AS customer_name,
        COALESCE(NULLIF(a.account_type, ''), o.account_type, 'N/A') AS account_type,
        COALESCE(NULLIF(a.industry, ''), o.industry, 'N/A') AS industry,
        COALESCE(NULLIF(a.customer_tier, ''), o.customer_tier, 'N/A') AS customer_tier,
        COALESCE(NULLIF(a.primary_city, ''), o.primary_city, 'N/A') AS primary_city,
        CASE
            WHEN a.is_fulfillment_client IS NOT NULL THEN a.is_fulfillment_client
            WHEN o.fulfillment_client_int = 1 THEN TRUE
            WHEN o.fulfillment_client_int = 0 THEN FALSE
            ELSE CAST(NULL AS BOOLEAN)
        END AS is_fulfillment_client,
        COALESCE(a.contracted_monthly_volume, o.contracted_monthly_volume)
            AS contracted_monthly_volume,
        COALESCE(NULLIF(a.contract_type, ''), o.contract_type, 'N/A') AS contract_type,
        a.source_updated_at_utc
    FROM all_customer_keys k
    LEFT JOIN account_one_row a ON k.customer_key = a.customer_key
    LEFT JOIN observed_clients o ON k.customer_key = o.customer_key
)

SELECT
    customer_key,
    client_id,
    account_id,
    customer_name,
    account_type,
    industry,
    customer_tier,
    primary_city,
    is_fulfillment_client,
    contracted_monthly_volume,
    contract_type,
    source_updated_at_utc,
    FALSE AS is_unknown_customer
FROM resolved_customers

UNION ALL

SELECT
    'N/A' AS customer_key,
    'N/A' AS client_id,
    'N/A' AS account_id,
    'N/A' AS customer_name,
    'N/A' AS account_type,
    'N/A' AS industry,
    'N/A' AS customer_tier,
    'N/A' AS primary_city,
    CAST(NULL AS BOOLEAN) AS is_fulfillment_client,
    CAST(NULL AS DECIMAL(18, 2)) AS contracted_monthly_volume,
    'N/A' AS contract_type,
    CAST(NULL AS TIMESTAMP) AS source_updated_at_utc,
    TRUE AS is_unknown_customer
