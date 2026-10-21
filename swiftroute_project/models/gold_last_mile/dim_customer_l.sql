{{
    config(
        schema = 'gold_last_mile',
        alias = 'dim_customer_l'
    )
}}

/*
    Customer dimension from Salesforce's account master.
    customer_key uses the persistent Silver Salesforce account_id because delivery
    rows already expose the matched account_id. client_id remains the cross-system ID.
    An explicit N/A member lets unmatched delivery rows retain referential integrity.
*/
WITH account_source AS (
    SELECT
        CAST(account_id AS STRING) AS account_id,
        CAST(source_account_id AS STRING) AS source_account_id,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(client_id AS STRING)), ''), 'N/A'), 'N/A') AS client_id,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(account_name AS STRING)), ''), 'N/A'), 'N/A') AS customer_name,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(account_type AS STRING)), ''), 'N/A'), 'N/A') AS account_type,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(industry AS STRING)), ''), 'N/A'), 'N/A') AS industry,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(account_tier AS STRING)), ''), 'N/A'), 'N/A') AS account_tier,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(primary_city AS STRING)), ''), 'N/A'), 'N/A') AS primary_city,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(contract_type AS STRING)), ''), 'N/A'), 'N/A') AS account_contract_type,
        CAST(is_fulfillment_client AS BOOLEAN) AS is_fulfillment_client,
        TRY_CAST(net_payment_terms AS INT) AS net_payment_terms,
        TRY_CAST(annual_revenue AS DECIMAL(18, 2)) AS annual_revenue,
        TRY_CAST(number_of_employees AS INT) AS number_of_employees
    FROM {{ ref('salesforce_accounts_t') }}
    WHERE account_id IS NOT NULL
      AND TRIM(CAST(account_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(account_id AS STRING))) <> 'N/A'
)
SELECT
    account_id AS customer_key,
    account_id,
    source_account_id,
    client_id,
    customer_name,
    account_type,
    industry,
    account_tier,
    primary_city,
    account_contract_type,
    is_fulfillment_client,
    net_payment_terms,
    annual_revenue,
    number_of_employees,
    FALSE AS is_unknown_customer
FROM account_source

UNION ALL

SELECT
    'N/A' AS customer_key,
    'N/A' AS account_id,
    'N/A' AS source_account_id,
    'N/A' AS client_id,
    'Unknown Customer' AS customer_name,
    'N/A' AS account_type,
    'N/A' AS industry,
    'N/A' AS account_tier,
    'N/A' AS primary_city,
    'N/A' AS account_contract_type,
    CAST(NULL AS BOOLEAN) AS is_fulfillment_client,
    CAST(NULL AS INT) AS net_payment_terms,
    CAST(NULL AS DECIMAL(18, 2)) AS annual_revenue,
    CAST(NULL AS INT) AS number_of_employees,
    TRUE AS is_unknown_customer
