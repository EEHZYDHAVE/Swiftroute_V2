{{
    config(
        schema = 'gold_last_mile',
        alias = 'dim_contract_l'
    )
}}

/*
    Contract dimension uses the persistent contract_id from Silver Salesforce.
    All contracts are retained for historical lookup; fact_delivery links only the
    unique contract that Silver Business determined was valid on its delivery event date.
*/
WITH contract_source AS (
    SELECT
        CAST(contract_id AS STRING) AS contract_key,
        CAST(source_contract_id AS STRING) AS source_contract_id,
        CAST(account_id AS STRING) AS account_id,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(client_id AS STRING)), ''), 'N/A'), 'N/A') AS client_id,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(account_name AS STRING)), ''), 'N/A'), 'N/A') AS customer_name,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(contract_status AS STRING)), ''), 'N/A'), 'N/A') AS contract_status,
        TRY_CAST(start_date AS DATE) AS start_date,
        TRY_CAST(end_date AS DATE) AS end_date,
        TRY_CAST(signed_date AS DATE) AS signed_date,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(contract_type AS STRING)), ''), 'N/A'), 'N/A') AS contract_type,
        TRY_CAST(contract_term_months AS INT) AS contract_term_months,
        TRY_CAST(committed_monthly_volume AS BIGINT) AS committed_monthly_volume,
        TRY_CAST(discount_rate AS DECIMAL(8, 4)) AS discount_rate,
        TRY_CAST(net_payment_terms AS INT) AS net_payment_terms,
        CAST(auto_renewal AS BOOLEAN) AS auto_renewal,
        TRY_CAST(termination_notice_days AS INT) AS termination_notice_days,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(primary_city AS STRING)), ''), 'N/A'), 'N/A') AS primary_city,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(account_tier AS STRING)), ''), 'N/A'), 'N/A') AS account_tier,
        CAST(is_orphaned_contract AS BOOLEAN) AS is_orphaned_contract
    FROM {{ ref('salesforce_contracts_t') }}
    WHERE contract_id IS NOT NULL
      AND TRIM(CAST(contract_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(contract_id AS STRING))) <> 'N/A'
)
SELECT
    contract_key,
    source_contract_id,
    account_id,
    client_id,
    customer_name,
    contract_status,
    start_date,
    end_date,
    signed_date,
    contract_type,
    contract_term_months,
    committed_monthly_volume,
    discount_rate,
    net_payment_terms,
    auto_renewal,
    termination_notice_days,
    primary_city,
    account_tier,
    is_orphaned_contract,
    FALSE AS is_unknown_contract
FROM contract_source

UNION ALL

SELECT
    'N/A' AS contract_key,
    'N/A' AS source_contract_id,
    'N/A' AS account_id,
    'N/A' AS client_id,
    'Unknown Customer' AS customer_name,
    'N/A' AS contract_status,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    CAST(NULL AS DATE) AS signed_date,
    'N/A' AS contract_type,
    CAST(NULL AS INT) AS contract_term_months,
    CAST(NULL AS BIGINT) AS committed_monthly_volume,
    CAST(NULL AS DECIMAL(8, 4)) AS discount_rate,
    CAST(NULL AS INT) AS net_payment_terms,
    CAST(NULL AS BOOLEAN) AS auto_renewal,
    CAST(NULL AS INT) AS termination_notice_days,
    'N/A' AS primary_city,
    'N/A' AS account_tier,
    CAST(NULL AS BOOLEAN) AS is_orphaned_contract,
    TRUE AS is_unknown_contract
