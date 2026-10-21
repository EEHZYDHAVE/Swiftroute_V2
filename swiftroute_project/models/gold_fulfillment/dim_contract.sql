{{
    config(
        schema = 'gold_fulfillment',
        alias = 'dim_contract'
    )
}}

/*
    Contract dimension replaces the proposed dim_service for this release.
    The sources expose client contracts and price-rate context, but do not prove
    which billable service was performed by each stock transaction. A service
    dimension would imply a transaction-to-service assignment that is not
    supported by the current data. Contract identity, in contrast, is matched
    to each movement as of the movement date in Silver Business.
*/

WITH contract_ranked AS (
    SELECT
        TRIM(CAST(contract_id AS STRING)) AS contract_key,
        TRIM(CAST(contract_id AS STRING)) AS contract_id,
        COALESCE(NULLIF(TRIM(CAST(client_id AS STRING)), ''), 'N/A') AS client_id,
        COALESCE(NULLIF(TRIM(CAST(contract_status AS STRING)), ''), 'N/A') AS contract_status,
        TRY_CAST(start_date AS DATE) AS contract_start_date,
        TRY_CAST(end_date AS DATE) AS contract_end_date,
        TRY_CAST(signed_date AS DATE) AS contract_signed_date,
        COALESCE(NULLIF(TRIM(CAST(contract_type AS STRING)), ''), 'N/A') AS contract_type,
        TRY_CAST(committed_monthly_volume AS DECIMAL(18, 2)) AS committed_monthly_volume,
        TRY_CAST(discount_rate AS DECIMAL(18, 6)) AS discount_rate,
        TRY_CAST(net_payment_terms AS INT) AS net_payment_terms_days,
        COALESCE(NULLIF(TRIM(CAST(primary_city AS STRING)), ''), 'N/A') AS primary_city,
        CAST(source_updated_at_utc AS TIMESTAMP) AS source_updated_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(CAST(contract_id AS STRING))
            ORDER BY source_updated_at_utc DESC NULLS LAST,
                     TRIM(CAST(contract_id AS STRING)) ASC
        ) AS contract_rank
    FROM {{ ref('salesforce_contracts_t') }}
    WHERE contract_id IS NOT NULL
      AND TRIM(CAST(contract_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(contract_id AS STRING))) <> 'N/A'
),
contracts_one_row AS (
    SELECT * FROM contract_ranked WHERE contract_rank = 1
)

SELECT
    contract_key,
    contract_id,
    client_id,
    contract_status,
    contract_start_date,
    contract_end_date,
    contract_signed_date,
    contract_type,
    committed_monthly_volume,
    discount_rate,
    net_payment_terms_days,
    primary_city,
    source_updated_at_utc,
    FALSE AS is_unknown_contract
FROM contracts_one_row

UNION ALL

SELECT
    'N/A' AS contract_key,
    'N/A' AS contract_id,
    'N/A' AS client_id,
    'N/A' AS contract_status,
    CAST(NULL AS DATE) AS contract_start_date,
    CAST(NULL AS DATE) AS contract_end_date,
    CAST(NULL AS DATE) AS contract_signed_date,
    'N/A' AS contract_type,
    CAST(NULL AS DECIMAL(18, 2)) AS committed_monthly_volume,
    CAST(NULL AS DECIMAL(18, 6)) AS discount_rate,
    CAST(NULL AS INT) AS net_payment_terms_days,
    'N/A' AS primary_city,
    CAST(NULL AS TIMESTAMP) AS source_updated_at_utc,
    TRUE AS is_unknown_contract
