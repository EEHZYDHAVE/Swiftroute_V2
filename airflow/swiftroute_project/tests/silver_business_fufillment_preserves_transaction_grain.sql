/*
    Singular dbt test: each Silver Technical stock transaction must be represented
    by the business model, and the business model must not introduce new keys.
    Duplicate target keys are tested separately by the properties.yml unique test.
*/
WITH source_keys AS (
    SELECT CAST(stock_transaction_id AS STRING) AS transaction_id
    FROM {{ ref('linnworks_stock_transaction_t') }}
),

target_keys AS (
    SELECT CAST(fulfillment_transaction_id AS STRING) AS transaction_id
    FROM {{ ref('silver_business_fufillment') }}
)

SELECT
    'MISSING_IN_BUSINESS_MODEL' AS issue_type,
    source_keys.transaction_id
FROM source_keys
LEFT ANTI JOIN target_keys
    ON source_keys.transaction_id = target_keys.transaction_id

UNION ALL

SELECT
    'EXTRA_IN_BUSINESS_MODEL' AS issue_type,
    target_keys.transaction_id
FROM target_keys
LEFT ANTI JOIN source_keys
    ON target_keys.transaction_id = source_keys.transaction_id
