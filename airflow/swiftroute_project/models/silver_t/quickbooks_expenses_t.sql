{{
    config(
        materialized = 'table',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: QuickBooks expenses (Purchase records)

    Grain: one row per retained source expense record.

    The simulation's source Id is not unique and has no reliable record-level
    identity. This model therefore rebuilds as a table instead of attempting an
    unsafe incremental merge. expense_id is a deterministic technical identifier
    derived from a fingerprint of the available business fields plus an ordinal
    for records with identical fingerprints. It is not a recovered source key.
    If a business field changes, the generated ID can change. Exact duplicate
    rows cannot be individually distinguished by the source.

    Source relation: source('swiftroute_databricks', 'quickbooks_expenses')
*/

WITH bronze_expenses AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'quickbooks_expenses') }}
),

source_expenses AS (
    SELECT
        CAST(id AS STRING) AS source_expense_id,
        CAST(sync_token AS STRING) AS source_sync_token,
        CAST(metadata_create_time AS TIMESTAMP) AS created_at,
        CAST(metadata_last_updated_time AS TIMESTAMP) AS last_updated_at,
        TRY_CAST(txn_date AS DATE) AS transaction_date,
        CAST(payment_type AS STRING) AS payment_type,
        CAST(account_ref_value AS STRING) AS source_account_id,
        CAST(account_ref_name AS STRING) AS account_name,
        CAST(entity_ref_value AS STRING) AS source_entity_id,
        CAST(entity_ref_name AS STRING) AS entity_name,
        CAST(entity_ref_type AS STRING) AS entity_type,
        TRANSFORM(line, expense_line -> NAMED_STRUCT(
            'line_id', CAST(expense_line.id AS STRING),
            'amount', TRY_CAST(expense_line.amount AS DECIMAL(18, 2)),
            'detail_type', CAST(expense_line.detail_type AS STRING),
            'account_based_expense_line_detail', CASE
                WHEN expense_line.account_based_expense_line_detail IS NULL THEN CAST(NULL AS STRUCT<account_ref:STRUCT<value:STRING,name:STRING>,billable_status:STRING,class_ref:STRUCT<value:STRING,name:STRING>>)
                ELSE NAMED_STRUCT(
                    'account_ref', CASE
                        WHEN expense_line.account_based_expense_line_detail.account_ref IS NULL THEN CAST(NULL AS STRUCT<value:STRING,name:STRING>)
                        ELSE NAMED_STRUCT(
                            'value', CAST(expense_line.account_based_expense_line_detail.account_ref.value AS STRING),
                            'name', CAST(expense_line.account_based_expense_line_detail.account_ref.name AS STRING)
                        )
                    END,
                    'billable_status', CAST(expense_line.account_based_expense_line_detail.billable_status AS STRING),
                    'class_ref', CASE
                        WHEN expense_line.account_based_expense_line_detail.class_ref IS NULL THEN CAST(NULL AS STRUCT<value:STRING,name:STRING>)
                        ELSE NAMED_STRUCT(
                            'value', CAST(expense_line.account_based_expense_line_detail.class_ref.value AS STRING),
                            'name', CAST(expense_line.account_based_expense_line_detail.class_ref.name AS STRING)
                        )
                    END
                )
            END,
            'description', CAST(expense_line.description AS STRING)
        )) AS lines,
        TRY_CAST(total_amt AS DECIMAL(18, 2)) AS total_amount,
        CAST(private_note AS STRING) AS private_note,
        CAST(currency_ref_value AS STRING) AS currency_code,
        CAST(currency_ref_name AS STRING) AS currency_name
    FROM bronze_expenses
),

fingerprinted_expenses AS (
    SELECT
        source_expenses.*,
        SHA2(TO_JSON(NAMED_STRUCT(
            'source_expense_id', source_expense_id,
            'source_sync_token', source_sync_token,
            'created_at', created_at,
            'last_updated_at', last_updated_at,
            'transaction_date', transaction_date,
            'payment_type', payment_type,
            'source_account_id', source_account_id,
            'account_name', account_name,
            'source_entity_id', source_entity_id,
            'entity_name', entity_name,
            'entity_type', entity_type,
            'lines', lines,
            'total_amount', total_amount,
            'private_note', private_note,
            'currency_code', currency_code,
            'currency_name', currency_name
        )), 256) AS record_fingerprint
    FROM source_expenses
),

ranked_expenses AS (
    SELECT
        fingerprinted_expenses.*,
        ROW_NUMBER() OVER (
            PARTITION BY record_fingerprint
            ORDER BY record_fingerprint
        ) AS identical_record_number,
        COUNT(source_expense_id) OVER (
            PARTITION BY source_expense_id
        ) AS source_expense_id_count
    FROM fingerprinted_expenses
)

SELECT
    CONCAT(
        'expense_id_',
        record_fingerprint,
        '_',
        CAST(identical_record_number AS STRING)
    ) AS expense_id,
    source_expense_id,
    source_sync_token,
    created_at,
    last_updated_at,
    transaction_date,
    payment_type,
    source_account_id,
    account_name,
    source_entity_id,
    entity_name,
    entity_type,
    lines,
    total_amount,
    private_note,
    currency_code,
    currency_name,
    CASE
        WHEN source_expense_id IS NOT NULL AND source_expense_id_count > 1 THEN TRUE
        ELSE FALSE
    END AS has_duplicate_source_expense_id,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM ranked_expenses
