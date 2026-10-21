{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'payment_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: QuickBooks payments

    Grain: one row per source Payment Id. The source ID is confirmed unique.
    Payment line allocations and linked transaction references remain nested.
    UnappliedAmt is retained as supplied and is not treated as an invoice balance.

    Incremental watermark: source MetaData.LastUpdatedTime.
    Source relation: source('swiftroute_databricks', 'quickbooks_payments')
*/

WITH bronze_payments AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'quickbooks_payments') }}
),

source_payments AS (
    SELECT
        CAST(id AS STRING) AS payment_id,
        CAST(sync_token AS STRING) AS source_sync_token,
        CAST(metadata_create_time AS TIMESTAMP) AS created_at,
        CAST(metadata_last_updated_time AS TIMESTAMP) AS last_updated_at,
        TRY_CAST(txn_date AS DATE) AS transaction_date,
        CAST(customer_ref_value AS STRING) AS source_customer_id,
        CAST(customer_ref_name AS STRING) AS customer_name,
        TRY_CAST(total_amt AS DECIMAL(18, 2)) AS total_amount,
        TRY_CAST(unapplied_amt AS DECIMAL(18, 2)) AS unapplied_amount,
        CAST(process_payment AS BOOLEAN) AS process_payment,
        CAST(payment_method_ref_value AS STRING) AS source_payment_method_id,
        CAST(payment_method_ref_name AS STRING) AS payment_method_name,
        CAST(deposit_to_account_ref_value AS STRING) AS source_deposit_account_id,
        CAST(deposit_to_account_ref_name AS STRING) AS deposit_account_name,
        TRANSFORM(line, payment_line -> NAMED_STRUCT(
            'amount', TRY_CAST(payment_line.amount AS DECIMAL(18, 2)),
            'linked_transactions', TRANSFORM(payment_line.linked_txn, linked -> NAMED_STRUCT(
                'transaction_id', CAST(linked.txn_id AS STRING),
                'transaction_type', CAST(linked.txn_type AS STRING)
            ))
        )) AS lines,
        custom_field AS custom_fields,
        CAST(ELEMENT_AT(FILTER(custom_field, field -> LOWER(TRIM(field.name)) = 'swiftroute client id'), 1).string_value AS STRING) AS client_id,
        TRY_CAST(ELEMENT_AT(FILTER(custom_field, field -> LOWER(TRIM(field.name)) = 'days to pay'), 1).string_value AS INT) AS days_to_pay
    FROM bronze_payments
),

eligible_payments AS (
    SELECT *
    FROM source_payments
    {% if is_incremental() %}
    WHERE last_updated_at IS NULL
       OR last_updated_at >= (
        SELECT COALESCE(
            MAX(target.last_updated_at),
            CAST('1900-01-01 00:00:00' AS TIMESTAMP)
        )
        FROM {{ this }} AS target
    )
    {% endif %}
)

SELECT
    payment_id,
    source_sync_token,
    created_at,
    last_updated_at,
    transaction_date,
    source_customer_id,
    customer_name,
    client_id,
    days_to_pay,
    total_amount,
    unapplied_amount,
    process_payment,
    source_payment_method_id,
    payment_method_name,
    -- new column: always USD
    'USD' AS currency_code,
    source_deposit_account_id,
    deposit_account_name,
    lines,
    custom_fields,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_payments;