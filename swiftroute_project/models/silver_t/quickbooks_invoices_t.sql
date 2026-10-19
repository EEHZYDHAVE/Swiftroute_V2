{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'invoice_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: QuickBooks invoices

    Grain: one row per source Invoice Id. The source ID is confirmed unique.
    The Line and LinkedTxn arrays remain nested; this model does not attempt to
    link invoice lines directly to delivery-level Onfleet tasks.

    Incremental watermark: source MetaData.LastUpdatedTime.
    Source relation: source('swiftroute_databricks', 'quickbooks_invoices')
*/

WITH bronze_invoices AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'quickbooks_invoices') }}
),

source_invoices AS (
    SELECT
        CAST(id AS STRING) AS invoice_id,
        CAST(sync_token AS STRING) AS source_sync_token,
        CAST(metadata_create_time AS TIMESTAMP) AS created_at,
        CAST(metadata_last_updated_time AS TIMESTAMP) AS last_updated_at,
        CAST(doc_number AS STRING) AS document_number,
        TRY_CAST(txn_date AS DATE) AS transaction_date,
        TRY_CAST(due_date AS DATE) AS due_date,
        CAST(customer_ref_value AS STRING) AS source_customer_id,
        CAST(customer_ref_name AS STRING) AS customer_name,
        CAST(bill_email_address AS STRING) AS billing_email,
        CAST(email_status AS STRING) AS email_status,
        TRANSFORM(line, invoice_line -> NAMED_STRUCT(
            'line_id', CAST(invoice_line.id AS STRING),
            'line_number', TRY_CAST(invoice_line.line_num AS INT),
            'description', CAST(invoice_line.description AS STRING),
            'amount', TRY_CAST(invoice_line.amount AS DECIMAL(18, 2)),
            'detail_type', CAST(invoice_line.detail_type AS STRING),
            'sales_item_line_detail', CASE
                WHEN invoice_line.sales_item_line_detail IS NULL THEN CAST(NULL AS STRUCT<item_ref:STRUCT<value:STRING,name:STRING>,unit_price:DECIMAL(18,4),qty:DECIMAL(18,4),service_date:DATE>)
                ELSE NAMED_STRUCT(
                    'item_ref', CASE
                        WHEN invoice_line.sales_item_line_detail.item_ref IS NULL THEN CAST(NULL AS STRUCT<value:STRING,name:STRING>)
                        ELSE NAMED_STRUCT(
                            'value', CAST(invoice_line.sales_item_line_detail.item_ref.value AS STRING),
                            'name', CAST(invoice_line.sales_item_line_detail.item_ref.name AS STRING)
                        )
                    END,
                    'unit_price', TRY_CAST(invoice_line.sales_item_line_detail.unit_price AS DECIMAL(18, 4)),
                    'qty', TRY_CAST(invoice_line.sales_item_line_detail.qty AS DECIMAL(18, 4)),
                    'service_date', TRY_CAST(invoice_line.sales_item_line_detail.service_date AS DATE)
                )
            END
        )) AS lines,
        TRY_CAST(total_amt AS DECIMAL(18, 2)) AS total_amount,
        TRY_CAST(balance AS DECIMAL(18, 2)) AS balance_amount,
        CAST(currency_ref_value AS STRING) AS currency_code,
        CAST(currency_ref_name AS STRING) AS currency_name,
        CAST(payment_method_ref_value AS STRING) AS source_payment_method_id,
        CAST(payment_method_ref_name AS STRING) AS payment_method_name,
        TRANSFORM(linked_txn, linked -> NAMED_STRUCT(
            'transaction_id', CAST(linked.txn_id AS STRING),
            'transaction_type', CAST(linked.txn_type AS STRING)
        )) AS linked_transactions,
        custom_field AS custom_fields,
        CAST(ELEMENT_AT(FILTER(custom_field, field -> LOWER(TRIM(field.name)) = 'swiftroute client id'), 1).string_value AS STRING) AS client_id,
        CAST(ELEMENT_AT(FILTER(custom_field, field -> LOWER(TRIM(field.name)) = 'billing period'), 1).string_value AS STRING) AS billing_period,
        CAST(ELEMENT_AT(FILTER(custom_field, field -> LOWER(TRIM(field.name)) = 'payment terms'), 1).string_value AS STRING) AS payment_terms
    FROM bronze_invoices
),

eligible_invoices AS (
    SELECT *
    FROM source_invoices
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
    invoice_id,
    source_sync_token,
    created_at,
    last_updated_at,
    document_number,
    transaction_date,
    due_date,
    source_customer_id,
    customer_name,
    client_id,
    billing_period,
    payment_terms,
    billing_email,
    email_status,
    total_amount,
    balance_amount,
    currency_code,
    currency_name,
    source_payment_method_id,
    payment_method_name,
    lines,
    linked_transactions,
    custom_fields,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_invoices
