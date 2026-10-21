{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'source_stock_transaction_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Linnworks stock transactions

    Grain: one row per source pkStockTransactionId.

    The source reference is preserved and the DELETED-ORD- prefix is removed
    into a separate standardized order_reference. A deleted-order flag prevents
    those transactions from being mistaken for current matched orders.

    Incremental watermark: Date, converted to transaction_at_utc. This is an
    event-time watermark (not a source last-modified timestamp).
*/

WITH bronze_transactions AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'linnworks_stock_transactions') }}
),

source_transaction_rows AS (
    SELECT
        CAST(pk_stock_transaction_id AS STRING) AS source_stock_transaction_id,
        CAST(fk_stock_item_id AS STRING) AS source_stock_item_id,
        CAST(sku AS STRING) AS sku,
        CAST(item_title AS STRING) AS item_title,
        CAST(transaction_type AS STRING) AS transaction_type,
        CAST(transaction_note AS STRING) AS transaction_note,
        TRY_CAST(quantity AS BIGINT) AS quantity,
        CAST(fk_order_id AS STRING) AS source_order_reference,
        REGEXP_REPLACE(CAST(fk_order_id AS STRING), '(?i)^DELETED-ORD-', '') AS order_reference,
        CASE
            WHEN UPPER(CAST(fk_order_id AS STRING)) LIKE 'DELETED-ORD-%' THEN TRUE
            ELSE FALSE
        END AS is_deleted_order_reference,
        CAST(location AS STRING) AS location,
        CAST(bin_rack AS STRING) AS bin_rack,
        CAST(date AS TIMESTAMP) AS transaction_at_utc,
        TRY_CAST(stock_value AS DECIMAL(18, 2)) AS stock_value
    FROM bronze_transactions
),

ranked_source_transactions AS (
    SELECT
        source_transaction_rows.*,
        ROW_NUMBER() OVER (
            PARTITION BY source_stock_transaction_id
            ORDER BY
                transaction_at_utc ASC NULLS LAST
        ) AS transaction_duplicate_rank
    FROM source_transaction_rows
),

deduplicated_source_transactions AS (
    SELECT * EXCEPT (transaction_duplicate_rank)
    FROM ranked_source_transactions
    WHERE transaction_duplicate_rank = 1
),

eligible_source_transactions AS (
    SELECT *
    FROM deduplicated_source_transactions
    {% if is_incremental() %}
    WHERE transaction_at_utc >= (
        SELECT COALESCE(
            MAX(target.transaction_at_utc),
            CAST('1900-01-01 00:00:00' AS TIMESTAMP)
        )
        FROM {{ this }} AS target
    )
    {% endif %}
),

existing_transaction_mapping AS (
    {% if is_incremental() %}
    SELECT
        CAST(source_stock_transaction_id AS STRING) AS source_stock_transaction_id,
        CAST(stock_transaction_id AS STRING) AS stock_transaction_id
    FROM {{ this }}
    {% else %}
    SELECT
        CAST(NULL AS STRING) AS source_stock_transaction_id,
        CAST(NULL AS STRING) AS stock_transaction_id
    WHERE 1 = 0
    {% endif %}
),

current_max_transaction_number AS (
    SELECT COALESCE(
        MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(stock_transaction_id, '^stock_transaction_id_([0-9]+)$', 1), '') AS BIGINT)),
        0
    ) AS max_transaction_number
    FROM existing_transaction_mapping
),

new_transaction_ids AS (
    SELECT DISTINCT eligible.source_stock_transaction_id
    FROM eligible_source_transactions AS eligible
    LEFT ANTI JOIN existing_transaction_mapping AS existing
        ON existing.source_stock_transaction_id <=> eligible.source_stock_transaction_id
    WHERE eligible.source_stock_transaction_id IS NOT NULL
),

new_transaction_assignments AS (
    SELECT
        new_ids.source_stock_transaction_id,
        CONCAT(
            'stock_transaction_id_',
            CAST(
                current_max.max_transaction_number
                + ROW_NUMBER() OVER (ORDER BY new_ids.source_stock_transaction_id ASC)
                AS STRING
            )
        ) AS stock_transaction_id
    FROM new_transaction_ids AS new_ids
    CROSS JOIN current_max_transaction_number AS current_max
),

transaction_id_mapping AS (
    SELECT source_stock_transaction_id, stock_transaction_id
    FROM existing_transaction_mapping

    UNION ALL

    SELECT source_stock_transaction_id, stock_transaction_id
    FROM new_transaction_assignments
),

stock_item_mapping AS (
    SELECT
        CAST(source_stock_item_id AS STRING) AS source_stock_item_id,
        CAST(stock_item_id AS STRING) AS stock_item_id
    FROM {{ ref('linnworks_stock_items_t') }}
),

order_reference_mapping AS (
    SELECT
        CAST(reference_num AS STRING) AS reference_num,
        MAX(CAST(order_id AS STRING)) AS order_id
    FROM {{ ref('linnworks_orders_t') }}
    WHERE reference_num IS NOT NULL
      AND reference_num <> 'N/A'
    GROUP BY reference_num
    HAVING COUNT(DISTINCT order_id) = 1
)

SELECT
    transaction_keys.stock_transaction_id AS stock_transaction_id,
    transactions.source_stock_transaction_id AS source_stock_transaction_id,
    transactions.source_stock_item_id AS source_stock_item_id,
    stock_items.stock_item_id AS stock_item_id,
    CASE WHEN transactions.sku IS NULL OR TRIM(transactions.sku) = '' THEN 'N/A' ELSE transactions.sku END AS sku,
    CASE WHEN transactions.item_title IS NULL OR TRIM(transactions.item_title) = '' THEN 'N/A' ELSE transactions.item_title END AS item_title,
    CASE WHEN transactions.transaction_type IS NULL OR TRIM(transactions.transaction_type) = '' THEN 'N/A' ELSE transactions.transaction_type END AS transaction_type,
    CASE WHEN transactions.transaction_note IS NULL OR TRIM(transactions.transaction_note) = '' THEN 'N/A' ELSE transactions.transaction_note END AS transaction_note,
    transactions.quantity AS quantity,
    transactions.source_order_reference AS source_order_reference,
    CASE WHEN transactions.order_reference IS NULL OR TRIM(transactions.order_reference) = '' THEN 'N/A' ELSE transactions.order_reference END AS order_reference,
    transactions.is_deleted_order_reference AS is_deleted_order_reference,
    CASE
        WHEN transactions.is_deleted_order_reference THEN CAST(NULL AS STRING)
        ELSE orders.order_id
    END AS order_id,
    CASE WHEN transactions.location IS NULL OR TRIM(transactions.location) = '' THEN 'N/A' ELSE transactions.location END AS location,
    CASE WHEN transactions.bin_rack IS NULL OR TRIM(transactions.bin_rack) = '' THEN 'N/A' ELSE transactions.bin_rack END AS bin_rack,
    transactions.transaction_at_utc AS transaction_at_utc,
    transactions.stock_value AS stock_value,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_source_transactions AS transactions
LEFT JOIN transaction_id_mapping AS transaction_keys
    ON transaction_keys.source_stock_transaction_id <=> transactions.source_stock_transaction_id
LEFT JOIN stock_item_mapping AS stock_items
    ON stock_items.source_stock_item_id <=> transactions.source_stock_item_id
LEFT JOIN order_reference_mapping AS orders
    ON orders.reference_num = transactions.order_reference
   AND NOT transactions.is_deleted_order_reference
