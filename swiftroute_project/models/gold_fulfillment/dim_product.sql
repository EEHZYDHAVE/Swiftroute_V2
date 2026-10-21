{{
    config(
        schema = 'gold_fulfillment',
        alias = 'dim_product'
    )
}}

/*
    Product dimension based on Linnworks stock items, supplemented by observed
    transaction attributes if a transaction's stock item is absent from master.
    The product key is stock_item_id; N/A is the unknown member.
*/

WITH stock_item_ranked AS (
    SELECT
        TRIM(CAST(stock_item_id AS STRING)) AS product_key,
        TRIM(CAST(stock_item_id AS STRING)) AS stock_item_id,
        CAST(source_stock_item_id AS STRING) AS source_stock_item_id,
        CAST(sku_normalized AS STRING) AS sku,
        CAST(item_number AS STRING) AS item_number,
        CAST(barcode_number AS STRING) AS barcode_number,
        CAST(item_title AS STRING) AS product_name,
        CAST(category_name AS STRING) AS product_category,
        CAST(package_group_name AS STRING) AS product_package_group,
        TRY_CAST(weight AS DECIMAL(18, 4)) AS product_weight,
        TRY_CAST(width AS DECIMAL(18, 4)) AS product_width,
        TRY_CAST(height AS DECIMAL(18, 4)) AS product_height,
        TRY_CAST(depth AS DECIMAL(18, 4)) AS product_depth,
        CAST(creation_date_utc AS TIMESTAMP) AS product_created_at_utc,
        CAST(source_updated_at_utc AS TIMESTAMP) AS product_source_updated_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(CAST(stock_item_id AS STRING))
            ORDER BY source_updated_at_utc DESC NULLS LAST,
                     TRIM(CAST(stock_item_id AS STRING)) ASC
        ) AS item_rank
    FROM {{ ref('linnworks_stock_items_t') }}
    WHERE stock_item_id IS NOT NULL
      AND TRIM(CAST(stock_item_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(stock_item_id AS STRING))) <> 'N/A'
),
stock_items_one_row AS (
    SELECT * FROM stock_item_ranked WHERE item_rank = 1
),
observed_products AS (
    SELECT
        TRIM(CAST(stock_item_id AS STRING)) AS product_key,
        MAX(NULLIF(TRIM(CAST(source_stock_item_id AS STRING)), 'N/A')) AS source_stock_item_id,
        MAX(NULLIF(TRIM(CAST(sku AS STRING)), 'N/A')) AS sku,
        MAX(NULLIF(TRIM(CAST(item_number AS STRING)), 'N/A')) AS item_number,
        MAX(NULLIF(TRIM(CAST(barcode_number AS STRING)), 'N/A')) AS barcode_number,
        MAX(NULLIF(TRIM(CAST(product_name AS STRING)), 'N/A')) AS product_name,
        MAX(NULLIF(TRIM(CAST(product_category AS STRING)), 'N/A')) AS product_category,
        MAX(NULLIF(TRIM(CAST(product_package_group AS STRING)), 'N/A')) AS product_package_group,
        MAX(TRY_CAST(product_weight AS DECIMAL(18, 4))) AS product_weight,
        MAX(TRY_CAST(product_width AS DECIMAL(18, 4))) AS product_width,
        MAX(TRY_CAST(product_height AS DECIMAL(18, 4))) AS product_height,
        MAX(TRY_CAST(product_depth AS DECIMAL(18, 4))) AS product_depth,
        MAX(CAST(product_created_at_utc AS TIMESTAMP)) AS product_created_at_utc,
        MAX(CAST(product_source_updated_at_utc AS TIMESTAMP)) AS product_source_updated_at_utc
    FROM {{ ref('silver_business_fufillment') }}
    WHERE stock_item_id IS NOT NULL
      AND TRIM(CAST(stock_item_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(stock_item_id AS STRING))) <> 'N/A'
    GROUP BY TRIM(CAST(stock_item_id AS STRING))
),
all_product_keys AS (
    SELECT product_key FROM stock_items_one_row
    UNION
    SELECT product_key FROM observed_products
),
resolved_products AS (
    SELECT
        k.product_key,
        k.product_key AS stock_item_id,
        COALESCE(NULLIF(s.source_stock_item_id, ''), o.source_stock_item_id, 'N/A') AS source_stock_item_id,
        COALESCE(NULLIF(s.sku, ''), o.sku, 'N/A') AS sku,
        COALESCE(NULLIF(s.item_number, ''), o.item_number, 'N/A') AS item_number,
        COALESCE(NULLIF(s.barcode_number, ''), o.barcode_number, 'N/A') AS barcode_number,
        COALESCE(NULLIF(s.product_name, ''), o.product_name, 'N/A') AS product_name,
        COALESCE(NULLIF(s.product_category, ''), o.product_category, 'N/A') AS product_category,
        COALESCE(NULLIF(s.product_package_group, ''), o.product_package_group, 'N/A') AS product_package_group,
        COALESCE(s.product_weight, o.product_weight) AS product_weight,
        COALESCE(s.product_width, o.product_width) AS product_width,
        COALESCE(s.product_height, o.product_height) AS product_height,
        COALESCE(s.product_depth, o.product_depth) AS product_depth,
        COALESCE(s.product_created_at_utc, o.product_created_at_utc) AS product_created_at_utc,
        COALESCE(s.product_source_updated_at_utc, o.product_source_updated_at_utc)
            AS product_source_updated_at_utc
    FROM all_product_keys k
    LEFT JOIN stock_items_one_row s ON k.product_key = s.product_key
    LEFT JOIN observed_products o ON k.product_key = o.product_key
)

SELECT
    product_key,
    stock_item_id,
    source_stock_item_id,
    sku,
    item_number,
    barcode_number,
    product_name,
    product_category,
    product_package_group,
    product_weight,
    product_width,
    product_height,
    product_depth,
    product_created_at_utc,
    product_source_updated_at_utc,
    FALSE AS is_unknown_product
FROM resolved_products

UNION ALL

SELECT
    'N/A' AS product_key,
    'N/A' AS stock_item_id,
    'N/A' AS source_stock_item_id,
    'N/A' AS sku,
    'N/A' AS item_number,
    'N/A' AS barcode_number,
    'N/A' AS product_name,
    'N/A' AS product_category,
    'N/A' AS product_package_group,
    CAST(NULL AS DECIMAL(18, 4)) AS product_weight,
    CAST(NULL AS DECIMAL(18, 4)) AS product_width,
    CAST(NULL AS DECIMAL(18, 4)) AS product_height,
    CAST(NULL AS DECIMAL(18, 4)) AS product_depth,
    CAST(NULL AS TIMESTAMP) AS product_created_at_utc,
    CAST(NULL AS TIMESTAMP) AS product_source_updated_at_utc,
    TRUE AS is_unknown_product
