{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'source_stock_item_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Linnworks stock items

    Grain: one row per source StockItemId.

    The source relation below uses the name shown in the supplied query:
    source('swiftroute_databricks', 'linnworks_inventory'). If the dbt source
    definition uses a different relation name, change it here.

    Readable stock_item_id assignments are persistent across incremental runs.
    SKU normalization is only a comparison aid; records are NOT deduplicated or
    aggregated by normalized SKU because the simulation contains SKU collisions.
*/

WITH bronze_stock_items AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'linnworks_inventory') }}
),

source_stock_items AS (
    SELECT
        CAST(stock_item_id AS STRING) AS source_stock_item_id,
        CAST(item_number AS STRING) AS item_number,
        CAST(item_title AS STRING) AS item_title,
        CAST(barcode_number AS STRING) AS barcode_number,
        CAST(category_name AS STRING) AS category_name,
        CAST(package_group_name AS STRING) AS package_group_name,
        CAST(tax_cost_inclusive AS BOOLEAN) AS tax_cost_inclusive,
        CAST(postal_service_name AS STRING) AS postal_service_name,
        TRY_CAST(cost_price AS DECIMAL(18, 2)) AS cost_price,
        TRY_CAST(retail_price AS DECIMAL(18, 2)) AS retail_price,
        TRY_CAST(weight AS DOUBLE) AS weight,
        TRY_CAST(width AS DOUBLE) AS width,
        TRY_CAST(height AS DOUBLE) AS height,
        TRY_CAST(depth AS DOUBLE) AS depth,
        CAST(is_composite_parent AS BOOLEAN) AS is_composite_parent,
        CAST(is_variation_parent AS BOOLEAN) AS is_variation_parent,
        TRY_CAST(quantity AS BIGINT) AS quantity,
        TRY_CAST(minimum_level AS BIGINT) AS minimum_level,
        TRY_CAST(in_order_book AS BIGINT) AS in_order_book,
        TRY_CAST(due AS BIGINT) AS due,
        CAST(jit AS BOOLEAN) AS jit,
        CAST(creation_date AS TIMESTAMP) AS creation_date_utc,
        CAST(modified_date AS TIMESTAMP) AS source_updated_at_utc,
        CAST(source AS STRING) AS item_source,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        CAST(_swiftroute_client_id AS STRING) AS client_id,
        CAST(_swiftroute_client_name AS STRING) AS client_name
    FROM bronze_stock_items
),

eligible_source_stock_items AS (
    SELECT *
    FROM source_stock_items
    {% if is_incremental() %}
    WHERE source_updated_at_utc >= (
        SELECT COALESCE(
            MAX(target.source_updated_at_utc),
            CAST('1900-01-01 00:00:00' AS TIMESTAMP)
        )
        FROM {{ this }} AS target
    )
    {% endif %}
),

/* Keep existing surrogate assignments stable on incremental runs. */
existing_stock_item_mapping AS (
    {% if is_incremental() %}
    SELECT
        CAST(source_stock_item_id AS STRING) AS source_stock_item_id,
        CAST(stock_item_id AS STRING) AS stock_item_id
    FROM {{ this }}
    {% else %}
    SELECT
        CAST(NULL AS STRING) AS source_stock_item_id,
        CAST(NULL AS STRING) AS stock_item_id
    WHERE 1 = 0
    {% endif %}
),

current_max_stock_item_number AS (
    SELECT COALESCE(
        MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(stock_item_id, '^stock_item_id_([0-9]+)$', 1), '') AS BIGINT)),
        0
    ) AS max_stock_item_number
    FROM existing_stock_item_mapping
),

new_stock_item_ids AS (
    SELECT DISTINCT eligible.source_stock_item_id
    FROM eligible_source_stock_items AS eligible
    LEFT ANTI JOIN existing_stock_item_mapping AS existing
        ON existing.source_stock_item_id <=> eligible.source_stock_item_id
    WHERE eligible.source_stock_item_id IS NOT NULL
),

new_stock_item_assignments AS (
    SELECT
        new_ids.source_stock_item_id,
        CONCAT(
            'stock_item_id_',
            CAST(
                current_max.max_stock_item_number
                + ROW_NUMBER() OVER (ORDER BY new_ids.source_stock_item_id ASC)
                AS STRING
            )
        ) AS stock_item_id
    FROM new_stock_item_ids AS new_ids
    CROSS JOIN current_max_stock_item_number AS current_max
),

stock_item_id_mapping AS (
    SELECT source_stock_item_id, stock_item_id
    FROM existing_stock_item_mapping

    UNION ALL

    SELECT source_stock_item_id, stock_item_id
    FROM new_stock_item_assignments
)

SELECT
    mapping.stock_item_id AS stock_item_id,
    source.source_stock_item_id AS source_stock_item_id,
    source.item_number AS item_number,
    UPPER(REGEXP_REPLACE(TRIM(source.item_number), '_', '-')) AS sku_normalized,
    CASE WHEN source.item_title IS NULL OR TRIM(source.item_title) = '' THEN 'N/A' ELSE source.item_title END AS item_title,
    CASE WHEN source.barcode_number IS NULL OR TRIM(source.barcode_number) = '' THEN 'N/A' ELSE source.barcode_number END AS barcode_number,
    CASE WHEN source.category_name IS NULL OR TRIM(source.category_name) = '' THEN 'N/A' ELSE source.category_name END AS category_name,
    CASE WHEN source.package_group_name IS NULL OR TRIM(source.package_group_name) = '' THEN 'N/A' ELSE source.package_group_name END AS package_group_name,
    source.tax_cost_inclusive AS tax_cost_inclusive,
    CASE WHEN source.postal_service_name IS NULL OR TRIM(source.postal_service_name) = '' THEN 'N/A' ELSE source.postal_service_name END AS postal_service_name,
    source.cost_price AS cost_price,
    source.retail_price AS retail_price,
    source.weight AS weight,
    source.width AS width,
    source.height AS height,
    source.depth AS depth,
    source.is_composite_parent AS is_composite_parent,
    source.is_variation_parent AS is_variation_parent,
    source.quantity AS quantity,
    source.minimum_level AS minimum_level,
    source.in_order_book AS in_order_book,
    source.due AS due,
    source.jit AS jit,
    source.creation_date_utc AS creation_date_utc,
    source.source_updated_at_utc AS source_updated_at_utc,
    CASE WHEN source.item_source IS NULL OR TRIM(source.item_source) = '' THEN 'N/A' ELSE source.item_source END AS item_source,
    source.is_deleted AS is_deleted,
    CASE WHEN source.client_id IS NULL OR TRIM(source.client_id) = '' THEN 'N/A' ELSE source.client_id END AS client_id,
    CASE WHEN source.client_name IS NULL OR TRIM(source.client_name) = '' THEN 'N/A' ELSE source.client_name END AS client_name,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_source_stock_items AS source
LEFT JOIN stock_item_id_mapping AS mapping
    ON mapping.source_stock_item_id <=> source.source_stock_item_id
