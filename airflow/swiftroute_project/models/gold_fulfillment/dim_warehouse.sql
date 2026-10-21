{{
    config(
        schema = 'gold_fulfillment',
        alias = 'dim_warehouse'
    )
}}

/*
    Location dimension built from the inventory location values actually
    observed in Linnworks stock transactions. This is not a separate warehouse
    master: do not interpret a location label as a named facility unless the
    source semantics confirm that it represents a warehouse.
*/

WITH locations AS (
    SELECT
        UPPER(TRIM(CAST(warehouse_location AS STRING))) AS warehouse_key,
        MAX(TRIM(CAST(warehouse_location AS STRING))) AS warehouse_location_name
    FROM {{ ref('silver_business_fufillment') }}
    WHERE warehouse_location IS NOT NULL
      AND TRIM(CAST(warehouse_location AS STRING)) <> ''
      AND UPPER(TRIM(CAST(warehouse_location AS STRING))) <> 'N/A'
    GROUP BY UPPER(TRIM(CAST(warehouse_location AS STRING)))
)

SELECT
    warehouse_key,
    warehouse_location_name,
    warehouse_location_name AS source_location_label,
    FALSE AS is_unknown_warehouse
FROM locations

UNION ALL

SELECT
    'N/A' AS warehouse_key,
    'N/A' AS warehouse_location_name,
    'N/A' AS source_location_label,
    TRUE AS is_unknown_warehouse
