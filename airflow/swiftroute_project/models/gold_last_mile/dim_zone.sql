{{
    config(
        schema = 'gold_last_mile',
        alias = 'dim_zone'
    )
}}

/*
    Zone dimension based on the zone_id assigned in the Onfleet task metadata.
    No unsourced zone name or mapping to city is inferred.
*/
WITH observed_zones AS (
    SELECT DISTINCT
        COALESCE(NULLIF(NULLIF(TRIM(CAST(zone_id AS STRING)), ''), 'N/A'), 'N/A') AS zone_key
    FROM {{ ref('silver_business_lastmile_delivery') }}
    WHERE zone_id IS NOT NULL
),
zone_rows AS (
    SELECT zone_key
    FROM observed_zones
    WHERE zone_key <> 'N/A'
)
SELECT
    zone_key,
    zone_key AS zone_id,
    zone_key AS zone_label,
    FALSE AS is_unknown_zone
FROM zone_rows

UNION ALL

SELECT
    'N/A' AS zone_key,
    'N/A' AS zone_id,
    'Unknown Zone' AS zone_label,
    TRUE AS is_unknown_zone
