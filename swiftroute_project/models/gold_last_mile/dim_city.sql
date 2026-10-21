{{
    config(
        schema = 'gold_last_mile',
        alias = 'dim_city'
    )
}}

/*
    Delivery destination geography, not customer headquarters geography.
    city_key is a normalized composite of city, state and country to distinguish same-name cities.
*/
WITH source_locations AS (
    SELECT
        CASE
            WHEN NULLIF(NULLIF(TRIM(CAST(destination_city AS STRING)), ''), 'N/A') IS NULL THEN 'N/A'
            ELSE CONCAT(
                UPPER(TRIM(CAST(destination_city AS STRING))), '|',
                COALESCE(NULLIF(NULLIF(UPPER(TRIM(CAST(destination_state AS STRING))), ''), 'N/A'), 'N/A'), '|',
                COALESCE(NULLIF(NULLIF(UPPER(TRIM(CAST(destination_country AS STRING))), ''), 'N/A'), 'N/A')
            )
        END AS city_key,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(destination_city AS STRING)), ''), 'N/A'), 'N/A') AS city_name,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(destination_state AS STRING)), ''), 'N/A'), 'N/A') AS state_or_region,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(destination_country AS STRING)), ''), 'N/A'), 'N/A') AS country
    FROM {{ ref('silver_business_lastmile_delivery') }}
),
city_rows AS (
    SELECT
        city_key,
        MAX(city_name) AS city_name,
        MAX(state_or_region) AS state_or_region,
        MAX(country) AS country
    FROM source_locations
    GROUP BY city_key
    HAVING city_key <> 'N/A'
)
SELECT
    city_key,
    city_name,
    state_or_region,
    country,
    CONCAT(city_name, ', ', state_or_region, ', ', country) AS city_label,
    FALSE AS is_unknown_city
FROM city_rows

UNION ALL

SELECT
    'N/A' AS city_key,
    'Unknown City' AS city_name,
    'N/A' AS state_or_region,
    'N/A' AS country,
    'Unknown City' AS city_label,
    TRUE AS is_unknown_city
