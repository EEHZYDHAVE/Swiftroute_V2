{{
    config(
        materialized = 'table',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Samsara vehicle roster

    Grain: one row per Samsara vehicle ID. The roster has no source modified timestamp,
    so a table rebuild is used rather than a synthetic incremental watermark.

    Vehicle IDs do not map directly to Onfleet fields. The source vehicle ID is preserved;
    no guessed delivery-to-vehicle mapping is created here. City is extracted from existing
    Samsara tags where a known city tag is present.
*/

WITH bronze_vehicles AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'samsara_vehicles') }}
),

normalized_vehicle_tags AS (
    SELECT
        COALESCE(NULLIF(TRIM(CAST(vehicle_id AS STRING)), ''), 'N/A') AS vehicle_id,
        COALESCE(NULLIF(TRIM(CAST(name AS STRING)), ''), 'N/A') AS vehicle_name,
        COALESCE(NULLIF(TRIM(CAST(vin AS STRING)), ''), 'N/A') AS vin,
        COALESCE(NULLIF(TRIM(CAST(make AS STRING)), ''), 'N/A') AS make,
        COALESCE(NULLIF(TRIM(CAST(model AS STRING)), ''), 'N/A') AS model,
        COALESCE(TRY_CAST(year AS INT), 0) AS model_year,
        COALESCE(NULLIF(TRIM(CAST(vehicle_type AS STRING)), ''), 'N/A') AS vehicle_type,
        COALESCE(NULLIF(TRIM(CAST(license_plate AS STRING)), ''), 'N/A') AS license_plate,
        COALESCE(NULLIF(LOWER(TRIM(CAST(fuel_type AS STRING))), ''), 'N/A') AS fuel_type,
        TRANSFORM(
            COALESCE(tags, ARRAY()),
            vehicle_tag -> NAMED_STRUCT(
                'tag_id', COALESCE(NULLIF(TRIM(CAST(vehicle_tag.id AS STRING)), ''), 'N/A'),
                'tag_name', COALESCE(NULLIF(TRIM(CAST(vehicle_tag.name AS STRING)), ''), 'N/A')
            )
        ) AS tags,
        TRANSFORM(
            COALESCE(tags, ARRAY()),
            vehicle_tag -> LOWER(TRIM(CAST(vehicle_tag.name AS STRING)))
        ) AS tag_names,
        COALESCE(TRY_CAST(current_odometer_meters AS DECIMAL(20, 2)), CAST(0 AS DECIMAL(20, 2))) AS current_odometer_meters,
        TRY_CAST(last_known_location_latitude AS DOUBLE) AS last_known_latitude,
        TRY_CAST(last_known_location_longitude AS DOUBLE) AS last_known_longitude,
        CASE
            WHEN LOWER(REPLACE(TRIM(CAST(operational_status AS STRING)), '_', '')) = 'outofservice'
                THEN 'out_of_service'
            ELSE COALESCE(
                NULLIF(LOWER(REGEXP_REPLACE(TRIM(CAST(operational_status AS STRING)), '\\s+', '_')), ''),
                'N/A'
            )
        END AS operational_status
    FROM bronze_vehicles
)

SELECT
    vehicle_id,
    vehicle_name,
    vin,
    make,
    model,
    model_year,
    vehicle_type,
    license_plate,
    fuel_type,
    CASE
        WHEN ARRAY_CONTAINS(tag_names, 'denver') THEN 'denver'
        WHEN ARRAY_CONTAINS(tag_names, 'slc')
          OR ARRAY_CONTAINS(tag_names, 'salt lake city')
          OR ARRAY_CONTAINS(tag_names, 'salt_lake_city') THEN 'slc'
        WHEN ARRAY_CONTAINS(tag_names, 'albuquerque') THEN 'albuquerque'
        ELSE 'N/A'
    END AS city,
    tags,
    current_odometer_meters,
    last_known_latitude,
    last_known_longitude,
    operational_status,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM normalized_vehicle_tags
