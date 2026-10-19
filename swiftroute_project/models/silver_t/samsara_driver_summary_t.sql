{{
    config(
        materialized = 'table',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Samsara driver summary

    Grain: one row per Samsara driver ID.
    This API payload contains an aggregate summary but no row-level as-of or modified
    timestamp, so table materialization is used rather than an invented watermark.
    HOS violations remain nested and are surfaced via a count/flag for downstream risk views.
*/

WITH bronze_driver_summary AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'samsara_driver_summary') }}
),

normalized_driver_summary AS (
    SELECT
        COALESCE(NULLIF(TRIM(CAST(driver_id AS STRING)), ''), 'N/A') AS driver_id,
        COALESCE(NULLIF(TRIM(CAST(driver_name AS STRING)), ''), 'N/A') AS driver_name,
        CASE LOWER(TRIM(CAST(city AS STRING)))
            WHEN 'denver' THEN 'denver'
            WHEN 'salt_lake_city' THEN 'salt_lake_city'
            WHEN 'albuquerque' THEN 'albuquerque'
            ELSE COALESCE(NULLIF(LOWER(TRIM(CAST(city AS STRING))), ''), 'N/A')
        END AS city,
        COALESCE(TRY_CAST(total_distance_meters AS DECIMAL(20, 2)), CAST(0 AS DECIMAL(20, 2))) AS total_distance_meters,
        COALESCE(TRY_CAST(total_drive_time_ms AS BIGINT), CAST(0 AS BIGINT)) AS total_drive_time_ms,
        COALESCE(TRY_CAST(total_idle_time_ms AS BIGINT), CAST(0 AS BIGINT)) AS total_idle_time_ms,
        COALESCE(TRY_CAST(total_trips AS BIGINT), CAST(0 AS BIGINT)) AS total_trips,
        COALESCE(TRY_CAST(speeding_count AS BIGINT), CAST(0 AS BIGINT)) AS speeding_count,
        COALESCE(TRY_CAST(harsh_braking_count AS BIGINT), CAST(0 AS BIGINT)) AS harsh_braking_count,
        COALESCE(TRY_CAST(harsh_accel_count AS BIGINT), CAST(0 AS BIGINT)) AS harsh_acceleration_count,
        COALESCE(TRY_CAST(safety_score AS DECIMAL(6, 2)), CAST(0 AS DECIMAL(6, 2))) AS safety_score,
        COALESCE(
            FROM_JSON(
                NULLIF(TRIM(hos_violations_json), ''),
                'ARRAY<STRING>'
            ),
            FROM_JSON('[]', 'ARRAY<STRING>')
        ) AS hos_violations
    FROM bronze_driver_summary
)

SELECT
    driver_id,
    driver_name,
    city,
    total_distance_meters,
    total_drive_time_ms,
    total_idle_time_ms,
    total_trips,
    speeding_count,
    harsh_braking_count,
    harsh_acceleration_count,
    safety_score,
    hos_violations,
    COALESCE(SIZE(hos_violations), 0) AS hos_violation_count,
    COALESCE(SIZE(hos_violations), 0) > 0 AS has_hos_violations,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM normalized_driver_summary
