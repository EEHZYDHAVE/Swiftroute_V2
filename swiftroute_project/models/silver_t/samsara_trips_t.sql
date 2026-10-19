{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'trip_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Samsara trips

    Grain: one row per Samsara trip ID. The source trip ID is retained as trip_id; no
    surrogate ID is introduced.

    Source startMs/endMs are epoch milliseconds and are converted to timestamps.
    durationMs includes idling, so total_duration_ms and idling_duration_ms are retained
    separately and non_idle_duration_ms is calculated for route-efficiency use.
    US gallons are the canonical fuel unit. Milliliters are only a fallback source unit.

    There is no source last-modified timestamp. Incremental runs use trip_start_at_utc
    as an event-time watermark with a 62-day lookback to reprocess recent monthly files.
    This cannot guarantee capture of arbitrarily late changes to older trips; full refresh
    remains the safest way to reconcile historical source-file corrections.
*/

WITH bronze_trips AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'samsara_trips') }}
),

normalized_trips AS (
    SELECT
        COALESCE(NULLIF(TRIM(CAST(trip_id AS STRING)), ''), 'N/A') AS trip_id,
        COALESCE(NULLIF(TRIM(CAST(vehicle_id AS STRING)), ''), 'N/A') AS vehicle_id,
        COALESCE(NULLIF(TRIM(CAST(source_vehicle_name AS STRING)), ''), 'N/A') AS vehicle_name,
        COALESCE(
            NULLIF(REPLACE(TRIM(CAST(source_month AS STRING)), '_', '-'), ''),
            'N/A'
        ) AS trip_month,
        CASE
            WHEN driver_id IS NULL OR NULLIF(TRIM(CAST(driver_id AS STRING)), '') IS NULL THEN 'N/A'
            ELSE TRIM(CAST(driver_id AS STRING))
        END AS driver_id,
        CASE
            WHEN driver_id IS NULL OR NULLIF(TRIM(CAST(driver_id AS STRING)), '') IS NULL THEN 'unassigned'
            ELSE 'assigned'
        END AS driver_assignment_status,
        timestamp_millis(TRY_CAST(start_ms AS BIGINT)) AS trip_start_at_utc,
        timestamp_millis(TRY_CAST(end_ms AS BIGINT)) AS trip_end_at_utc,
        COALESCE(NULLIF(TRIM(CAST(start_address AS STRING)), ''), 'N/A') AS start_address,
        COALESCE(NULLIF(TRIM(CAST(end_address AS STRING)), ''), 'N/A') AS end_address,
        TRY_CAST(start_coords_latitude AS DOUBLE) AS start_latitude,
        TRY_CAST(start_coords_longitude AS DOUBLE) AS start_longitude,
        TRY_CAST(end_coords_latitude AS DOUBLE) AS end_latitude,
        TRY_CAST(end_coords_longitude AS DOUBLE) AS end_longitude,
        COALESCE(TRY_CAST(distance_meters AS DECIMAL(20, 2)), CAST(0 AS DECIMAL(20, 2))) AS distance_meters,
        COALESCE(TRY_CAST(duration_ms AS BIGINT), CAST(0 AS BIGINT)) AS total_duration_ms,
        COALESCE(TRY_CAST(driving_duration_ms AS BIGINT), CAST(0 AS BIGINT)) AS driving_duration_ms,
        COALESCE(TRY_CAST(idling_duration_ms AS BIGINT), CAST(0 AS BIGINT)) AS idling_duration_ms,
        COALESCE(
            TRY_CAST(fuel_consumed_gallons AS DECIMAL(18, 6)),
            TRY_CAST(fuel_consumed_ml AS DECIMAL(20, 6)) / CAST(3785.411784 AS DECIMAL(20, 6)),
            CAST(0 AS DECIMAL(18, 6))
        ) AS fuel_consumed_gallons,
        COALESCE(TRY_CAST(max_speed_mph AS DECIMAL(10, 2)), CAST(0 AS DECIMAL(10, 2))) AS max_speed_mph,
        COALESCE(TRY_CAST(average_speed_mph AS DECIMAL(10, 2)), CAST(0 AS DECIMAL(10, 2))) AS average_speed_mph,
        COALESCE(
            FROM_JSON(
                NULLIF(TRIM(safety_events_json), ''),
                'ARRAY<STRING>'
            ),
            FROM_JSON('[]', 'ARRAY<STRING>')
        ) AS safety_events
    FROM bronze_trips
),

eligible_trips AS (
    SELECT *
    FROM normalized_trips
    {% if is_incremental() %}
    WHERE trip_start_at_utc IS NULL
       OR trip_start_at_utc >= (
            SELECT COALESCE(
                MAX(existing_target.trip_start_at_utc) - INTERVAL 62 DAYS,
                CAST('1900-01-01 00:00:00' AS TIMESTAMP)
            )
            FROM {{ this }} AS existing_target
       )
    {% endif %}
)

SELECT
    trip_id,
    vehicle_id,
    vehicle_name,
    trip_month,
    driver_id,
    driver_assignment_status,
    trip_start_at_utc,
    trip_end_at_utc,
    start_address,
    end_address,
    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,
    distance_meters,
    total_duration_ms,
    driving_duration_ms,
    idling_duration_ms,
    GREATEST(total_duration_ms - idling_duration_ms, CAST(0 AS BIGINT)) AS non_idle_duration_ms,
    fuel_consumed_gallons,
    max_speed_mph,
    average_speed_mph,
    safety_events,
    COALESCE(SIZE(safety_events), 0) AS safety_event_count,
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_trips
