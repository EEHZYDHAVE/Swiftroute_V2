{{
    config(
        schema = 'gold_last_mile',
        alias = 'dim_service'
    )
}}

/*
    Service dimension based on observed Onfleet service_type metadata.
    The mappings below are presentation groupings only; they do not affect rate matching.
*/
WITH observed_services AS (
    SELECT DISTINCT
        COALESCE(NULLIF(NULLIF(LOWER(TRIM(CAST(service_type AS STRING))), ''), 'n/a'), 'N/A') AS service_key
    FROM {{ ref('silver_business_lastmile_delivery') }}
    WHERE service_type IS NOT NULL
),
service_rows AS (
    SELECT service_key
    FROM observed_services
    WHERE service_key <> 'N/A'
)
SELECT
    service_key,
    service_key AS service_type,
    CASE service_key
        WHEN 'same_day' THEN 'Same Day'
        WHEN 'next_day' THEN 'Next Day'
        WHEN 'scheduled' THEN 'Scheduled'
        WHEN 'distribution_run' THEN 'Distribution Run'
        ELSE service_key
    END AS service_name,
    CASE service_key
        WHEN 'same_day' THEN 'EXPEDITED'
        WHEN 'next_day' THEN 'NEXT_DAY'
        WHEN 'scheduled' THEN 'SCHEDULED'
        WHEN 'distribution_run' THEN 'DISTRIBUTION'
        ELSE 'OTHER'
    END AS service_group,
    (service_key = 'same_day') AS is_same_day_service,
    (service_key = 'next_day') AS is_next_day_service,
    (service_key = 'scheduled') AS is_scheduled_service,
    (service_key = 'distribution_run') AS is_distribution_service,
    FALSE AS is_unknown_service
FROM service_rows

UNION ALL

SELECT
    'N/A' AS service_key,
    'N/A' AS service_type,
    'Unknown Service' AS service_name,
    'OTHER' AS service_group,
    CAST(NULL AS BOOLEAN) AS is_same_day_service,
    CAST(NULL AS BOOLEAN) AS is_next_day_service,
    CAST(NULL AS BOOLEAN) AS is_scheduled_service,
    CAST(NULL AS BOOLEAN) AS is_distribution_service,
    TRUE AS is_unknown_service
