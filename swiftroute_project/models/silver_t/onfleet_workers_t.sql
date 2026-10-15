{{
    config(
        materialized = 'table',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Onfleet workers

    Grain: one row per worker_id.

    Design choices:
      - Retain the original worker identifier as worker_id; no surrogate key is required.
      - Flatten vehicle attributes and the employment_type metadata entry.
      - Preserve teams as an array so the worker grain does not change.
      - Convert source epoch-millisecond values to UTC timestamps.
      - Normalize the known organization identifier to SwiftRoute only when the source
        contains one unambiguous non-null organization identifier.
      - If duplicate source representations exist for a worker, keep the most recently
        modified representation; use creation time and source path as deterministic tie-breakers.
*/

WITH bronze_workers AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'onfleet_workers') }}
),

known_swiftroute_org_id AS (
    SELECT
        CASE
            WHEN COUNT(DISTINCT CAST(organization AS STRING)) = 1
                THEN MAX(CAST(organization AS STRING))
            ELSE CAST(NULL AS STRING)
        END AS organization_identifier
    FROM bronze_workers
    WHERE organization IS NOT NULL
),

ranked_workers AS (
    SELECT
        w.*,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(w.id AS STRING)
            ORDER BY
                w.time_last_modified DESC NULLS LAST,
                w.time_created DESC NULLS LAST,
                w.source_file_path ASC NULLS LAST
        ) AS source_worker_rank
    FROM bronze_workers AS w
),

deduplicated_workers AS (
    SELECT *
    FROM ranked_workers
    WHERE source_worker_rank = 1
),

worker_metadata AS (
    SELECT
        w.*,
        ELEMENT_AT(
            FILTER(
                w.metadata,
                metadata_item -> metadata_item.name = 'employment_type'
            ),
            1
        ) AS employment_type_entry
    FROM deduplicated_workers AS w
)

SELECT
    /* Identity and organization */
    CAST(w.id AS STRING) AS worker_id,
    CASE
        WHEN org.organization_identifier IS NOT NULL
         AND CAST(w.organization AS STRING) = org.organization_identifier
            THEN 'SwiftRoute'
        ELSE CAST(w.organization AS STRING)
    END AS organization_name,

    /* Worker details */
    CAST(w.name AS STRING) AS worker_name,
    CAST(w.display_name AS STRING) AS display_name,
    CAST(w.phone AS STRING) AS phone,
    w.teams AS teams,

    /* Vehicle details */
    CAST(w.vehicle.id AS STRING) AS vehicle_id,
    CASE
        WHEN w.vehicle.type IS NULL THEN NULL
        ELSE LOWER(TRIM(CAST(w.vehicle.type AS STRING)))
    END AS vehicle_type,
    CAST(w.vehicle.description AS STRING) AS vehicle_description,
    CAST(w.vehicle.license_plate AS STRING) AS vehicle_license_plate,
    CAST(w.vehicle.color AS STRING) AS vehicle_color,

    /* Image and source record timestamps */
    CAST(w.image_url AS STRING) AS image_url,
    timestamp_millis(CAST(w.time_created AS BIGINT)) AS worker_created_at_utc,
    timestamp_millis(CAST(w.time_last_modified AS BIGINT)) AS source_updated_at_utc,

    /* Duty status and active task reference */
    CAST(w.on_duty AS BOOLEAN) AS on_duty,
    CAST(w.active_task AS STRING) AS active_task_id,

    /* Metadata: employment_type is a string output, regardless of JSON storage encoding. */
    COALESCE(
        NULLIF(
            GET_JSON_OBJECT(w.employment_type_entry.value, '$'),
            ''
        ),
        NULLIF(CAST(w.employment_type_entry.value AS STRING), ''),
        'N/A'
    ) AS employment_type

FROM worker_metadata AS w
CROSS JOIN known_swiftroute_org_id AS org
