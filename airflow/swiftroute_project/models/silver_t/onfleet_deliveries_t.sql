{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'source_task_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Onfleet tasks

    Grain: one row per source_task_id.

    Key design choices:
      - The earliest timeCreated representation wins when a source task is duplicated.
      - task_id is a persistent, readable surrogate key. Existing assignments are reused;
        only new source_task_id values receive new sequential IDs during incremental runs.
      - Incremental filtering uses source_updated_at_utc, derived from time_last_modified.
      - Dependencies are mapped to task_id where possible. Unresolved source references are
        retained separately in unresolved_dependency_source_ids.
      - JSON held as strings in Bronze remains a JSON string in Silver where this model does
        not need to flatten the nested structure.
*/

WITH bronze_tasks AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
),

/*
    Only identify a canonical SwiftRoute organization ID when every non-null value found in
    organization, merchant, and executor is the same ID. This avoids normalizing arbitrary IDs.
*/
organization_identifier_values AS (
    SELECT CAST(organization AS STRING) AS organization_identifier
    FROM bronze_tasks

    UNION ALL

    SELECT CAST(merchant AS STRING) AS organization_identifier
    FROM bronze_tasks

    UNION ALL

    SELECT CAST(executor AS STRING) AS organization_identifier
    FROM bronze_tasks
),

known_swiftroute_org_id AS (
    SELECT
        CASE
            WHEN COUNT(DISTINCT organization_identifier) = 1
                THEN MAX(organization_identifier)
            ELSE CAST(NULL AS STRING)
        END AS organization_identifier
    FROM organization_identifier_values
    WHERE organization_identifier IS NOT NULL
),

source_task_rows AS (
    SELECT
        b.*,
        CAST(b.task_id AS STRING) AS source_task_id,
        timestamp_millis(CAST(b.time_created AS BIGINT)) AS task_created_at_utc,
        timestamp_millis(CAST(b.time_last_modified AS BIGINT)) AS source_updated_at_utc
    FROM bronze_tasks AS b
),

/*
    Duplicate representations are resolved before the incremental watermark is applied.
    source_file_path is a final deterministic tie-breaker if both source timestamps tie.
*/
deduplicated_source_tasks AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.source_task_id
                ORDER BY
                    s.time_created ASC NULLS LAST,
                    s.time_last_modified ASC NULLS LAST,
                    s.source_file_path ASC NULLS LAST
            ) AS source_duplicate_rank
        FROM source_task_rows AS s
    ) AS ranked_source_tasks
    WHERE source_duplicate_rank = 1
),

/*
    First run/full refresh: process the complete deduplicated source.
    Incremental run: include the inclusive upper watermark boundary as specified.
*/
eligible_source_tasks AS (
    SELECT *
    FROM deduplicated_source_tasks
    {% if is_incremental() %}
    WHERE source_updated_at_utc >= (
        SELECT COALESCE(
            MAX(existing_target.source_updated_at_utc),
            CAST('1900-01-01 00:00:00' AS TIMESTAMP)
        )
        FROM {{ this }} AS existing_target
    )
    {% endif %}
),

/*
    The existing target is the source of truth for already assigned surrogate IDs.
    When the target does not exist (initial build/full refresh), this CTE is empty.
*/
existing_task_mapping AS (
    {% if is_incremental() %}
    SELECT
        CAST(source_task_id AS STRING) AS source_task_id,
        CAST(task_id AS STRING) AS task_id
    FROM {{ this }}
    {% else %}
    SELECT
        CAST(NULL AS STRING) AS source_task_id,
        CAST(NULL AS STRING) AS task_id
    WHERE 1 = 0
    {% endif %}
),

current_max_task_number AS (
    SELECT
        COALESCE(
            MAX(
                TRY_CAST(
                    NULLIF(REGEXP_EXTRACT(task_id, '^task_id_([0-9]+)$', 1), '')
                    AS BIGINT
                )
            ),
            0
        ) AS max_task_number
    FROM existing_task_mapping
),

/*
    Assign sequential IDs only to genuinely new source IDs, starting after the highest
    number already present in the target. Ordering new source IDs keeps each batch deterministic.
*/
new_task_assignments AS (
    SELECT
        eligible.source_task_id,
        CONCAT(
            'task_id_',
            CAST(
                current_max.max_task_number
                + ROW_NUMBER() OVER (ORDER BY eligible.source_task_id ASC NULLS FIRST)
                AS STRING
            )
        ) AS task_id
    FROM eligible_source_tasks AS eligible
    LEFT ANTI JOIN existing_task_mapping AS existing
        ON existing.source_task_id <=> eligible.source_task_id
    CROSS JOIN current_max_task_number AS current_max
),

/* All known mappings: existing assignments plus IDs assigned to new tasks in this batch. */
task_id_mapping AS (
    SELECT source_task_id, task_id
    FROM existing_task_mapping

    UNION ALL

    SELECT source_task_id, task_id
    FROM new_task_assignments
),

dependency_lookup AS (
    SELECT
        MAP_FROM_ENTRIES(
            COLLECT_LIST(NAMED_STRUCT('key', source_task_id, 'value', task_id))
        ) AS source_to_surrogate_map
    FROM task_id_mapping
    WHERE source_task_id IS NOT NULL
),

task_components AS (
    SELECT
        eligible.*,
        ELEMENT_AT(eligible.recipients, 1) AS recipient_record
    FROM eligible_source_tasks AS eligible
),

task_metadata_fields AS (
    SELECT
        eligible.source_task_id,
        GET(
            FILTER(eligible.metadata, metadata_item -> metadata_item.name = 'service_type'),
            0
        ) AS service_type_entry,
        GET(
            FILTER(eligible.metadata, metadata_item -> metadata_item.name = 'client_id'),
            0
        ) AS client_id_entry,
        GET(
            FILTER(eligible.metadata, metadata_item -> metadata_item.name = 'order_value'),
            0
        ) AS order_value_entry,
        GET(
            FILTER(eligible.metadata, metadata_item -> metadata_item.name = 'zone_id'),
            0
        ) AS zone_id_entry,
        GET(
            FILTER(eligible.metadata, metadata_item -> metadata_item.name = 'vehicle_type'),
            0
        ) AS vehicle_type_entry
    FROM eligible_source_tasks AS eligible
)

SELECT
    /* 1. Identity, organization, and processing audit */
    key_mapping.task_id AS task_id,
    t.source_task_id AS source_task_id,
    t.task_created_at_utc AS task_created_at_utc,
    t.source_updated_at_utc AS source_updated_at_utc,
    CASE
        WHEN org.organization_identifier IS NOT NULL
         AND CAST(t.organization AS STRING) = org.organization_identifier
            THEN 'SwiftRoute'
        ELSE CAST(t.organization AS STRING)
    END AS organization_name,
    CAST(t.tracking_url AS STRING) AS tracking_url,
    CASE
        WHEN t.notes IS NULL THEN NULL
        WHEN t.notes = '' THEN 'N/A'
        ELSE CAST(t.notes AS STRING)
    END AS task_notes,
    CASE
        WHEN org.organization_identifier IS NOT NULL
         AND CAST(t.creator AS STRING) = org.organization_identifier
            THEN 'SwiftRoute'
        ELSE CAST(t.creator AS STRING)
    END AS creator_id,
    CURRENT_TIMESTAMP() AS processed_at_utc,

    /* 2. Assignment and task control */
    CASE
        WHEN t.worker IS NULL OR TRIM(CAST(t.worker AS STRING)) = '' THEN 'N/A'
        ELSE CAST(t.worker AS STRING)
    END AS worker_id,
    CASE
        WHEN t.worker IS NULL OR TRIM(CAST(t.worker AS STRING)) = '' THEN TRUE
        ELSE FALSE
    END AS worker_reference_missing,
    CAST(t.state AS INT) AS task_state,
    CAST(t.pickup_task AS BOOLEAN) AS pickup_task,
    CASE
        WHEN t.dependencies IS NULL THEN CAST(NULL AS ARRAY<STRING>)
        ELSE TRANSFORM(
            FILTER(
                t.dependencies,
                dependency_source_id -> ELEMENT_AT(
                    dependency_map.source_to_surrogate_map,
                    dependency_source_id
                ) IS NOT NULL
            ),
            dependency_source_id -> ELEMENT_AT(
                dependency_map.source_to_surrogate_map,
                dependency_source_id
            )
        )
    END AS dependency_task_ids,
    CASE
        WHEN t.dependencies IS NULL THEN CAST(NULL AS ARRAY<STRING>)
        ELSE FILTER(
            t.dependencies,
            dependency_source_id -> ELEMENT_AT(
                dependency_map.source_to_surrogate_map,
                dependency_source_id
            ) IS NULL
        )
    END AS unresolved_dependency_source_ids,
    CAST(t.quantity AS BIGINT) AS quantity,
    CAST(t.service_time AS BIGINT) AS service_time,
    CASE
        WHEN t.container.type IS NULL THEN NULL
        WHEN LOWER(TRIM(CAST(t.container.type AS STRING))) = 'worker' THEN 'worker'
        WHEN LOWER(TRIM(CAST(t.container.type AS STRING))) = 'organization' THEN 'organization'
        ELSE LOWER(TRIM(CAST(t.container.type AS STRING)))
    END AS container_owner_type,
    CASE
        WHEN LOWER(TRIM(CAST(t.container.type AS STRING))) = 'organization'
         AND org.organization_identifier IS NOT NULL
         AND CAST(t.container.worker AS STRING) = org.organization_identifier
            THEN 'SwiftRoute'
        ELSE CAST(t.container.worker AS STRING)
    END AS container_owner_id,

    /* 3. Scheduling timestamps */
    timestamp_millis(CAST(t.complete_after AS BIGINT)) AS complete_after_utc,
    timestamp_millis(CAST(t.complete_before AS BIGINT)) AS complete_before_utc,
    timestamp_millis(CAST(t.estimated_arrival_time AS BIGINT)) AS estimated_arrival_time_utc,
    timestamp_millis(CAST(t.estimated_completion_time AS BIGINT)) AS estimated_completion_time_utc,

    /* 4. Completion details. JSON-bearing columns remain JSON strings. */
    CAST(t.completion_details.result AS STRING) AS completion_result,
    CASE
        WHEN t.completion_details.failure_reason = '' THEN 'N/A'
        ELSE CAST(t.completion_details.failure_reason AS STRING)
    END AS completion_failure_reason,
    CASE
        WHEN t.completion_details.success_notes = '' THEN 'N/A'
        ELSE CAST(t.completion_details.success_notes AS STRING)
    END AS completion_success_notes,
    CASE
        WHEN t.completion_details.success_evidence = '' THEN 'N/A'
        ELSE CAST(t.completion_details.success_evidence AS STRING)
    END AS completion_success_evidence,
    CAST(t.completion_details.photo_upload_id AS STRING) AS completion_photo_upload_id,
    t.completion_details.photo_upload_ids AS completion_photo_upload_ids,
    CAST(t.completion_details.signature_upload_id AS STRING) AS completion_signature_upload_id,
    timestamp_millis(CAST(t.completion_details.time AS BIGINT)) AS completion_time_utc,
    CAST(t.completion_details.first_photo_upload_id AS STRING) AS completion_first_photo_upload_id,
    CAST(t.completion_details.unavailable_attachments_json AS STRING) AS completion_unavailable_attachments,
    CAST(t.completion_details.actions_json AS STRING) AS completion_actions,
    CAST(t.completion_details.distance AS DOUBLE) AS completion_distance,

    /* 5. Recipient. The one-recipient source array is flattened without changing task grain. */
    CAST(t.recipient_record.name AS STRING) AS recipient_name,
    CAST(t.recipient_record.phone AS STRING) AS recipient_phone,
    CASE
        WHEN t.recipient_record IS NULL THEN NULL
        WHEN t.recipient_record.notes = '' THEN 'N/A'
        ELSE CAST(t.recipient_record.notes AS STRING)
    END AS recipient_notes,
    CAST(t.recipient_record.skip_sms_notifications AS BOOLEAN) AS skip_sms_notifications,

    /* 6. Destination. The source coordinate order is [longitude, latitude]. */
    CAST(t.destination.id AS STRING) AS destination_id,
    CAST(ELEMENT_AT(t.destination.location, 1) AS DOUBLE) AS longitude,
    CAST(ELEMENT_AT(t.destination.location, 2) AS DOUBLE) AS latitude,
    CAST(t.destination.address.apartment AS STRING) AS destination_apartment,
    CAST(t.destination.address.state AS STRING) AS destination_state,
    CAST(t.destination.address.postal_code AS STRING) AS destination_postal_code,
    CAST(t.destination.address.country AS STRING) AS destination_country,
    CAST(t.destination.address.city AS STRING) AS destination_city,
    CAST(t.destination.address.street AS STRING) AS destination_street,
    CAST(t.destination.address.number AS STRING) AS destination_number,
    CAST(t.destination.address.name AS STRING) AS destination_name,
    CASE
        WHEN t.destination.notes IS NULL THEN NULL
        WHEN t.destination.notes = '' THEN 'N/A'
        ELSE CAST(t.destination.notes AS STRING)
    END AS destination_notes,

    /* 7. Known custom metadata. Values are decoded from their JSON scalar representation. */
    COALESCE(
        NULLIF(GET_JSON_OBJECT(metadata.service_type_entry.value_json, '$'), ''),
        'N/A'
    ) AS service_type,
    COALESCE(
        NULLIF(GET_JSON_OBJECT(metadata.client_id_entry.value_json, '$'), ''),
        'N/A'
    ) AS client_id,
    CASE
        WHEN LOWER(CAST(metadata.order_value_entry.`type` AS STRING)) IN (
            'number', 'numeric', 'integer', 'int', 'long', 'float', 'double', 'decimal'
        )
            THEN TRY_CAST(
                GET_JSON_OBJECT(metadata.order_value_entry.value_json, '$')
                AS DECIMAL(18, 2)
            )
        ELSE CAST(NULL AS DECIMAL(18, 2))
    END AS order_value,
    COALESCE(
        NULLIF(GET_JSON_OBJECT(metadata.zone_id_entry.value_json, '$'), ''),
        'N/A'
    ) AS zone_id,
    COALESCE(
        NULLIF(GET_JSON_OBJECT(metadata.vehicle_type_entry.value_json, '$'), ''),
        'N/A'
    ) AS vehicle_type,

    /* 8. Retained barcode and identity fields */
    t.barcodes.`require` AS barcode_requirements,
    t.barcodes.capture AS barcode_capture,
    CAST(t.barcodes.capture_max_count AS BIGINT) AS barcode_capture_max_count,
    CAST(t.identity.failed_scan_count AS BIGINT) AS failed_scan_count

FROM task_components AS t
INNER JOIN task_id_mapping AS key_mapping
    ON key_mapping.source_task_id <=> t.source_task_id
LEFT JOIN task_metadata_fields AS metadata
    ON metadata.source_task_id <=> t.source_task_id
CROSS JOIN known_swiftroute_org_id AS org
CROSS JOIN dependency_lookup AS dependency_map