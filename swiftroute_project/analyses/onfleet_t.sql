SELECT *
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
WHERE task_id IN (
    SELECT task_id
    FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
    GROUP BY task_id
    HAVING COUNT(*) > 1
)
ORDER BY task_id;



SELECT *
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}



SELECT 'task_id' AS column_name,
       COUNT(CASE WHEN task_id IS NULL THEN 1 END) AS null_count
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'time_created',
       COUNT(CASE WHEN time_created IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'time_last_modified',
       COUNT(CASE WHEN time_last_modified IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'organization',
       COUNT(CASE WHEN organization IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'short_id',
       COUNT(CASE WHEN short_id IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'tracking_url',
       COUNT(CASE WHEN tracking_url IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'worker',
       COUNT(CASE WHEN worker IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'merchant',
       COUNT(CASE WHEN merchant IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'executor',
       COUNT(CASE WHEN executor IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'creator',
       COUNT(CASE WHEN creator IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'dependencies',
       COUNT(CASE WHEN dependencies IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'state',
       COUNT(CASE WHEN state IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'complete_after',
       COUNT(CASE WHEN complete_after IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'complete_before',
       COUNT(CASE WHEN complete_before IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'pickup_task',
       COUNT(CASE WHEN pickup_task IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'notes',
       COUNT(CASE WHEN notes IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'completion_details',
       COUNT(CASE WHEN completion_details IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'feedback_json',
       COUNT(CASE WHEN feedback_json IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'metadata',
       COUNT(CASE WHEN metadata IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'overrides_json',
       COUNT(CASE WHEN overrides_json IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'quantity',
       COUNT(CASE WHEN quantity IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'service_time',
       COUNT(CASE WHEN service_time IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'identity',
       COUNT(CASE WHEN identity IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'appearance',
       COUNT(CASE WHEN appearance IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'container',
       COUNT(CASE WHEN container IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'recipients',
       COUNT(CASE WHEN recipients IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'destination',
       COUNT(CASE WHEN destination IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'estimated_arrival_time',
       COUNT(CASE WHEN estimated_arrival_time IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'estimated_completion_time',
       COUNT(CASE WHEN estimated_completion_time IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'barcodes',
       COUNT(CASE WHEN barcodes IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'source_file_path',
       COUNT(CASE WHEN source_file_path IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'ingested_at',
       COUNT(CASE WHEN ingested_at IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }}
UNION ALL
SELECT 'ingestion_run_id',
       COUNT(CASE WHEN ingestion_run_id IS NULL THEN 1 END)
FROM {{ source('swiftroute_databricks', 'onfleet_deliveries') }};