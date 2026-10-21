/*
    Singular dbt test: the Silver Business delivery model must contain exactly
    the non-pickup Onfleet task keys, with no missing, extra, or duplicate rows.

    A returned row indicates a test failure.
*/
WITH expected_deliveries AS (
    SELECT CAST(task_id AS STRING) AS delivery_id
    FROM {{ ref('onfleet_deliveries_t') }}
    WHERE COALESCE(CAST(pickup_task AS BOOLEAN), FALSE) = FALSE
),

actual_deliveries AS (
    SELECT CAST(delivery_id AS STRING) AS delivery_id
    FROM {{ ref('silver_business_lastmile_delivery') }}
),

duplicate_actual_keys AS (
    SELECT delivery_id, COUNT(*) AS key_count
    FROM actual_deliveries
    GROUP BY delivery_id
    HAVING delivery_id IS NULL OR COUNT(*) > 1
),

missing_deliveries AS (
    SELECT e.delivery_id
    FROM expected_deliveries AS e
    LEFT JOIN actual_deliveries AS a
        ON a.delivery_id = e.delivery_id
    WHERE a.delivery_id IS NULL
),

unexpected_deliveries AS (
    SELECT a.delivery_id
    FROM actual_deliveries AS a
    LEFT JOIN expected_deliveries AS e
        ON e.delivery_id = a.delivery_id
    WHERE e.delivery_id IS NULL
)

SELECT
    'DUPLICATE_OR_NULL_DELIVERY_KEY' AS issue_type,
    delivery_id
FROM duplicate_actual_keys

UNION ALL

SELECT
    'MISSING_NON_PICKUP_TASK' AS issue_type,
    delivery_id
FROM missing_deliveries

UNION ALL

SELECT
    'UNEXPECTED_DELIVERY_KEY' AS issue_type,
    delivery_id
FROM unexpected_deliveries
