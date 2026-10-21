/*
    Singular dbt test: fact_delivery must preserve the complete one-row-per-delivery
    grain from the completed Silver Business Last-Mile model.
    A returned row indicates a failure.
*/
WITH expected AS (
    SELECT CAST(delivery_id AS STRING) AS delivery_id
    FROM {{ ref('silver_business_lastmile_delivery') }}
),
actual AS (
    SELECT CAST(delivery_id AS STRING) AS delivery_id
    FROM {{ ref('fact_delivery') }}
),
duplicates AS (
    SELECT delivery_id
    FROM actual
    GROUP BY delivery_id
    HAVING delivery_id IS NULL OR COUNT(*) > 1
),
missing_rows AS (
    SELECT e.delivery_id
    FROM expected e
    LEFT JOIN actual a ON a.delivery_id = e.delivery_id
    WHERE a.delivery_id IS NULL
),
unexpected_rows AS (
    SELECT a.delivery_id
    FROM actual a
    LEFT JOIN expected e ON e.delivery_id = a.delivery_id
    WHERE e.delivery_id IS NULL
)
SELECT 'DUPLICATE_OR_NULL_DELIVERY_ID' AS issue_type, delivery_id FROM duplicates
UNION ALL
SELECT 'MISSING_SILVER_DELIVERY' AS issue_type, delivery_id FROM missing_rows
UNION ALL
SELECT 'UNEXPECTED_FACT_DELIVERY' AS issue_type, delivery_id FROM unexpected_rows
