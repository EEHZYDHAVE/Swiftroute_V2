{{
    config(
        schema = 'gold_last_mile',
        alias = 'dim_date_l'
    )
}}

/*
    Calendar dimension spanning all task-created, delivery-event and completion dates
    observed in the Silver Business delivery view, plus an explicit unknown-date row.
    Date key is YYYYMMDD; -1 represents an unavailable date.
*/
WITH delivery_dates AS (
    SELECT CAST(task_created_at_utc AS DATE) AS calendar_date
    FROM {{ ref('silver_business_lastmile_delivery') }}
    WHERE task_created_at_utc IS NOT NULL

    UNION

    SELECT CAST(delivery_event_date AS DATE) AS calendar_date
    FROM {{ ref('silver_business_lastmile_delivery') }}
    WHERE delivery_event_date IS NOT NULL

    UNION

    SELECT CAST(completion_time_utc AS DATE) AS calendar_date
    FROM {{ ref('silver_business_lastmile_delivery') }}
    WHERE completion_time_utc IS NOT NULL
),
date_bounds AS (
    SELECT MIN(calendar_date) AS min_date, MAX(calendar_date) AS max_date
    FROM delivery_dates
),
calendar AS (
    SELECT EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY)) AS calendar_date
    FROM date_bounds
    WHERE min_date IS NOT NULL AND max_date IS NOT NULL
)
SELECT
    CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
    calendar_date,
    YEAR(calendar_date) AS calendar_year,
    QUARTER(calendar_date) AS calendar_quarter,
    CONCAT(CAST(YEAR(calendar_date) AS STRING), '-Q', CAST(QUARTER(calendar_date) AS STRING)) AS year_quarter,
    MONTH(calendar_date) AS month_number,
    DATE_FORMAT(calendar_date, 'MMMM') AS month_name,
    DATE_FORMAT(calendar_date, 'yyyy-MM') AS year_month,
    DAYOFMONTH(calendar_date) AS day_of_month,
    DAYOFWEEK(calendar_date) AS day_of_week_number,
    DATE_FORMAT(calendar_date, 'EEEE') AS day_name,
    WEEKOFYEAR(calendar_date) AS week_of_year,
    CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_unknown_date
FROM calendar

UNION ALL

SELECT
    -1 AS date_key,
    CAST(NULL AS DATE) AS calendar_date,
    CAST(NULL AS INT) AS calendar_year,
    CAST(NULL AS INT) AS calendar_quarter,
    'N/A' AS year_quarter,
    CAST(NULL AS INT) AS month_number,
    'N/A' AS month_name,
    'N/A' AS year_month,
    CAST(NULL AS INT) AS day_of_month,
    CAST(NULL AS INT) AS day_of_week_number,
    'N/A' AS day_name,
    CAST(NULL AS INT) AS week_of_year,
    CAST(NULL AS BOOLEAN) AS is_weekend,
    TRUE AS is_unknown_date
