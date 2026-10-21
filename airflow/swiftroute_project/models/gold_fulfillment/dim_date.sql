{{
    config(
        schema = 'gold_fulfillment',
        alias = 'dim_date'
    )
}}

/*
    Calendar dimension spanning the available fulfillment activity dates.
    It is generated dynamically because this layer is a set of views.
    date_key -1 is reserved for a transaction without a usable date.
*/

WITH date_bounds AS (
    SELECT
        MIN(CAST(transaction_date AS DATE)) AS min_date,
        MAX(CAST(transaction_date AS DATE)) AS max_date
    FROM {{ ref('silver_business_fufillment') }}
    WHERE transaction_date IS NOT NULL
),
calendar_dates AS (
    SELECT EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY)) AS calendar_date
    FROM date_bounds
    WHERE min_date IS NOT NULL
      AND max_date IS NOT NULL
),
calendar_dimension AS (
    SELECT
        CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
        calendar_date,
        YEAR(calendar_date) AS calendar_year,
        QUARTER(calendar_date) AS calendar_quarter,
        MONTH(calendar_date) AS month_number,
        DATE_FORMAT(calendar_date, 'MMMM') AS month_name,
        DATE_FORMAT(calendar_date, 'yyyy-MM') AS year_month,
        DAYOFMONTH(calendar_date) AS day_of_month,
        DAYOFYEAR(calendar_date) AS day_of_year,
        DAYOFWEEK(calendar_date) AS day_of_week_number,
        DATE_FORMAT(calendar_date, 'EEEE') AS day_of_week_name,
        WEEKOFYEAR(calendar_date) AS week_of_year,
        CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFMONTH(calendar_date) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
        CASE WHEN calendar_date = LAST_DAY(calendar_date) THEN TRUE ELSE FALSE END AS is_month_end,
        FALSE AS is_unknown_date
    FROM calendar_dates
)

SELECT * FROM calendar_dimension

UNION ALL

SELECT
    -1 AS date_key,
    CAST(NULL AS DATE) AS calendar_date,
    CAST(NULL AS INT) AS calendar_year,
    CAST(NULL AS INT) AS calendar_quarter,
    CAST(NULL AS INT) AS month_number,
    'Unknown' AS month_name,
    'N/A' AS year_month,
    CAST(NULL AS INT) AS day_of_month,
    CAST(NULL AS INT) AS day_of_year,
    CAST(NULL AS INT) AS day_of_week_number,
    'Unknown' AS day_of_week_name,
    CAST(NULL AS INT) AS week_of_year,
    CAST(NULL AS BOOLEAN) AS is_weekend,
    CAST(NULL AS BOOLEAN) AS is_month_start,
    CAST(NULL AS BOOLEAN) AS is_month_end,
    TRUE AS is_unknown_date
