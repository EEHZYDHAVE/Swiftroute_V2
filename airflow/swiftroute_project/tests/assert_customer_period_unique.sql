-- The Gold fact must contain one row per canonical customer per calendar month.
SELECT
    client_id,
    period_month,
    COUNT(*) AS row_count
FROM {{ ref('fact_customer_economics') }}
GROUP BY client_id, period_month
HAVING COUNT(*) > 1
