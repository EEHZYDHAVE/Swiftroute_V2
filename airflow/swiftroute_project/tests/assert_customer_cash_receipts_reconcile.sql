-- Cash received is assigned to the payment transaction month and should reconcile
-- directly to QuickBooks payment records independently aggregated at client-month.
WITH expected AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(transaction_date AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT payment_id) AS payments_received_count,
        SUM(COALESCE(total_amount, 0)) AS cash_received_amount
    FROM {{ ref('quickbooks_payments_t') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND transaction_date IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(transaction_date AS DATE), 'MM')
),
actual AS (
    SELECT
        client_id,
        period_month,
        payments_received_count,
        cash_received_amount
    FROM {{ ref('fact_customer_economics') }}
)
SELECT
    COALESCE(e.client_id, a.client_id) AS client_id,
    COALESCE(e.period_month, a.period_month) AS period_month,
    e.payments_received_count AS expected_payment_count,
    a.payments_received_count AS actual_payment_count,
    e.cash_received_amount AS expected_cash_received_amount,
    a.cash_received_amount AS actual_cash_received_amount
FROM expected e
FULL OUTER JOIN actual a
    ON e.client_id = a.client_id
   AND e.period_month = a.period_month
WHERE COALESCE(e.payments_received_count, 0) <> COALESCE(a.payments_received_count, 0)
   OR ABS(COALESCE(e.cash_received_amount, 0) - COALESCE(a.cash_received_amount, 0)) > 0.02
