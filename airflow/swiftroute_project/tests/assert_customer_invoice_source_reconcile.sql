-- Reconcile Gold invoice count, billed amount, and current balance against
-- QuickBooks invoices aggregated independently to the same customer-month grain.
WITH expected AS (
    SELECT
        TRIM(client_id) AS client_id,
        TO_DATE(CONCAT(TRIM(billing_period), '-01')) AS period_month,
        COUNT(DISTINCT invoice_id) AS invoice_count,
        SUM(COALESCE(total_amount, 0)) AS invoiced_amount,
        SUM(COALESCE(balance_amount, 0)) AS current_outstanding_balance
    FROM {{ ref('quickbooks_invoices_t') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND TRIM(billing_period) RLIKE '^[0-9]{4}-[0-9]{2}$'
    GROUP BY TRIM(client_id), TO_DATE(CONCAT(TRIM(billing_period), '-01'))
),
actual AS (
    SELECT
        client_id,
        period_month,
        invoice_count,
        invoiced_amount,
        current_outstanding_balance_for_billing_period AS current_outstanding_balance
    FROM {{ ref('fact_customer_economics') }}
)
SELECT
    COALESCE(e.client_id, a.client_id) AS client_id,
    COALESCE(e.period_month, a.period_month) AS period_month,
    e.invoice_count AS expected_invoice_count,
    a.invoice_count AS actual_invoice_count,
    e.invoiced_amount AS expected_invoiced_amount,
    a.invoiced_amount AS actual_invoiced_amount,
    e.current_outstanding_balance AS expected_current_outstanding_balance,
    a.current_outstanding_balance AS actual_current_outstanding_balance
FROM expected e
FULL OUTER JOIN actual a
    ON e.client_id = a.client_id
   AND e.period_month = a.period_month
WHERE COALESCE(e.invoice_count, 0) <> COALESCE(a.invoice_count, 0)
   OR ABS(COALESCE(e.invoiced_amount, 0) - COALESCE(a.invoiced_amount, 0)) > 0.02
   OR ABS(
        COALESCE(e.current_outstanding_balance, 0)
        - COALESCE(a.current_outstanding_balance, 0)
      ) > 0.02
