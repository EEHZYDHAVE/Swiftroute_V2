WITH line_totals AS (
    SELECT
        transaction_id AS invoice_id,
        SUM(amount) AS line_total_amount
    FROM {{ ref('financial_transaction_lines') }}
    WHERE amount_role = 'billed_revenue'
      AND amount IS NOT NULL
    GROUP BY transaction_id
)
SELECT
    i.invoice_id,
    i.total_amount,
    l.line_total_amount,
    i.total_amount - l.line_total_amount AS reconciliation_variance
FROM {{ ref('quickbooks_invoices_t') }} i
LEFT JOIN line_totals l
    ON i.invoice_id = l.invoice_id
WHERE i.total_amount IS NOT NULL
  AND (
       l.invoice_id IS NULL
       OR ABS(COALESCE(i.total_amount, 0) - COALESCE(l.line_total_amount, 0)) > 0.01
  )
