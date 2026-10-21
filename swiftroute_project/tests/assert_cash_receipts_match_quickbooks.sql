WITH cash_receipts AS (
    SELECT
        transaction_id AS payment_id,
        SUM(CAST(amount AS DECIMAL(18, 2))) AS modeled_cash_received
    FROM {{ ref('financial_transaction_lines') }}
    WHERE transaction_type = 'payment'
      AND amount_role = 'cash_received'
    GROUP BY transaction_id
)
SELECT
    p.payment_id,
    p.total_amount AS source_total_amount,
    c.modeled_cash_received
FROM {{ ref('quickbooks_payments_t') }} p
LEFT JOIN cash_receipts c
    ON p.payment_id = c.payment_id
WHERE p.total_amount IS NOT NULL
  AND (
       c.payment_id IS NULL
       OR ABS(COALESCE(p.total_amount, 0) - COALESCE(c.modeled_cash_received, 0)) > 0.01
  )
