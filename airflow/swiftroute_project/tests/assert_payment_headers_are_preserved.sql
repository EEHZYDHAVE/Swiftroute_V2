SELECT p.payment_id
FROM {{ ref('quickbooks_payments_t') }} p
LEFT ANTI JOIN {{ ref('financial_transaction_lines') }} f
    ON p.payment_id = f.transaction_id
   AND f.amount_role = 'cash_received'
WHERE p.payment_id IS NOT NULL

UNION ALL

SELECT f.transaction_id AS payment_id
FROM {{ ref('financial_transaction_lines') }} f
LEFT ANTI JOIN {{ ref('quickbooks_payments_t') }} p
    ON f.transaction_id = p.payment_id
WHERE f.amount_role = 'cash_received'
  AND f.transaction_id IS NOT NULL
