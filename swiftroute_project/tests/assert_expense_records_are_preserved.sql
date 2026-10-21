SELECT e.expense_id
FROM {{ ref('quickbooks_expenses_t') }} e
LEFT ANTI JOIN {{ ref('financial_transaction_lines') }} f
    ON e.expense_id = f.transaction_id
   AND f.transaction_type = 'expense'
   AND f.amount_role = 'operating_expense'
WHERE e.expense_id IS NOT NULL

UNION ALL

SELECT f.transaction_id AS expense_id
FROM {{ ref('financial_transaction_lines') }} f
LEFT ANTI JOIN {{ ref('quickbooks_expenses_t') }} e
    ON f.transaction_id = e.expense_id
WHERE f.transaction_type = 'expense'
  AND f.amount_role = 'operating_expense'
  AND f.transaction_id IS NOT NULL
