SELECT invoice_id
FROM {{ ref('accounts_receivable') }}
GROUP BY invoice_id
HAVING COUNT(*) > 1
