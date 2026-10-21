SELECT transaction_line_key
FROM {{ ref('financial_transaction_lines') }}
GROUP BY transaction_line_key
HAVING COUNT(*) > 1
