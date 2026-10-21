SELECT *
FROM {{ ref('billing_invoice_lines') }}
WHERE COALESCE(candidate_quantity, 0) < 0
   OR COALESCE(billable_quantity, 0) < 0
