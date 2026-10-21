WITH source_applications AS (
    SELECT
        p_line.linked_transactions[0].transaction_id AS invoice_id,
        SUM(CAST(p_line.amount AS DECIMAL(18, 2))) AS source_applied_amount
    FROM {{ ref('quickbooks_payments_t') }} p
    LATERAL VIEW OUTER EXPLODE(p.lines) payment_lines AS p_line
    WHERE p_line.amount IS NOT NULL
      AND SIZE(p_line.linked_transactions) = 1
      AND LOWER(TRIM(p_line.linked_transactions[0].transaction_type)) = 'invoice'
      AND p_line.linked_transactions[0].transaction_id IS NOT NULL
    GROUP BY p_line.linked_transactions[0].transaction_id
)
SELECT
    ar.invoice_id,
    ar.linked_applied_payment_amount,
    s.source_applied_amount
FROM {{ ref('accounts_receivable') }} ar
LEFT JOIN source_applications s
    ON ar.invoice_id = s.invoice_id
WHERE ABS(
    COALESCE(ar.linked_applied_payment_amount, 0)
    - COALESCE(s.source_applied_amount, 0)
) > 0.01
