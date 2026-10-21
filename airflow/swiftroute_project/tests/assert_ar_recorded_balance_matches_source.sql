SELECT
    ar.invoice_id,
    ar.recorded_current_balance,
    i.balance_amount AS source_balance_amount
FROM {{ ref('accounts_receivable') }} ar
JOIN {{ ref('quickbooks_invoices_t') }} i
    ON ar.invoice_id = i.invoice_id
WHERE
    (ar.recorded_current_balance IS NULL AND i.balance_amount IS NOT NULL)
    OR (ar.recorded_current_balance IS NOT NULL AND i.balance_amount IS NULL)
    OR ABS(COALESCE(ar.recorded_current_balance, 0) - COALESCE(i.balance_amount, 0)) > 0.01
