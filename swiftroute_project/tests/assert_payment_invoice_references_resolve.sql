WITH invoice_ids AS (
    SELECT DISTINCT invoice_id
    FROM {{ ref('quickbooks_invoices_t') }}
    WHERE invoice_id IS NOT NULL
),
linked_payment_lines AS (
    SELECT
        p.payment_id,
        line_position,
        line.linked_transactions[0].transaction_id AS linked_transaction_id
    FROM {{ ref('quickbooks_payments_t') }} p
    LATERAL VIEW OUTER POSEXPLODE(p.lines) payment_lines AS line_position, line
    WHERE SIZE(line.linked_transactions) = 1
      AND LOWER(TRIM(line.linked_transactions[0].transaction_type)) = 'invoice'
      AND line.linked_transactions[0].transaction_id IS NOT NULL
)
SELECT
    p.payment_id,
    p.line_position,
    p.linked_transaction_id
FROM linked_payment_lines p
LEFT ANTI JOIN invoice_ids i
    ON p.linked_transaction_id = i.invoice_id
