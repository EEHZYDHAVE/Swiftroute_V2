WITH payment_lines AS (
    SELECT
        p.payment_id,
        p.total_amount,
        p.unapplied_amount,
        line.amount AS payment_line_amount,
        line.linked_transactions

    FROM silver_t.quickbooks_payments_t p

    LATERAL VIEW OUTER EXPLODE(p.lines) exploded AS line
),

payment_summary AS (
    SELECT
        payment_id,

        MAX(total_amount) AS total_payment_amount,
        MAX(unapplied_amount) AS unapplied_amount,

        SUM(COALESCE(payment_line_amount, 0))
            AS sum_payment_line_amount,

        SUM(
            CASE
                WHEN COALESCE(SIZE(linked_transactions), 0) > 1
                THEN 1
                ELSE 0
            END
        ) AS lines_with_multiple_links

    FROM payment_lines
    GROUP BY payment_id
)

SELECT
    COUNT(*) AS payment_count,

    COUNT_IF(lines_with_multiple_links > 0)
        AS payments_with_multiple_linked_transactions,

    COUNT_IF(
        total_payment_amount IS NOT NULL
        AND ABS(
            total_payment_amount
            - sum_payment_line_amount
            - COALESCE(unapplied_amount, 0)
        ) > 0.01
    ) AS payment_total_reconciliation_mismatches,

    MAX(
        ABS(
            total_payment_amount
            - sum_payment_line_amount
            - COALESCE(unapplied_amount, 0)
        )
    ) AS maximum_reconciliation_difference

FROM payment_summary;