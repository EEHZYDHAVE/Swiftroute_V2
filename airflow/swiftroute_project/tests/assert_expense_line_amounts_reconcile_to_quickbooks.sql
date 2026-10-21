WITH source_line_totals AS (
    SELECT
        e.expense_id,
        e.total_amount,
        SUM(
            CASE
                WHEN line_position IS NULL THEN e.total_amount
                ELSE line.amount
            END
        ) AS line_total_amount
    FROM {{ ref('quickbooks_expenses_t') }} e
    LATERAL VIEW OUTER POSEXPLODE(e.lines) exploded_expense_lines AS line_position, line
    GROUP BY e.expense_id, e.total_amount
)
SELECT
    expense_id,
    total_amount,
    line_total_amount,
    total_amount - line_total_amount AS reconciliation_variance
FROM source_line_totals
WHERE total_amount IS NOT NULL
  AND (
      line_total_amount IS NULL
      OR ABS(total_amount - line_total_amount) > 0.01
  )
