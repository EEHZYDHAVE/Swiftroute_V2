SELECT *
FROM {{ ref('financial_reporting_monthly') }}
WHERE
    (
        financial_area = 'REVENUE'
        AND ABS(
            COALESCE(pnl_signed_amount, 0)
            - COALESCE(recorded_billed_revenue_amount, 0)
        ) > 0.01
    )
    OR
    (
        financial_area = 'OPERATING_EXPENSE'
        AND ABS(
            COALESCE(pnl_signed_amount, 0)
            + COALESCE(operating_expense_amount, 0)
        ) > 0.01
    )
    OR
    (
        financial_area NOT IN ('REVENUE', 'OPERATING_EXPENSE')
        AND ABS(COALESCE(pnl_signed_amount, 0)) > 0.01
    )
