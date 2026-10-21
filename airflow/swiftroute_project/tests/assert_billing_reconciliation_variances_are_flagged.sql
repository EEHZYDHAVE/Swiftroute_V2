SELECT *
FROM {{ ref('billing_reconciliation') }}
WHERE
    (
        reconciliation_status = 'MATCHED'
        AND (
            ABS(COALESCE(actual_minus_invoice_ready_variance, 0)) > 0.01
            OR COALESCE(review_line_count, 0) > 0
        )
    )
    OR
    (
        ABS(COALESCE(actual_minus_invoice_ready_variance, 0)) > 0.01
        AND reconciliation_status NOT IN (
            'ACTIVITY_REQUIRES_REVIEW',
            'WEEKLY_CADENCE_MONTHLY_ROLLUP_ONLY',
            'ACTUAL_INVOICE_WITHOUT_READY_BILLING_LINE',
            'READY_BILLING_WITHOUT_ACTUAL_INVOICE',
            'VARIANCE_REQUIRES_REVIEW'
        )
    )
