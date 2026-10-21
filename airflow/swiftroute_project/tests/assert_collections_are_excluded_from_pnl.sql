SELECT *
FROM {{ ref('financial_reporting_monthly') }}
WHERE financial_area IN ('COLLECTIONS', 'COLLECTION_APPLICATIONS', 'PAYMENT_LINES_REQUIRING_REVIEW')
  AND ABS(COALESCE(pnl_signed_amount, 0)) > 0.01
