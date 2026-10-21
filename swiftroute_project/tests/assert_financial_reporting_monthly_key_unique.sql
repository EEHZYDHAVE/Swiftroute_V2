SELECT financial_monthly_key
FROM {{ ref('financial_reporting_monthly') }}
GROUP BY financial_monthly_key
HAVING COUNT(*) > 1
