SELECT *
FROM {{ ref('billing_invoice_lines') }}
WHERE billing_line_status = 'READY_TO_BILL'
  AND (
       client_id IS NULL
    OR billing_period_start IS NULL
    OR service_month IS NULL
    OR first_activity_date IS NULL
    OR last_activity_date IS NULL
    OR applicable_contract_id IS NULL
    OR applicable_contract_rate_id IS NULL
    OR rate_effective_date IS NULL
    OR rate_effective_date > first_activity_date
    OR contract_candidate_count <> 1
    OR billable_quantity <= 0
    OR calculated_net_rate IS NULL
    OR invoice_ready_amount IS NULL
    OR ABS(COALESCE(invoice_ready_amount, 0) - COALESCE(calculated_net_charge, 0)) > 0.01
    OR (source_net_rate IS NULL AND applicable_discount_rate IS NOT NULL AND (applicable_discount_rate < 0 OR applicable_discount_rate > 1))
  )
