{{ config(materialized='view', schema='gold_finance') }}

/*
Purpose:
  Compare independently aggregated operational charge calculations with the
  actual QuickBooks invoice lines.

Grain:
  One row per canonical customer, calendar service month, and normalized service.

Matching convention:
  QuickBooks billing_period is a yyyy-MM value, so actual billing is matched to
  expected operational charges at customer + calendar service month + service.
  For weekly pay-as-you-go customers this is a monthly roll-up, not proof that a
  particular weekly invoice was issued on the correct cadence.
*/

WITH expected_by_service_month AS (
    SELECT
        client_id,
        service_month,
        service_type,
        MAX(billing_cadence) AS billing_cadence,
        SUM(source_activity_count) AS source_activity_count,
        SUM(candidate_quantity) AS candidate_quantity,
        SUM(billable_quantity) AS eligible_billable_quantity,
        SUM(calculated_net_charge) AS calculated_expected_charge_amount,
        SUM(invoice_ready_amount) AS invoice_ready_expected_charge_amount,
        SUM(CASE WHEN billing_line_status NOT IN ('READY_TO_BILL', 'NOT_COMPLETED') THEN 1 ELSE 0 END) AS review_line_count,
        SUM(CASE WHEN billing_line_status IN ('MISSING_APPLICABLE_RATE', 'NO_APPLICABLE_CONTRACT', 'AMBIGUOUS_CONTRACT', 'AMBIGUOUS_APPLICABLE_RATE', 'MISSING_RATE_AMOUNT') THEN 1 ELSE 0 END) AS pricing_exception_line_count,
        SUM(CASE WHEN billing_line_status IN ('REVIEW_PICKUP_CHARGEABILITY', 'REVIEW_FAILED_TASK_CHARGEABILITY', 'REVIEW_MISSING_COMPLETION_TIMESTAMP') THEN 1 ELSE 0 END) AS chargeability_review_line_count
    FROM {{ ref('billing_invoice_lines') }}
    WHERE client_id IS NOT NULL
      AND service_month IS NOT NULL
      AND service_type IS NOT NULL
    GROUP BY client_id, service_month, service_type
),

actual_invoice_lines AS (
    SELECT
        TRIM(i.client_id) AS client_id,
        TO_DATE(CONCAT(TRIM(i.billing_period), '-01')) AS billing_month,
        CASE
            WHEN LOWER(TRIM(COALESCE(
                NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
                NULLIF(TRIM(line.description), ''),
                line.detail_type
            ))) RLIKE 'next.?day' THEN 'next_day'
            WHEN LOWER(TRIM(COALESCE(
                NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
                NULLIF(TRIM(line.description), ''),
                line.detail_type
            ))) RLIKE 'same.?day' THEN 'same_day'
            WHEN LOWER(TRIM(COALESCE(
                NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
                NULLIF(TRIM(line.description), ''),
                line.detail_type
            ))) RLIKE 'return' THEN 'returns'
            WHEN LOWER(TRIM(COALESCE(
                NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
                NULLIF(TRIM(line.description), ''),
                line.detail_type
            ))) RLIKE 'distribution.?run' THEN 'distribution_run'
            WHEN LOWER(TRIM(COALESCE(
                NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
                NULLIF(TRIM(line.description), ''),
                line.detail_type
            ))) RLIKE 'fulfillment.*pick|pick.*pack' THEN 'fulfillment_pick'
            WHEN LOWER(TRIM(COALESCE(
                NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
                NULLIF(TRIM(line.description), ''),
                line.detail_type
            ))) RLIKE 'warehouse.*stor|storage' THEN 'storage'
            ELSE 'unclassified_revenue'
        END AS service_type,
        SUM(COALESCE(line.amount, 0)) AS actual_invoiced_amount,
        COUNT(DISTINCT i.invoice_id) AS actual_invoice_count
    FROM {{ ref('quickbooks_invoices_t') }} i
    LATERAL VIEW OUTER EXPLODE(i.lines) exploded_lines AS line
    WHERE i.client_id IS NOT NULL
      AND LOWER(TRIM(i.client_id)) NOT IN ('', 'n/a', 'null')
      AND TRIM(i.billing_period) RLIKE '^[0-9]{4}-[0-9]{2}$'
    GROUP BY
        TRIM(i.client_id),
        TO_DATE(CONCAT(TRIM(i.billing_period), '-01')),
        CASE
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'next.?day' THEN 'next_day'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'same.?day' THEN 'same_day'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'return' THEN 'returns'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'distribution.?run' THEN 'distribution_run'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'fulfillment.*pick|pick.*pack' THEN 'fulfillment_pick'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'warehouse.*stor|storage' THEN 'storage'
            ELSE 'unclassified_revenue'
        END
),

reconciliation_keys AS (
    SELECT client_id, service_month AS billing_month, service_type
    FROM expected_by_service_month
    UNION
    SELECT client_id, billing_month, service_type
    FROM actual_invoice_lines
),

combined AS (
    SELECT
        k.client_id,
        k.billing_month,
        k.service_type,
        e.billing_cadence,
        e.source_activity_count,
        e.candidate_quantity,
        e.eligible_billable_quantity,
        e.calculated_expected_charge_amount,
        e.invoice_ready_expected_charge_amount,
        e.review_line_count,
        e.pricing_exception_line_count,
        e.chargeability_review_line_count,
        a.actual_invoiced_amount,
        a.actual_invoice_count,
        CASE
            WHEN e.client_id IS NOT NULL OR a.client_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_reconciliation_evidence
    FROM reconciliation_keys k
    LEFT JOIN expected_by_service_month e
        ON k.client_id = e.client_id
       AND k.billing_month = e.service_month
       AND k.service_type = e.service_type
    LEFT JOIN actual_invoice_lines a
        ON k.client_id = a.client_id
       AND k.billing_month = a.billing_month
       AND k.service_type = a.service_type
)

SELECT
    SHA2(CONCAT_WS('||', client_id, DATE_FORMAT(billing_month, 'yyyy-MM'), service_type), 256) AS reconciliation_key,
    client_id,
    billing_month,
    DATE_FORMAT(billing_month, 'yyyy-MM') AS billing_period,
    service_type,
    COALESCE(billing_cadence, 'UNKNOWN') AS billing_cadence,
    COALESCE(source_activity_count, 0) AS source_activity_count,
    COALESCE(candidate_quantity, 0) AS candidate_quantity,
    COALESCE(eligible_billable_quantity, 0) AS eligible_billable_quantity,
    calculated_expected_charge_amount,
    invoice_ready_expected_charge_amount,
    COALESCE(actual_invoiced_amount, CAST(0 AS DECIMAL(18, 2))) AS actual_invoiced_amount,
    COALESCE(actual_invoice_count, 0) AS actual_invoice_count,
    COALESCE(review_line_count, 0) AS review_line_count,
    COALESCE(pricing_exception_line_count, 0) AS pricing_exception_line_count,
    COALESCE(chargeability_review_line_count, 0) AS chargeability_review_line_count,
    CAST(
        COALESCE(actual_invoiced_amount, 0) - COALESCE(invoice_ready_expected_charge_amount, 0)
        AS DECIMAL(18, 2)
    ) AS actual_minus_invoice_ready_variance,
    CASE
        WHEN COALESCE(review_line_count, 0) > 0
         AND COALESCE(invoice_ready_expected_charge_amount, 0) = 0
            THEN 'ACTIVITY_REQUIRES_REVIEW'
        WHEN COALESCE(billing_cadence, 'UNKNOWN') = 'WEEKLY'
            THEN 'WEEKLY_CADENCE_MONTHLY_ROLLUP_ONLY'
        WHEN invoice_ready_expected_charge_amount IS NULL
         AND actual_invoiced_amount IS NOT NULL
            THEN 'ACTUAL_INVOICE_WITHOUT_READY_BILLING_LINE'
        WHEN invoice_ready_expected_charge_amount IS NOT NULL
         AND actual_invoiced_amount IS NULL
            THEN 'READY_BILLING_WITHOUT_ACTUAL_INVOICE'
        WHEN ABS(COALESCE(actual_invoiced_amount, 0) - COALESCE(invoice_ready_expected_charge_amount, 0)) <= 0.01
         AND COALESCE(review_line_count, 0) = 0
            THEN 'MATCHED'
        ELSE 'VARIANCE_REQUIRES_REVIEW'
    END AS reconciliation_status,
    CASE
        WHEN COALESCE(billing_cadence, 'UNKNOWN') = 'WEEKLY'
            THEN 'Amounts are compared at customer/service/month grain because QuickBooks billing_period is monthly; weekly invoice timing cannot be verified.'
        WHEN COALESCE(review_line_count, 0) > 0
            THEN 'One or more operational activity lines require review; do not interpret the expected amount as a complete invoice-ready total.'
        WHEN invoice_ready_expected_charge_amount IS NULL AND actual_invoiced_amount IS NOT NULL
            THEN 'QuickBooks contains billed service lines with no corresponding invoice-ready operational charge.'
        WHEN invoice_ready_expected_charge_amount IS NOT NULL AND actual_invoiced_amount IS NULL
            THEN 'Operational activity produced an invoice-ready amount but no matching QuickBooks invoice line was found for this billing month/service.'
        WHEN ABS(COALESCE(actual_invoiced_amount, 0) - COALESCE(invoice_ready_expected_charge_amount, 0)) > 0.01
            THEN 'Calculated operational charges differ from QuickBooks invoice-line amounts.'
        ELSE 'Calculated billable lines match QuickBooks invoice-line amounts within one cent.'
    END AS reconciliation_explanation
FROM combined
