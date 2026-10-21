-- Invoice-level source validation showed line totals reconcile to invoice totals.
-- At customer-month grain, service-classified lines plus unclassified lines must
-- therefore reconcile to the invoiced total (allowing one cent of decimal rounding).
SELECT
    client_id,
    period_month,
    invoiced_amount,
    COALESCE(last_mile_invoiced_amount, 0)
      + COALESCE(fulfillment_invoiced_amount, 0)
      + COALESCE(unclassified_invoice_line_amount, 0) AS classified_line_total
FROM {{ ref('fact_customer_economics') }}
WHERE ABS(
    COALESCE(invoiced_amount, 0)
    - (
        COALESCE(last_mile_invoiced_amount, 0)
        + COALESCE(fulfillment_invoiced_amount, 0)
        + COALESCE(unclassified_invoice_line_amount, 0)
      )
) > 0.02
