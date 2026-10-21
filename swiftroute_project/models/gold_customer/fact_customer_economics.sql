{{ config(materialized='view', schema='gold_customer') }}

/*
Gold fact: one row per canonical SwiftRoute customer per month.

The Silver Business model has already independently aggregated operational and
financial sources to this grain. This model does not re-join native-grain source
records; it exposes stable Gold keys, period attributes, and analyst-ready metrics.

Financial interpretation:
  - invoiced_amount and service-level billed amounts describe customer billing.
  - cash_received_amount belongs to the payment transaction month.
  - collections_applied_to_period_invoices_amount is attributed to the billing month
    of invoices linked to the payment, where the source link supports the attribution.
  - current_outstanding_balance_for_billing_period is the current balance of invoices
    in that billing period, not the historical month-end balance.
  - Cost, contribution, and margin are intentionally not calculated without a
    defensible customer-level cost-allocation method.
*/

WITH customer_period_source AS (
    SELECT *
    FROM {{ ref('silver_business_customer_period_integrated') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND period_month IS NOT NULL
),

fact_base AS (
    SELECT
        SHA2(client_id, 256) AS customer_key,
        SHA2(
            CONCAT(client_id, '||', DATE_FORMAT(period_month, 'yyyy-MM')),
            256
        ) AS customer_period_key,
        client_id,
        period_month,
        DATE_FORMAT(period_month, 'yyyy-MM') AS billing_period,
        YEAR(period_month) AS period_year,
        MONTH(period_month) AS period_number,

        has_fulfillment_activity,
        has_last_mile_activity,
        customer_service_mix,

        contract_candidate_count,
        has_contract_in_force_for_period,
        contract_match_is_ambiguous,
        applicable_contract_id,
        applicable_source_contract_id,
        contract_status_at_extract,
        contract_start_date,
        contract_end_date,
        contract_signed_date,
        applicable_contract_type,
        contracted_monthly_volume,
        contract_discount_rate,
        contract_net_payment_terms_days,
        contract_auto_renewal,
        contract_termination_notice_days,

        fulfillment_orders_received_count,
        fulfillment_units_ordered,
        fulfillment_orders_dispatched_count,
        fulfillment_pick_duration_observation_count,
        average_fulfillment_pick_duration_mins,
        fulfillment_stock_transaction_count,
        fulfillment_stock_dispatch_units,
        fulfillment_stock_receipt_units,
        fulfillment_stock_adjustment_net_quantity,

        last_mile_tasks_created_count,
        last_mile_delivery_tasks_created_count,
        last_mile_pickup_tasks_created_count,
        last_mile_completed_delivery_count,
        last_mile_successful_delivery_count,
        last_mile_failed_delivery_count,
        last_mile_completed_pickup_count,
        last_mile_completed_tasks_missing_completion_time_count,

        invoice_count,
        invoiced_amount,
        current_outstanding_balance_for_billing_period,
        invoice_payment_terms,
        next_day_invoiced_amount,
        same_day_invoiced_amount,
        returns_invoiced_amount,
        distribution_run_invoiced_amount,
        fulfillment_pick_invoiced_amount,
        storage_invoiced_amount,
        last_mile_invoiced_amount,
        fulfillment_invoiced_amount,
        unclassified_invoice_line_amount,

        payments_received_count,
        cash_received_amount,
        payments_applied_to_period_invoices_count,
        collections_applied_to_period_invoices_amount
    FROM customer_period_source
)

SELECT
    customer_period_key,
    customer_key,
    client_id,
    period_month,
    billing_period,
    period_year,
    period_number,

    has_fulfillment_activity,
    has_last_mile_activity,
    customer_service_mix,

    contract_candidate_count,
    has_contract_in_force_for_period,
    contract_match_is_ambiguous,
    applicable_contract_id,
    applicable_source_contract_id,
    contract_status_at_extract,
    contract_start_date,
    contract_end_date,
    contract_signed_date,
    applicable_contract_type,
    contracted_monthly_volume,
    contract_discount_rate,
    contract_net_payment_terms_days,
    contract_auto_renewal,
    contract_termination_notice_days,

    fulfillment_orders_received_count,
    fulfillment_units_ordered,
    fulfillment_orders_dispatched_count,
    fulfillment_pick_duration_observation_count,
    average_fulfillment_pick_duration_mins,
    fulfillment_stock_transaction_count,
    fulfillment_stock_dispatch_units,
    fulfillment_stock_receipt_units,
    fulfillment_stock_adjustment_net_quantity,

    last_mile_tasks_created_count,
    last_mile_delivery_tasks_created_count,
    last_mile_pickup_tasks_created_count,
    last_mile_completed_delivery_count,
    last_mile_successful_delivery_count,
    last_mile_failed_delivery_count,
    last_mile_completed_pickup_count,
    last_mile_completed_tasks_missing_completion_time_count,
    CASE
        WHEN last_mile_completed_delivery_count > 0
            THEN CAST(last_mile_successful_delivery_count AS DOUBLE)
                 / last_mile_completed_delivery_count
        ELSE NULL
    END AS last_mile_success_rate,
    CASE
        WHEN last_mile_completed_delivery_count > 0
            THEN CAST(last_mile_failed_delivery_count AS DOUBLE)
                 / last_mile_completed_delivery_count
        ELSE NULL
    END AS last_mile_failure_rate,

    invoice_count,
    invoiced_amount,
    current_outstanding_balance_for_billing_period,
    invoice_payment_terms,
    next_day_invoiced_amount,
    same_day_invoiced_amount,
    returns_invoiced_amount,
    distribution_run_invoiced_amount,
    fulfillment_pick_invoiced_amount,
    storage_invoiced_amount,
    last_mile_invoiced_amount,
    fulfillment_invoiced_amount,
    unclassified_invoice_line_amount,

    payments_received_count,
    cash_received_amount,
    payments_applied_to_period_invoices_count,
    collections_applied_to_period_invoices_amount,

    CASE WHEN invoice_count > 0 THEN TRUE ELSE FALSE END AS has_invoice_activity,
    CASE WHEN payments_received_count > 0 THEN TRUE ELSE FALSE END AS has_cash_receipt_activity,
    CASE
        WHEN payments_applied_to_period_invoices_count > 0 THEN TRUE ELSE FALSE
    END AS has_collections_applied_to_period_invoices
FROM fact_base
