{{ config(materialized='table') }}

/*
Purpose:
  Integrated customer-period business table for SwiftRoute.

Grain:
  One row per canonical client_id per calendar month (period_month).

Important modelling decisions:
  - Each source is aggregated to client-month before it is joined to the final spine.
  - Invoice totals, service-classified invoice lines, cash receipts, and collections
    applied to invoices are kept separate.
  - Invoice billing_period determines the invoice's billing month.
  - Cash receipts use the payment transaction month. Applied collections use the
    billing month of the linked invoice and only allocate payment lines with exactly
    one linked transaction, avoiding accidental multiplication.
  - Last-mile task creation uses task_created_at_utc; completion outcomes use
    completion_time_utc. These dates can legitimately place the two measures in
    different months.
  - Contract applicability is based on signed/start/end dates and excludes Draft
    contracts. A contract currently marked Expired may still have applied in a
    historical month. Ambiguous contract matches are flagged and their individual
    attributes are not arbitrarily selected.
  - Contract rates are not collapsed into a single customer-level rate because they
    vary by service, zone, unit, and effective date. No customer cost, contribution,
    or margin is invented without a defensible cost-allocation method.
*/

WITH accounts_ranked AS (
    SELECT
        TRIM(client_id) AS client_id,
        account_id,
        source_account_id,
        account_name,
        account_type,
        industry,
        account_tier,
        primary_city,
        owner_name AS account_owner_name,
        is_fulfillment_client AS is_fulfillment_client_configured,
        created_at_utc AS account_created_at_utc,
        source_updated_at_utc AS account_source_updated_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(client_id)
            ORDER BY source_updated_at_utc DESC NULLS LAST, account_id
        ) AS account_row_num
    FROM silver_t.salesforce_accounts_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
),

accounts AS (
    SELECT *
    FROM accounts_ranked
    WHERE account_row_num = 1
),

orders_source AS (
    SELECT
        order_id,
        NULLIF(TRIM(client_id), '') AS client_id,
        client_name,
        customer_name,
        received_at_utc,
        order_processed_at_utc,
        dispatched_at_utc,
        order_activity_at_utc,
        items,
        pick_duration_mins
    FROM silver_t.linnworks_orders_t
),

stock_items_source AS (
    SELECT
        stock_item_id,
        NULLIF(TRIM(client_id), '') AS client_id,
        client_name
    FROM silver_t.linnworks_stock_items_t
),

stock_transactions_attributed AS (
    SELECT
        st.stock_transaction_id,
        st.source_stock_transaction_id,
        st.transaction_type,
        st.quantity,
        st.transaction_at_utc,
        st.order_id,
        st.stock_item_id,
        CASE
            WHEN o.client_id IS NOT NULL
             AND LOWER(TRIM(o.client_id)) NOT IN ('', 'n/a', 'null')
                THEN TRIM(o.client_id)
            WHEN si.client_id IS NOT NULL
             AND LOWER(TRIM(si.client_id)) NOT IN ('', 'n/a', 'null')
                THEN TRIM(si.client_id)
            ELSE NULL
        END AS client_id
    FROM silver_t.linnworks_stock_transaction_t st
    LEFT JOIN orders_source o
        ON st.order_id = o.order_id
    LEFT JOIN stock_items_source si
        ON st.stock_item_id = si.stock_item_id
),

client_name_candidates AS (
    SELECT TRIM(client_id) AS client_id, account_name AS client_name, 1 AS source_priority
    FROM silver_t.salesforce_accounts_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND NULLIF(TRIM(account_name), '') IS NOT NULL

    UNION ALL

    SELECT TRIM(client_id), customer_name, 2
    FROM silver_t.quickbooks_invoices_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND NULLIF(TRIM(customer_name), '') IS NOT NULL

    UNION ALL

    SELECT TRIM(client_id), customer_name, 3
    FROM silver_t.quickbooks_payments_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND NULLIF(TRIM(customer_name), '') IS NOT NULL

    UNION ALL

    SELECT TRIM(client_id), COALESCE(client_name, customer_name), 4
    FROM orders_source
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND NULLIF(TRIM(COALESCE(client_name, customer_name)), '') IS NOT NULL

    UNION ALL

    SELECT TRIM(client_id), client_name, 5
    FROM stock_items_source
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND NULLIF(TRIM(client_name), '') IS NOT NULL
),

client_names_ranked AS (
    SELECT
        client_id,
        client_name,
        ROW_NUMBER() OVER (
            PARTITION BY client_id
            ORDER BY source_priority, client_name
        ) AS name_row_num
    FROM client_name_candidates
),

client_names AS (
    SELECT client_id, client_name
    FROM client_names_ranked
    WHERE name_row_num = 1
),

source_months AS (
    SELECT TO_DATE(CONCAT(TRIM(billing_period), '-01')) AS period_month
    FROM silver_t.quickbooks_invoices_t
    WHERE TRIM(billing_period) RLIKE '^[0-9]{4}-[0-9]{2}$'

    UNION ALL

    SELECT TRUNC(CAST(transaction_date AS DATE), 'MM')
    FROM silver_t.quickbooks_payments_t
    WHERE transaction_date IS NOT NULL

    UNION ALL

    SELECT TRUNC(CAST(received_at_utc AS DATE), 'MM')
    FROM orders_source
    WHERE received_at_utc IS NOT NULL

    UNION ALL

    SELECT TRUNC(CAST(dispatched_at_utc AS DATE), 'MM')
    FROM orders_source
    WHERE dispatched_at_utc IS NOT NULL

    UNION ALL

    SELECT TRUNC(CAST(order_activity_at_utc AS DATE), 'MM')
    FROM orders_source
    WHERE order_activity_at_utc IS NOT NULL

    UNION ALL

    SELECT TRUNC(CAST(order_processed_at_utc AS DATE), 'MM')
    FROM orders_source
    WHERE order_processed_at_utc IS NOT NULL

    UNION ALL

    SELECT TRUNC(CAST(transaction_at_utc AS DATE), 'MM')
    FROM silver_t.linnworks_stock_transaction_t
    WHERE transaction_at_utc IS NOT NULL

    UNION ALL

    SELECT TRUNC(CAST(task_created_at_utc AS DATE), 'MM')
    FROM silver_t.onfleet_deliveries_t
    WHERE task_created_at_utc IS NOT NULL

    UNION ALL

    SELECT TRUNC(CAST(completion_time_utc AS DATE), 'MM')
    FROM silver_t.onfleet_deliveries_t
    WHERE completion_time_utc IS NOT NULL
),

period_bounds AS (
    SELECT
        MIN(period_month) AS min_period_month,
        MAX(period_month) AS max_period_month
    FROM source_months
    WHERE period_month IS NOT NULL
),

period_spine AS (
    SELECT EXPLODE(
        SEQUENCE(min_period_month, max_period_month, INTERVAL 1 MONTH)
    ) AS period_month
    FROM period_bounds
    WHERE min_period_month IS NOT NULL
      AND max_period_month IS NOT NULL
),

raw_client_ids AS (
    SELECT client_id FROM silver_t.salesforce_accounts_t
    UNION ALL SELECT client_id FROM silver_t.quickbooks_invoices_t
    UNION ALL SELECT client_id FROM silver_t.quickbooks_payments_t
    UNION ALL SELECT client_id FROM orders_source
    UNION ALL SELECT client_id FROM silver_t.onfleet_deliveries_t
    UNION ALL SELECT client_id FROM stock_items_source
    UNION ALL SELECT client_id FROM stock_transactions_attributed
    UNION ALL SELECT client_id FROM silver_t.salesforce_contracts_t
),

customer_ids AS (
    SELECT DISTINCT TRIM(client_id) AS client_id
    FROM raw_client_ids
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
),

customer_periods AS (
    SELECT
        c.client_id,
        p.period_month
    FROM customer_ids c
    CROSS JOIN period_spine p
),

customer_context AS (
    SELECT
        cp.client_id,
        cp.period_month,
        COALESCE(cn.client_name, 'N/A') AS client_name,
        CASE WHEN a.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_salesforce_account,
        a.account_type,
        a.industry,
        a.account_tier,
        a.primary_city,
        a.account_owner_name,
        a.is_fulfillment_client_configured
    FROM customer_periods cp
    LEFT JOIN accounts a
        ON cp.client_id = a.client_id
    LEFT JOIN client_names cn
        ON cp.client_id = cn.client_id
),

invoice_source AS (
    SELECT
        invoice_id,
        TRIM(client_id) AS client_id,
        customer_name,
        TO_DATE(CONCAT(TRIM(billing_period), '-01')) AS period_month,
        payment_terms,
        total_amount,
        balance_amount,
        lines
    FROM silver_t.quickbooks_invoices_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND TRIM(billing_period) RLIKE '^[0-9]{4}-[0-9]{2}$'
),

invoice_monthly AS (
    SELECT
        client_id,
        period_month,
        COUNT(DISTINCT invoice_id) AS invoice_count,
        SUM(COALESCE(total_amount, 0)) AS invoiced_amount,
        SUM(COALESCE(balance_amount, 0)) AS current_outstanding_balance_for_billing_period,
        MAX(payment_terms) AS invoice_payment_terms
    FROM invoice_source
    GROUP BY client_id, period_month
),

invoice_line_service_mapping AS (
    SELECT
        i.invoice_id,
        i.client_id,
        i.period_month,
        line.amount AS line_amount,
        LOWER(TRIM(COALESCE(
            NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
            NULLIF(TRIM(line.description), ''),
            line.detail_type
        ))) AS invoice_line_label,
        CASE LOWER(TRIM(COALESCE(
            NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
            NULLIF(TRIM(line.description), ''),
            line.detail_type
        )))
            WHEN 'next-day delivery' THEN 'next_day'
            WHEN 'same-day delivery' THEN 'same_day'
            WHEN 'returns handling' THEN 'returns'
            WHEN 'b2b distribution run' THEN 'distribution_run'
            WHEN 'fulfillment — pick & pack' THEN 'fulfillment_pick'
            WHEN 'fulfillment - pick & pack' THEN 'fulfillment_pick'
            WHEN 'warehouse storage' THEN 'storage'
            ELSE NULL
        END AS service_type
    FROM invoice_source i
    LATERAL VIEW OUTER EXPLODE(i.lines) exploded AS line
),

invoice_lines_monthly AS (
    SELECT
        client_id,
        period_month,
        SUM(CASE WHEN service_type = 'next_day' THEN COALESCE(line_amount, 0) ELSE 0 END)
            AS next_day_invoiced_amount,
        SUM(CASE WHEN service_type = 'same_day' THEN COALESCE(line_amount, 0) ELSE 0 END)
            AS same_day_invoiced_amount,
        SUM(CASE WHEN service_type = 'returns' THEN COALESCE(line_amount, 0) ELSE 0 END)
            AS returns_invoiced_amount,
        SUM(CASE WHEN service_type = 'distribution_run' THEN COALESCE(line_amount, 0) ELSE 0 END)
            AS distribution_run_invoiced_amount,
        SUM(CASE WHEN service_type = 'fulfillment_pick' THEN COALESCE(line_amount, 0) ELSE 0 END)
            AS fulfillment_pick_invoiced_amount,
        SUM(CASE WHEN service_type = 'storage' THEN COALESCE(line_amount, 0) ELSE 0 END)
            AS storage_invoiced_amount,
        SUM(CASE WHEN service_type IS NULL THEN COALESCE(line_amount, 0) ELSE 0 END)
            AS unclassified_invoice_line_amount
    FROM invoice_line_service_mapping
    GROUP BY client_id, period_month
),

payments_cash_monthly AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(transaction_date AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT payment_id) AS payments_received_count,
        SUM(COALESCE(total_amount, 0)) AS cash_received_amount
    FROM silver_t.quickbooks_payments_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND transaction_date IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(transaction_date AS DATE), 'MM')
),

payment_invoice_links AS (
    SELECT
        p.payment_id,
        TRIM(p.client_id) AS payment_client_id,
        line.amount AS applied_amount,
        linked.transaction_id AS linked_transaction_id,
        LOWER(TRIM(linked.transaction_type)) AS linked_transaction_type,
        COALESCE(SIZE(line.linked_transactions), 0) AS linked_transaction_count
    FROM silver_t.quickbooks_payments_t p
    LATERAL VIEW OUTER EXPLODE(p.lines) payment_line_exploded AS line
    LATERAL VIEW OUTER EXPLODE(line.linked_transactions) linked_tx_exploded AS linked
    WHERE p.client_id IS NOT NULL
      AND LOWER(TRIM(p.client_id)) NOT IN ('', 'n/a', 'null')
),

collections_applied_monthly AS (
    SELECT
        TRIM(i.client_id) AS client_id,
        i.period_month AS period_month,
        COUNT(DISTINCT pl.payment_id) AS payments_applied_to_period_invoices_count,
        SUM(COALESCE(pl.applied_amount, 0)) AS collections_applied_to_period_invoices_amount
    FROM payment_invoice_links pl
    INNER JOIN invoice_source i
        ON pl.linked_transaction_id = i.invoice_id
    WHERE pl.linked_transaction_type = 'invoice'
      AND pl.linked_transaction_count = 1
      AND pl.applied_amount IS NOT NULL
    GROUP BY TRIM(i.client_id), i.period_month
),

fulfillment_orders_received_monthly AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(received_at_utc AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT order_id) AS fulfillment_orders_received_count,
        SUM(
            COALESCE(
                AGGREGATE(
                    items,
                    CAST(0 AS BIGINT),
                    (acc, item) -> acc + COALESCE(item.quantity, CAST(0 AS BIGINT))
                ),
                CAST(0 AS BIGINT)
            )
        ) AS fulfillment_units_ordered
    FROM orders_source
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND received_at_utc IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(received_at_utc AS DATE), 'MM')
),

fulfillment_orders_dispatched_monthly AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(dispatched_at_utc AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT order_id) AS fulfillment_orders_dispatched_count
    FROM orders_source
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND dispatched_at_utc IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(dispatched_at_utc AS DATE), 'MM')
),

fulfillment_pick_duration_monthly AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(order_processed_at_utc AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT CASE WHEN pick_duration_mins IS NOT NULL THEN order_id END)
            AS fulfillment_pick_duration_observation_count,
        AVG(CAST(pick_duration_mins AS DOUBLE)) AS average_fulfillment_pick_duration_mins
    FROM orders_source
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND order_processed_at_utc IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(order_processed_at_utc AS DATE), 'MM')
),

fulfillment_stock_monthly AS (
    SELECT
        client_id,
        TRUNC(CAST(transaction_at_utc AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT stock_transaction_id) AS fulfillment_stock_transaction_count,
        SUM(CASE WHEN UPPER(TRIM(transaction_type)) = 'DISPATCH'
                 THEN ABS(COALESCE(quantity, 0)) ELSE 0 END)
            AS fulfillment_stock_dispatch_units,
        SUM(CASE WHEN UPPER(TRIM(transaction_type)) = 'RECEIPT'
                 THEN ABS(COALESCE(quantity, 0)) ELSE 0 END)
            AS fulfillment_stock_receipt_units,
        SUM(CASE WHEN UPPER(TRIM(transaction_type)) = 'ADJUSTMENT'
                 THEN COALESCE(quantity, 0) ELSE 0 END)
            AS fulfillment_stock_adjustment_net_quantity
    FROM stock_transactions_attributed
    WHERE client_id IS NOT NULL
      AND transaction_at_utc IS NOT NULL
    GROUP BY client_id, TRUNC(CAST(transaction_at_utc AS DATE), 'MM')
),

delivery_tasks_created_monthly AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(task_created_at_utc AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT task_id) AS last_mile_tasks_created_count,
        COUNT(DISTINCT CASE WHEN COALESCE(pickup_task, FALSE) = FALSE THEN task_id END)
            AS last_mile_delivery_tasks_created_count,
        COUNT(DISTINCT CASE WHEN pickup_task = TRUE THEN task_id END)
            AS last_mile_pickup_tasks_created_count
    FROM silver_t.onfleet_deliveries_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND task_created_at_utc IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(task_created_at_utc AS DATE), 'MM')
),

delivery_outcomes_monthly AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(completion_time_utc AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT CASE WHEN COALESCE(pickup_task, FALSE) = FALSE THEN task_id END)
            AS last_mile_completed_delivery_count,
        COUNT(DISTINCT CASE
            WHEN COALESCE(pickup_task, FALSE) = FALSE
             AND LOWER(TRIM(completion_result)) = 'success'
            THEN task_id END
        ) AS last_mile_successful_delivery_count,
        COUNT(DISTINCT CASE
            WHEN COALESCE(pickup_task, FALSE) = FALSE
             AND LOWER(TRIM(completion_result)) = 'failure'
            THEN task_id END
        ) AS last_mile_failed_delivery_count,
        COUNT(DISTINCT CASE WHEN pickup_task = TRUE THEN task_id END)
            AS last_mile_completed_pickup_count
    FROM silver_t.onfleet_deliveries_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND task_state = 3
      AND completion_time_utc IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(completion_time_utc AS DATE), 'MM')
),

delivery_completed_missing_timestamp_monthly AS (
    SELECT
        TRIM(client_id) AS client_id,
        TRUNC(CAST(task_created_at_utc AS DATE), 'MM') AS period_month,
        COUNT(DISTINCT task_id) AS last_mile_completed_tasks_missing_completion_time_count
    FROM silver_t.onfleet_deliveries_t
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND task_state = 3
      AND completion_time_utc IS NULL
      AND task_created_at_utc IS NOT NULL
    GROUP BY TRIM(client_id), TRUNC(CAST(task_created_at_utc AS DATE), 'MM')
),

contract_period_candidates AS (
    SELECT
        cp.client_id,
        cp.period_month,
        c.contract_id,
        c.source_contract_id,
        c.contract_status,
        c.start_date,
        c.end_date,
        c.signed_date,
        c.contract_type,
        c.committed_monthly_volume,
        c.discount_rate,
        c.net_payment_terms,
        c.auto_renewal,
        c.termination_notice_days,
        COUNT(*) OVER (
            PARTITION BY cp.client_id, cp.period_month
        ) AS contract_candidate_count,
        ROW_NUMBER() OVER (
            PARTITION BY cp.client_id, cp.period_month
            ORDER BY c.start_date DESC NULLS LAST, c.signed_date DESC NULLS LAST,
                     c.contract_id
        ) AS contract_row_num
    FROM customer_periods cp
    INNER JOIN silver_t.salesforce_contracts_t c
        ON TRIM(c.client_id) = cp.client_id
       AND c.start_date <= LAST_DAY(cp.period_month)
       AND (c.end_date IS NULL OR c.end_date >= cp.period_month)
       AND (c.signed_date IS NULL OR c.signed_date <= LAST_DAY(cp.period_month))
       AND UPPER(TRIM(c.contract_status)) IN ('ACTIVATED', 'EXPIRED')
    WHERE c.client_id IS NOT NULL
      AND LOWER(TRIM(c.client_id)) NOT IN ('', 'n/a', 'null')
),

contract_period_selected AS (
    SELECT
        client_id,
        period_month,
        contract_candidate_count,
        CASE WHEN contract_candidate_count = 1 THEN contract_id END AS applicable_contract_id,
        CASE WHEN contract_candidate_count = 1 THEN source_contract_id END AS applicable_source_contract_id,
        CASE WHEN contract_candidate_count = 1 THEN contract_status END AS contract_status_at_extract,
        CASE WHEN contract_candidate_count = 1 THEN start_date END AS contract_start_date,
        CASE WHEN contract_candidate_count = 1 THEN end_date END AS contract_end_date,
        CASE WHEN contract_candidate_count = 1 THEN signed_date END AS contract_signed_date,
        CASE WHEN contract_candidate_count = 1 THEN contract_type END AS applicable_contract_type,
        CASE WHEN contract_candidate_count = 1 THEN committed_monthly_volume END AS contracted_monthly_volume,
        CASE WHEN contract_candidate_count = 1 THEN discount_rate END AS contract_discount_rate,
        CASE WHEN contract_candidate_count = 1 THEN net_payment_terms END AS contract_net_payment_terms_days,
        CASE WHEN contract_candidate_count = 1 THEN auto_renewal END AS contract_auto_renewal,
        CASE WHEN contract_candidate_count = 1 THEN termination_notice_days END AS contract_termination_notice_days
    FROM contract_period_candidates
    WHERE contract_row_num = 1
),

assembled AS (
    SELECT
        cc.client_id,
        cc.period_month,
        cc.client_name,
        cc.has_salesforce_account,
        cc.account_type,
        cc.industry,
        cc.account_tier,
        cc.primary_city,
        cc.account_owner_name,
        cc.is_fulfillment_client_configured,

        COALESCE(cps.contract_candidate_count, 0) AS contract_candidate_count,
        CASE
            WHEN cps.contract_candidate_count = 1 THEN TRUE
            WHEN cps.contract_candidate_count > 1 THEN NULL
            ELSE FALSE
        END AS has_contract_in_force_for_period,
        CASE WHEN COALESCE(cps.contract_candidate_count, 0) > 1 THEN TRUE ELSE FALSE END
            AS contract_match_is_ambiguous,
        cps.applicable_contract_id,
        cps.applicable_source_contract_id,
        cps.contract_status_at_extract,
        cps.contract_start_date,
        cps.contract_end_date,
        cps.contract_signed_date,
        cps.applicable_contract_type,
        cps.contracted_monthly_volume,
        cps.contract_discount_rate,
        cps.contract_net_payment_terms_days,
        cps.contract_auto_renewal,
        cps.contract_termination_notice_days,

        COALESCE(fo.fulfillment_orders_received_count, 0) AS fulfillment_orders_received_count,
        COALESCE(fo.fulfillment_units_ordered, 0) AS fulfillment_units_ordered,
        COALESCE(fd.fulfillment_orders_dispatched_count, 0) AS fulfillment_orders_dispatched_count,
        COALESCE(fp.fulfillment_pick_duration_observation_count, 0)
            AS fulfillment_pick_duration_observation_count,
        fp.average_fulfillment_pick_duration_mins,
        COALESCE(fs.fulfillment_stock_transaction_count, 0)
            AS fulfillment_stock_transaction_count,
        COALESCE(fs.fulfillment_stock_dispatch_units, 0) AS fulfillment_stock_dispatch_units,
        COALESCE(fs.fulfillment_stock_receipt_units, 0) AS fulfillment_stock_receipt_units,
        COALESCE(fs.fulfillment_stock_adjustment_net_quantity, 0)
            AS fulfillment_stock_adjustment_net_quantity,

        COALESCE(dt.last_mile_tasks_created_count, 0) AS last_mile_tasks_created_count,
        COALESCE(dt.last_mile_delivery_tasks_created_count, 0)
            AS last_mile_delivery_tasks_created_count,
        COALESCE(dt.last_mile_pickup_tasks_created_count, 0)
            AS last_mile_pickup_tasks_created_count,
        COALESCE(dout.last_mile_completed_delivery_count, 0)
            AS last_mile_completed_delivery_count,
        COALESCE(dout.last_mile_successful_delivery_count, 0)
            AS last_mile_successful_delivery_count,
        COALESCE(dout.last_mile_failed_delivery_count, 0)
            AS last_mile_failed_delivery_count,
        COALESCE(dout.last_mile_completed_pickup_count, 0)
            AS last_mile_completed_pickup_count,
        COALESCE(dm.last_mile_completed_tasks_missing_completion_time_count, 0)
            AS last_mile_completed_tasks_missing_completion_time_count,

        COALESCE(im.invoice_count, 0) AS invoice_count,
        COALESCE(im.invoiced_amount, 0) AS invoiced_amount,
        COALESCE(im.current_outstanding_balance_for_billing_period, 0)
            AS current_outstanding_balance_for_billing_period,
        im.invoice_payment_terms,
        COALESCE(il.next_day_invoiced_amount, 0) AS next_day_invoiced_amount,
        COALESCE(il.same_day_invoiced_amount, 0) AS same_day_invoiced_amount,
        COALESCE(il.returns_invoiced_amount, 0) AS returns_invoiced_amount,
        COALESCE(il.distribution_run_invoiced_amount, 0) AS distribution_run_invoiced_amount,
        COALESCE(il.fulfillment_pick_invoiced_amount, 0) AS fulfillment_pick_invoiced_amount,
        COALESCE(il.storage_invoiced_amount, 0) AS storage_invoiced_amount,
        COALESCE(il.unclassified_invoice_line_amount, 0) AS unclassified_invoice_line_amount,

        COALESCE(pc.payments_received_count, 0) AS payments_received_count,
        COALESCE(pc.cash_received_amount, 0) AS cash_received_amount,
        COALESCE(ca.payments_applied_to_period_invoices_count, 0)
            AS payments_applied_to_period_invoices_count,
        COALESCE(ca.collections_applied_to_period_invoices_amount, 0)
            AS collections_applied_to_period_invoices_amount
    FROM customer_context cc
    LEFT JOIN contract_period_selected cps
        ON cc.client_id = cps.client_id
       AND cc.period_month = cps.period_month
    LEFT JOIN fulfillment_orders_received_monthly fo
        ON cc.client_id = fo.client_id AND cc.period_month = fo.period_month
    LEFT JOIN fulfillment_orders_dispatched_monthly fd
        ON cc.client_id = fd.client_id AND cc.period_month = fd.period_month
    LEFT JOIN fulfillment_pick_duration_monthly fp
        ON cc.client_id = fp.client_id AND cc.period_month = fp.period_month
    LEFT JOIN fulfillment_stock_monthly fs
        ON cc.client_id = fs.client_id AND cc.period_month = fs.period_month
    LEFT JOIN delivery_tasks_created_monthly dt
        ON cc.client_id = dt.client_id AND cc.period_month = dt.period_month
    LEFT JOIN delivery_outcomes_monthly dout
        ON cc.client_id = dout.client_id AND cc.period_month = dout.period_month
    LEFT JOIN delivery_completed_missing_timestamp_monthly dm
        ON cc.client_id = dm.client_id AND cc.period_month = dm.period_month
    LEFT JOIN invoice_monthly im
        ON cc.client_id = im.client_id AND cc.period_month = im.period_month
    LEFT JOIN invoice_lines_monthly il
        ON cc.client_id = il.client_id AND cc.period_month = il.period_month
    LEFT JOIN payments_cash_monthly pc
        ON cc.client_id = pc.client_id AND cc.period_month = pc.period_month
    LEFT JOIN collections_applied_monthly ca
        ON cc.client_id = ca.client_id AND cc.period_month = ca.period_month
)

SELECT
    client_id,
    period_month,
    client_name,
    has_salesforce_account,
    account_type,
    industry,
    account_tier,
    primary_city,
    account_owner_name,
    is_fulfillment_client_configured,

    CASE
        WHEN fulfillment_orders_received_count > 0
          OR fulfillment_orders_dispatched_count > 0
          OR fulfillment_stock_transaction_count > 0
          OR fulfillment_pick_duration_observation_count > 0
        THEN TRUE ELSE FALSE
    END AS has_fulfillment_activity,
    CASE
        WHEN last_mile_tasks_created_count > 0
          OR last_mile_completed_delivery_count > 0
          OR last_mile_completed_pickup_count > 0
        THEN TRUE ELSE FALSE
    END AS has_last_mile_activity,
    CASE
        WHEN (fulfillment_orders_received_count > 0
           OR fulfillment_orders_dispatched_count > 0
           OR fulfillment_stock_transaction_count > 0
           OR fulfillment_pick_duration_observation_count > 0)
         AND (last_mile_tasks_created_count > 0
           OR last_mile_completed_delivery_count > 0
           OR last_mile_completed_pickup_count > 0)
            THEN 'fulfillment_and_last_mile'
        WHEN fulfillment_orders_received_count > 0
          OR fulfillment_orders_dispatched_count > 0
          OR fulfillment_stock_transaction_count > 0
          OR fulfillment_pick_duration_observation_count > 0
            THEN 'fulfillment_only'
        WHEN last_mile_tasks_created_count > 0
          OR last_mile_completed_delivery_count > 0
          OR last_mile_completed_pickup_count > 0
            THEN 'last_mile_only'
        ELSE 'no_operational_activity'
    END AS customer_service_mix,

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
    (next_day_invoiced_amount + same_day_invoiced_amount
      + returns_invoiced_amount + distribution_run_invoiced_amount)
        AS last_mile_invoiced_amount,
    (fulfillment_pick_invoiced_amount + storage_invoiced_amount)
        AS fulfillment_invoiced_amount,
    unclassified_invoice_line_amount,

    payments_received_count,
    cash_received_amount,
    payments_applied_to_period_invoices_count,
    collections_applied_to_period_invoices_amount,

    CURRENT_TIMESTAMP() AS processed_at_utc
FROM assembled
