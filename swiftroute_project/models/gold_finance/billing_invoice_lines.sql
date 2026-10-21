{{ config(materialized='view', schema='gold_finance') }}

/*
Purpose:
  Calculate reviewable customer charges from operational activity and the
  Salesforce contractual rate valid on the activity date.

Grain:
  One row per customer, service month, billing window, service, zone, selected
  contract/rate, source quantity basis, and billing review status.

Important safeguards:
  - Successful non-pickup Onfleet delivery tasks with a completion timestamp are
    billable candidates. Failed tasks, pickups, incomplete tasks, and completed
    tasks with no completion timestamp are surfaced for review, not automatically
    charged.
  - Fulfillment quantities come from matched Linnworks DISPATCH stock transactions.
    Dispatched orders without a matching dispatch transaction are surfaced as
    review-only quantity proxies from order items.
  - Rates are selected by client, contract, service type, unit, zone preference,
    and latest effective_date that is not after the activity date.
  - A source's net_rate is used directly when present. A discount is applied to
    base_rate only when net_rate is absent; the discount is never applied twice.
  - Fixed fees/overages and historic storage quantities are not inferred because
    the supplied technical inputs do not establish those calculations reliably.
*/

WITH account_ranked AS (
    SELECT
        TRIM(client_id) AS client_id,
        account_id,
        account_name,
        account_type,
        contract_type AS account_contract_type,
        industry,
        account_tier,
        primary_city,
        net_payment_terms,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(client_id)
            ORDER BY source_updated_at_utc DESC NULLS LAST, account_id
        ) AS row_num
    FROM {{ ref('salesforce_accounts_t') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
),

accounts AS (
    SELECT *
    FROM account_ranked
    WHERE row_num = 1
),

contracts AS (
    SELECT DISTINCT
        contract_id,
        source_contract_id,
        TRIM(client_id) AS client_id,
        contract_status,
        start_date,
        end_date,
        signed_date,
        contract_type,
        discount_rate AS contract_discount_rate,
        net_payment_terms,
        committed_monthly_volume
    FROM {{ ref('salesforce_contracts_t') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND UPPER(TRIM(contract_status)) IN ('ACTIVATED', 'EXPIRED')
      AND COALESCE(is_orphaned_contract, FALSE) = FALSE
      AND COALESCE(account_reference_unmatched, FALSE) = FALSE
),

rates AS (
    SELECT
        contract_rate_id,
        TRIM(client_id) AS client_id,
        contract_id,
        service_type,
        zone_id,
        is_zone_specific,
        base_rate,
        discount_rate,
        net_rate,
        rate_unit,
        effective_date
    FROM {{ ref('salesforce_contract_rates_t') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
      AND COALESCE(client_id_mismatch, FALSE) = FALSE
      AND COALESCE(contract_account_mismatch, FALSE) = FALSE
      AND COALESCE(contract_reference_unmatched, FALSE) = FALSE
      AND COALESCE(account_reference_unmatched, FALSE) = FALSE
),

stock_items_ranked AS (
    SELECT
        stock_item_id,
        NULLIF(TRIM(client_id), '') AS client_id,
        ROW_NUMBER() OVER (
            PARTITION BY stock_item_id
            ORDER BY source_updated_at_utc DESC NULLS LAST,
                     processed_at_utc DESC NULLS LAST,
                     stock_item_id
        ) AS row_num
    FROM {{ ref('linnworks_stock_items_t') }}
),

stock_items AS (
    SELECT stock_item_id, client_id
    FROM stock_items_ranked
    WHERE row_num = 1
),

orders_raw AS (
    SELECT
        o.order_id,
        NULLIF(TRIM(o.client_id), '') AS client_id,
        o.dispatched_at_utc,
        o.received_at_utc,
        o.order_processed_at_utc,
        o.order_activity_at_utc,
        item.quantity AS item_quantity,
        item.item_title,
        COALESCE(item.is_composite, FALSE) AS is_composite
    FROM {{ ref('linnworks_orders_t') }} o
    LATERAL VIEW OUTER EXPLODE(o.items) exploded_items AS item
),

order_summary AS (
    SELECT
        order_id,
        MAX(client_id) AS client_id,
        MAX(dispatched_at_utc) AS dispatched_at_utc,
        MAX(received_at_utc) AS received_at_utc,
        MAX(order_processed_at_utc) AS order_processed_at_utc,
        MAX(order_activity_at_utc) AS order_activity_at_utc,
        SUM(COALESCE(item_quantity, 0)) AS order_item_units,
        SUM(CASE WHEN item_title IS NOT NULL THEN 1 ELSE 0 END) AS order_item_line_count,
        SUM(CASE WHEN is_composite THEN 1 ELSE 0 END) AS composite_item_line_count
    FROM orders_raw
    GROUP BY order_id
),

stock_dispatch_source AS (
    SELECT
        st.stock_transaction_id,
        st.source_stock_transaction_id,
        st.order_id,
        st.stock_item_id,
        st.transaction_type,
        st.quantity,
        st.transaction_at_utc,
        st.is_deleted_order_reference,
        CASE
            WHEN o.client_id IS NOT NULL
             AND LOWER(TRIM(o.client_id)) NOT IN ('', 'n/a', 'null')
                THEN TRIM(o.client_id)
            WHEN si.client_id IS NOT NULL
             AND LOWER(TRIM(si.client_id)) NOT IN ('', 'n/a', 'null')
                THEN TRIM(si.client_id)
            ELSE NULL
        END AS client_id,
        CASE WHEN o.order_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_matched_order
    FROM {{ ref('linnworks_stock_transaction_t') }} st
    LEFT JOIN order_summary o
        ON st.order_id = o.order_id
    LEFT JOIN stock_items si
        ON st.stock_item_id = si.stock_item_id
    WHERE UPPER(TRIM(st.transaction_type)) = 'DISPATCH'
),

onfleet_source AS (
    SELECT
        CONCAT('ONFLEET::', COALESCE(task_id, source_task_id, SHA2(CONCAT_WS('||', COALESCE(task_created_at_utc, CAST('1900-01-01' AS TIMESTAMP)), COALESCE(client_id, 'N/A')), 256))) AS activity_id,
        'onfleet' AS activity_source,
        COALESCE(NULLIF(TRIM(client_id), ''), NULL) AS client_id,
        CASE
            WHEN LOWER(TRIM(service_type)) RLIKE 'next.?day' THEN 'next_day'
            WHEN LOWER(TRIM(service_type)) RLIKE 'same.?day' THEN 'same_day'
            WHEN LOWER(TRIM(service_type)) RLIKE 'return' THEN 'returns'
            WHEN LOWER(TRIM(service_type)) RLIKE 'distribution.?run' THEN 'distribution_run'
            ELSE NULL
        END AS service_type,
        NULLIF(TRIM(zone_id), '') AS zone_id,
        CASE
            WHEN task_state = 3 AND completion_time_utc IS NOT NULL
                THEN CAST(completion_time_utc AS DATE)
            WHEN task_state = 3 AND completion_time_utc IS NULL
                THEN CAST(task_created_at_utc AS DATE)
            ELSE CAST(task_created_at_utc AS DATE)
        END AS activity_date,
        'per_delivery' AS expected_rate_unit,
        'completed_non_pickup_task_count' AS quantity_basis,
        CAST(1 AS DECIMAL(18, 4)) AS candidate_quantity,
        CASE
            WHEN COALESCE(pickup_task, FALSE) = FALSE
             AND task_state = 3
             AND LOWER(TRIM(completion_result)) = 'success'
             AND completion_time_utc IS NOT NULL
                THEN CAST(1 AS DECIMAL(18, 4))
            ELSE CAST(0 AS DECIMAL(18, 4))
        END AS billable_quantity,
        CASE
            WHEN client_id IS NULL OR LOWER(TRIM(client_id)) IN ('', 'n/a', 'null')
                THEN 'MISSING_CUSTOMER_MAPPING'
            WHEN COALESCE(pickup_task, FALSE) = TRUE
                THEN 'REVIEW_PICKUP_CHARGEABILITY'
            WHEN task_state = 3
             AND LOWER(TRIM(completion_result)) = 'success'
             AND completion_time_utc IS NULL
                THEN 'REVIEW_MISSING_COMPLETION_TIMESTAMP'
            WHEN task_state = 3
             AND LOWER(TRIM(completion_result)) = 'success'
             AND completion_time_utc IS NOT NULL
                THEN 'ELIGIBLE'
            WHEN task_state = 3 AND LOWER(TRIM(completion_result)) = 'failure'
                THEN 'REVIEW_FAILED_TASK_CHARGEABILITY'
            WHEN task_state = 3
                THEN 'REVIEW_MISSING_COMPLETION_RESULT'
            ELSE 'NOT_COMPLETED'
        END AS activity_eligibility_status,
        CASE
            WHEN task_state = 3 AND completion_time_utc IS NOT NULL THEN 'completion_time_utc'
            WHEN task_created_at_utc IS NOT NULL THEN 'task_created_at_utc_fallback'
            ELSE 'missing_service_date'
        END AS service_date_basis,
        COALESCE(task_id, source_task_id) AS source_activity_id,
        CAST(NULL AS STRING) AS source_order_id,
        CAST(NULL AS BIGINT) AS source_stock_quantity
    FROM {{ ref('onfleet_deliveries_t') }}
),

stock_dispatch_activity AS (
    SELECT
        CONCAT('LINNWORKS_STOCK::', COALESCE(stock_transaction_id, source_stock_transaction_id, SHA2(CONCAT_WS('||', COALESCE(order_id, 'N/A'), COALESCE(transaction_at_utc, CAST('1900-01-01' AS TIMESTAMP)), COALESCE(stock_item_id, 'N/A')), 256))) AS activity_id,
        'linnworks_stock_dispatch' AS activity_source,
        client_id,
        'fulfillment_pick' AS service_type,
        CAST(NULL AS STRING) AS zone_id,
        CAST(transaction_at_utc AS DATE) AS activity_date,
        'per_unit' AS expected_rate_unit,
        'absolute_stock_dispatch_transaction_quantity' AS quantity_basis,
        CAST(ABS(quantity) AS DECIMAL(18, 4)) AS candidate_quantity,
        CASE
            WHEN client_id IS NOT NULL
             AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
             AND transaction_at_utc IS NOT NULL
             AND quantity IS NOT NULL
             AND quantity <> 0
             AND has_matched_order = TRUE
             AND COALESCE(is_deleted_order_reference, FALSE) = FALSE
                THEN CAST(ABS(quantity) AS DECIMAL(18, 4))
            ELSE CAST(0 AS DECIMAL(18, 4))
        END AS billable_quantity,
        CASE
            WHEN client_id IS NULL OR LOWER(TRIM(client_id)) IN ('', 'n/a', 'null')
                THEN 'MISSING_CUSTOMER_MAPPING'
            WHEN transaction_at_utc IS NULL THEN 'MISSING_SERVICE_DATE'
            WHEN quantity IS NULL OR quantity = 0 THEN 'REVIEW_MISSING_DISPATCH_QUANTITY'
            WHEN COALESCE(is_deleted_order_reference, FALSE) = TRUE
                THEN 'REVIEW_DELETED_ORDER_REFERENCE'
            WHEN has_matched_order = FALSE
                THEN 'REVIEW_MISSING_ORDER_REFERENCE'
            ELSE 'ELIGIBLE'
        END AS activity_eligibility_status,
        'transaction_at_utc' AS service_date_basis,
        stock_transaction_id AS source_activity_id,
        order_id AS source_order_id,
        quantity AS source_stock_quantity
    FROM stock_dispatch_source
),

orders_without_dispatch AS (
    SELECT
        CONCAT('LINNWORKS_ORDER_FALLBACK::', COALESCE(o.order_id, SHA2(CONCAT_WS('||', COALESCE(o.client_id, 'N/A'), COALESCE(o.dispatched_at_utc, CAST('1900-01-01' AS TIMESTAMP))), 256))) AS activity_id,
        'linnworks_dispatched_order_fallback' AS activity_source,
        o.client_id,
        'fulfillment_pick' AS service_type,
        CAST(NULL AS STRING) AS zone_id,
        CAST(o.dispatched_at_utc AS DATE) AS activity_date,
        'per_unit' AS expected_rate_unit,
        'order_item_quantity_without_matched_dispatch' AS quantity_basis,
        CAST(COALESCE(o.order_item_units, 0) AS DECIMAL(18, 4)) AS candidate_quantity,
        CAST(0 AS DECIMAL(18, 4)) AS billable_quantity,
        CASE
            WHEN o.client_id IS NULL OR LOWER(TRIM(o.client_id)) IN ('', 'n/a', 'null')
                THEN 'MISSING_CUSTOMER_MAPPING'
            WHEN COALESCE(o.order_item_units, 0) <= 0 THEN 'REVIEW_MISSING_FULFILLMENT_QUANTITY'
            WHEN COALESCE(o.composite_item_line_count, 0) > 0
                THEN 'REVIEW_COMPOSITE_ORDER_QUANTITY'
            ELSE 'REVIEW_NO_MATCHED_STOCK_DISPATCH'
        END AS activity_eligibility_status,
        'dispatched_at_utc' AS service_date_basis,
        o.order_id AS source_activity_id,
        o.order_id AS source_order_id,
        CAST(NULL AS BIGINT) AS source_stock_quantity
    FROM order_summary o
    LEFT ANTI JOIN (
        SELECT DISTINCT order_id
        FROM stock_dispatch_source
        WHERE order_id IS NOT NULL
    ) dispatch_orders
        ON o.order_id = dispatch_orders.order_id
    WHERE o.dispatched_at_utc IS NOT NULL
),

activity_base AS (
    SELECT * FROM onfleet_source
    UNION ALL
    SELECT * FROM stock_dispatch_activity
    UNION ALL
    SELECT * FROM orders_without_dispatch
),

contract_match_candidates AS (
    SELECT DISTINCT
        a.activity_id,
        c.contract_id,
        c.source_contract_id,
        c.client_id AS contract_client_id,
        c.contract_status,
        c.start_date,
        c.end_date,
        c.signed_date,
        c.contract_type,
        c.contract_discount_rate,
        c.net_payment_terms,
        c.committed_monthly_volume
    FROM activity_base a
    INNER JOIN contracts c
        ON a.client_id = c.client_id
       AND a.activity_date IS NOT NULL
       AND c.start_date <= a.activity_date
       AND (c.end_date IS NULL OR c.end_date >= a.activity_date)
       AND (c.signed_date IS NULL OR c.signed_date <= a.activity_date)
),

contract_match_counts AS (
    SELECT activity_id, COUNT(*) AS contract_candidate_count
    FROM contract_match_candidates
    GROUP BY activity_id
),

contract_single_candidate AS (
    SELECT c.*
    FROM contract_match_candidates c
    INNER JOIN contract_match_counts mc
        ON c.activity_id = mc.activity_id
       AND mc.contract_candidate_count = 1
),

activity_context AS (
    SELECT
        a.*,
        ac.account_id,
        ac.account_name,
        ac.account_type,
        ac.account_contract_type,
        ac.industry,
        ac.account_tier,
        ac.primary_city,
        ac.net_payment_terms AS account_net_payment_terms,
        COALESCE(mc.contract_candidate_count, 0) AS contract_candidate_count,
        sc.contract_id,
        sc.source_contract_id,
        sc.contract_status,
        sc.start_date AS contract_start_date,
        sc.end_date AS contract_end_date,
        sc.signed_date AS contract_signed_date,
        sc.contract_type AS applicable_contract_type,
        sc.contract_discount_rate,
        sc.net_payment_terms AS contract_net_payment_terms,
        sc.committed_monthly_volume,
        CASE
            WHEN LOWER(COALESCE(ac.account_contract_type, '')) RLIKE '(pay.?as.?you.?go|payg|weekly)'
                THEN 'WEEKLY'
            WHEN COALESCE(mc.contract_candidate_count, 0) = 1
                THEN 'MONTHLY'
            ELSE 'UNKNOWN'
        END AS billing_cadence,
        TRUNC(CAST(a.activity_date AS DATE), 'MM') AS service_month,
        CASE
            WHEN LOWER(COALESCE(ac.account_contract_type, '')) RLIKE '(pay.?as.?you.?go|payg|weekly)'
                THEN CAST(DATE_TRUNC('WEEK', CAST(a.activity_date AS TIMESTAMP)) AS DATE)
            ELSE TRUNC(CAST(a.activity_date AS DATE), 'MM')
        END AS billing_period_start
    FROM activity_base a
    LEFT JOIN accounts ac
        ON a.client_id = ac.client_id
    LEFT JOIN contract_match_counts mc
        ON a.activity_id = mc.activity_id
    LEFT JOIN contract_single_candidate sc
        ON a.activity_id = sc.activity_id
),

activity_with_period AS (
    SELECT
        *,
        CASE
            WHEN billing_cadence = 'WEEKLY' THEN DATE_ADD(billing_period_start, 6)
            WHEN billing_cadence = 'MONTHLY' THEN LAST_DAY(billing_period_start)
            WHEN billing_period_start IS NOT NULL THEN LAST_DAY(billing_period_start)
            ELSE CAST(NULL AS DATE)
        END AS billing_period_end
    FROM activity_context
),

rate_candidates AS (
    SELECT
        a.*,
        r.contract_rate_id,
        r.service_type AS rate_service_type,
        r.zone_id AS rate_zone_id,
        r.is_zone_specific,
        r.base_rate,
        r.discount_rate AS rate_discount_rate,
        r.net_rate AS source_net_rate,
        r.rate_unit,
        r.effective_date AS rate_effective_date,
        CASE
            WHEN r.contract_rate_id IS NULL THEN 99
            WHEN COALESCE(r.is_zone_specific, FALSE) = TRUE
             AND a.zone_id IS NOT NULL
             AND TRIM(r.zone_id) = TRIM(a.zone_id) THEN 1
            ELSE 2
        END AS rate_priority
    FROM activity_with_period a
    LEFT JOIN rates r
        ON a.contract_id = r.contract_id
       AND a.client_id = r.client_id
       AND a.service_type = r.service_type
       AND a.expected_rate_unit = r.rate_unit
       AND a.activity_date IS NOT NULL
       AND r.effective_date <= a.activity_date
       AND (
            (
                COALESCE(r.is_zone_specific, FALSE) = TRUE
                AND a.zone_id IS NOT NULL
                AND TRIM(r.zone_id) = TRIM(a.zone_id)
            )
            OR r.is_zone_specific = FALSE
            OR
            (
                r.is_zone_specific IS NULL
                AND (r.zone_id IS NULL OR TRIM(r.zone_id) = '' OR a.zone_id IS NULL)
            )
       )
),

rate_ranked AS (
    SELECT
        r.*,
        DENSE_RANK() OVER (
            PARTITION BY activity_id
            ORDER BY rate_priority ASC, rate_effective_date DESC NULLS LAST
        ) AS best_rate_rank,
        ROW_NUMBER() OVER (
            PARTITION BY activity_id
            ORDER BY rate_priority ASC, rate_effective_date DESC NULLS LAST,
                     contract_rate_id ASC NULLS LAST
        ) AS rate_row_num
    FROM rate_candidates r
),

rate_ranked_with_tie_count AS (
    SELECT
        r.*,
        SUM(
            CASE WHEN best_rate_rank = 1 AND contract_rate_id IS NOT NULL THEN 1 ELSE 0 END
        ) OVER (PARTITION BY activity_id) AS best_rate_candidate_count
    FROM rate_ranked r
),

priced_activity AS (
    SELECT
        r.*,
        COALESCE(r.rate_discount_rate, r.contract_discount_rate) AS applicable_discount_rate,
        COALESCE(
            r.source_net_rate,
            CASE
                WHEN r.base_rate IS NOT NULL
                 AND COALESCE(r.rate_discount_rate, r.contract_discount_rate) IS NULL
                    THEN r.base_rate
                WHEN r.base_rate IS NOT NULL
                 AND COALESCE(r.rate_discount_rate, r.contract_discount_rate) BETWEEN 0 AND 1
                    THEN r.base_rate * (1 - COALESCE(r.rate_discount_rate, r.contract_discount_rate))
                ELSE CAST(NULL AS DECIMAL(18, 4))
            END
        ) AS calculated_net_rate,
        CASE
            WHEN r.client_id IS NULL OR LOWER(TRIM(r.client_id)) IN ('', 'n/a', 'null')
                THEN 'MISSING_CUSTOMER_MAPPING'
            WHEN r.activity_date IS NULL THEN 'MISSING_SERVICE_DATE'
            WHEN r.service_type IS NULL THEN 'UNMAPPED_SERVICE_TYPE'
            WHEN r.activity_eligibility_status <> 'ELIGIBLE' THEN r.activity_eligibility_status
            WHEN r.billing_cadence = 'UNKNOWN' THEN 'REVIEW_BILLING_CADENCE_UNKNOWN'
            WHEN r.contract_candidate_count > 1 THEN 'AMBIGUOUS_CONTRACT'
            WHEN r.contract_candidate_count = 0 THEN 'NO_APPLICABLE_CONTRACT'
            WHEN r.contract_rate_id IS NULL THEN 'MISSING_APPLICABLE_RATE'
            WHEN COALESCE(r.best_rate_candidate_count, 0) > 1 THEN 'AMBIGUOUS_APPLICABLE_RATE'
            WHEN r.source_net_rate IS NULL
             AND COALESCE(r.rate_discount_rate, r.contract_discount_rate) IS NOT NULL
             AND (COALESCE(r.rate_discount_rate, r.contract_discount_rate) < 0 OR COALESCE(r.rate_discount_rate, r.contract_discount_rate) > 1)
                THEN 'REVIEW_DISCOUNT_RATE_SCALE'
            WHEN COALESCE(r.source_net_rate, r.base_rate) IS NULL THEN 'MISSING_RATE_AMOUNT'
            ELSE 'READY_TO_BILL'
        END AS billing_line_status
    FROM rate_ranked_with_tie_count r
    WHERE rate_row_num = 1
),

priced_activity_amounts AS (
    SELECT
        *,
        CASE
            WHEN billable_quantity > 0
             AND contract_candidate_count = 1
             AND COALESCE(best_rate_candidate_count, 0) = 1
             AND base_rate IS NOT NULL
                THEN CAST(billable_quantity * base_rate AS DECIMAL(18, 2))
            ELSE CAST(NULL AS DECIMAL(18, 2))
        END AS calculated_gross_charge,
        CASE
            WHEN billable_quantity > 0
             AND contract_candidate_count = 1
             AND COALESCE(best_rate_candidate_count, 0) = 1
             AND calculated_net_rate IS NOT NULL
                THEN CAST(billable_quantity * calculated_net_rate AS DECIMAL(18, 2))
            ELSE CAST(NULL AS DECIMAL(18, 2))
        END AS calculated_net_charge
    FROM priced_activity
),

aggregated_lines AS (
    SELECT
        client_id,
        account_name AS customer_name,
        account_type,
        account_contract_type,
        account_tier,
        industry,
        primary_city,
        account_net_payment_terms,
        billing_cadence,
        service_month,
        billing_period_start,
        billing_period_end,
        activity_source,
        service_type,
        zone_id,
        expected_rate_unit AS billing_unit,
        quantity_basis,
        activity_eligibility_status,
        billing_line_status,
        contract_candidate_count,
        contract_id AS applicable_contract_id,
        source_contract_id AS applicable_source_contract_id,
        contract_status AS contract_status_at_extract,
        contract_start_date,
        contract_end_date,
        contract_signed_date,
        applicable_contract_type,
        committed_monthly_volume,
        contract_discount_rate,
        contract_net_payment_terms,
        contract_rate_id AS applicable_contract_rate_id,
        rate_effective_date,
        rate_zone_id AS applicable_rate_zone_id,
        is_zone_specific AS applicable_rate_is_zone_specific,
        rate_unit,
        base_rate,
        applicable_discount_rate,
        source_net_rate,
        calculated_net_rate,
        MIN(activity_date) AS first_activity_date,
        MAX(activity_date) AS last_activity_date,
        service_date_basis,
        COUNT(DISTINCT activity_id) AS source_activity_count,
        COLLECT_SET(source_activity_id) AS source_activity_ids,
        COUNT(DISTINCT source_order_id) AS source_order_count,
        COLLECT_SET(source_order_id) AS source_order_ids,
        SUM(COALESCE(candidate_quantity, 0)) AS candidate_quantity,
        SUM(COALESCE(billable_quantity, 0)) AS billable_quantity,
        SUM(calculated_gross_charge) AS calculated_gross_charge,
        SUM(calculated_net_charge) AS calculated_net_charge
    FROM priced_activity_amounts
    GROUP BY
        client_id, account_name, account_type, account_contract_type,
        account_tier, industry, primary_city, account_net_payment_terms,
        billing_cadence, service_month, billing_period_start, billing_period_end,
        activity_source, service_type, zone_id, expected_rate_unit, quantity_basis,
        activity_eligibility_status, billing_line_status, service_date_basis, contract_candidate_count,
        contract_id, source_contract_id, contract_status, contract_start_date,
        contract_end_date, contract_signed_date, applicable_contract_type,
        committed_monthly_volume, contract_discount_rate, contract_net_payment_terms,
        contract_rate_id, rate_effective_date, rate_zone_id, is_zone_specific,
        rate_unit, base_rate, applicable_discount_rate, source_net_rate,
        calculated_net_rate
)

SELECT
    SHA2(CONCAT_WS('||',
        COALESCE(client_id, 'N/A'),
        COALESCE(DATE_FORMAT(service_month, 'yyyy-MM'), 'NO_SERVICE_MONTH'),
        COALESCE(DATE_FORMAT(billing_period_start, 'yyyy-MM-dd'), 'NO_BILLING_START'),
        COALESCE(activity_source, 'N/A'),
        COALESCE(service_type, 'UNMAPPED'),
        COALESCE(zone_id, 'NO_ZONE'),
        COALESCE(applicable_contract_id, 'NO_CONTRACT'),
        COALESCE(applicable_contract_rate_id, 'NO_RATE'),
        COALESCE(activity_eligibility_status, 'N/A'),
        COALESCE(billing_line_status, 'N/A'),
        COALESCE(quantity_basis, 'N/A'),
        COALESCE(service_date_basis, 'N/A')
    ), 256) AS billing_line_key,
    client_id,
    customer_name,
    account_type,
    account_contract_type,
    account_tier,
    industry,
    primary_city,
    account_net_payment_terms,
    billing_cadence,
    service_month,
    billing_period_start,
    billing_period_end,
    DATE_FORMAT(billing_period_start, 'yyyy-MM-dd') AS billing_period_label,
    activity_source,
    service_type,
    zone_id,
    billing_unit,
    quantity_basis,
    activity_eligibility_status,
    billing_line_status,
    CASE
        WHEN billing_line_status = 'READY_TO_BILL' THEN 'READY'
        WHEN billing_line_status IN ('NOT_COMPLETED') THEN 'NOT_BILLABLE_UNDER_CURRENT_RULE'
        ELSE 'REVIEW_REQUIRED'
    END AS billing_review_status,
    CASE billing_line_status
        WHEN 'MISSING_CUSTOMER_MAPPING' THEN 'Canonical client_id is missing or invalid.'
        WHEN 'MISSING_SERVICE_DATE' THEN 'No usable service date is available.'
        WHEN 'UNMAPPED_SERVICE_TYPE' THEN 'The source service type does not map to a supported rate-card service.'
        WHEN 'REVIEW_PICKUP_CHARGEABILITY' THEN 'Pickup tasks are not automatically charged until their contractual billability is established.'
        WHEN 'REVIEW_MISSING_COMPLETION_TIMESTAMP' THEN 'Successful task has no completion timestamp; creation time is not accepted as a final billable service date.'
        WHEN 'REVIEW_FAILED_TASK_CHARGEABILITY' THEN 'Failed-task chargeability is not established by the available source data.'
        WHEN 'REVIEW_MISSING_COMPLETION_RESULT' THEN 'Task is marked completed but has no recognized success/failure result.'
        WHEN 'NOT_COMPLETED' THEN 'Task is not marked completed; it is excluded from chargeable quantity.'
        WHEN 'REVIEW_MISSING_DISPATCH_QUANTITY' THEN 'Dispatch transaction has no non-zero quantity.'
        WHEN 'REVIEW_DELETED_ORDER_REFERENCE' THEN 'Dispatch reference is marked as deleted or unresolved.'
        WHEN 'REVIEW_MISSING_ORDER_REFERENCE' THEN 'Dispatch transaction is not tied to a matched order.'
        WHEN 'REVIEW_MISSING_FULFILLMENT_QUANTITY' THEN 'Dispatched order has no usable item quantity.'
        WHEN 'REVIEW_COMPOSITE_ORDER_QUANTITY' THEN 'Only order-item quantities are available and composite lines may affect the unit count.'
        WHEN 'REVIEW_NO_MATCHED_STOCK_DISPATCH' THEN 'The order is marked dispatched but no matching stock dispatch transaction was found; order item units are review-only.'
        WHEN 'REVIEW_BILLING_CADENCE_UNKNOWN' THEN 'Billing cadence could not be established as monthly contracted or weekly pay-as-you-go.'
        WHEN 'AMBIGUOUS_CONTRACT' THEN 'More than one contract is in force for the customer and service date.'
        WHEN 'NO_APPLICABLE_CONTRACT' THEN 'No in-force signed/activated contract was found for the activity date.'
        WHEN 'MISSING_APPLICABLE_RATE' THEN 'No effective service/unit/zone rate was found for the applicable contract.'
        WHEN 'AMBIGUOUS_APPLICABLE_RATE' THEN 'Multiple rates share the best zone preference and latest effective date.'
        WHEN 'REVIEW_DISCOUNT_RATE_SCALE' THEN 'No source net_rate is available and the discount rate is outside the expected 0-to-1 fraction range.'
        WHEN 'MISSING_RATE_AMOUNT' THEN 'The chosen rate has neither a usable net rate nor a base rate.'
        ELSE 'Billable activity has a unique applicable contract and rate.'
    END AS review_explanation,
    contract_candidate_count,
    applicable_contract_id,
    applicable_source_contract_id,
    contract_status_at_extract,
    contract_start_date,
    contract_end_date,
    contract_signed_date,
    applicable_contract_type,
    committed_monthly_volume,
    contract_discount_rate,
    contract_net_payment_terms,
    applicable_contract_rate_id,
    rate_effective_date,
    applicable_rate_zone_id,
    applicable_rate_is_zone_specific,
    rate_unit,
    base_rate,
    applicable_discount_rate,
    source_net_rate,
    calculated_net_rate,
    first_activity_date,
    last_activity_date,
    service_date_basis,
    source_activity_count,
    source_activity_ids,
    source_order_count,
    source_order_ids,
    CAST(candidate_quantity AS DECIMAL(18, 4)) AS candidate_quantity,
    CAST(billable_quantity AS DECIMAL(18, 4)) AS billable_quantity,
    calculated_gross_charge,
    calculated_net_charge,
    CASE
        WHEN billing_line_status = 'READY_TO_BILL'
            THEN calculated_net_charge
        ELSE CAST(NULL AS DECIMAL(18, 2))
    END AS invoice_ready_amount
FROM aggregated_lines
