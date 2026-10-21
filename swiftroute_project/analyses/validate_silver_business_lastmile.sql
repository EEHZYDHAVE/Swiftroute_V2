/*
SwiftRoute — Silver Business Last-Mile validation queries

Purpose:
  Validate relationship quality and source grains BEFORE building the wide
  one-row-per-Onfleet-delivery Silver Business model.

Run each numbered query separately in Databricks SQL. The queries use the
physical Silver Technical table names provided by the user (silver_t.*_t).

These are diagnostics, not assertions that every enrichment must match.
Do not force a relationship into the model if the evidence is weak.
*/

/* -------------------------------------------------------------------------
1. Onfleet delivery/task grain and task scope

What this checks:
- Whether task_id is present and unique.
- How many source tasks are pickup tasks versus non-pickup tasks.
- Whether task status, outcome, client, and service are populated.

This helps decide whether pickup_task = true should be excluded from the
last-mile delivery spine or retained as a distinct task type.
--------------------------------------------------------------------------- */
SELECT
    COUNT(*) AS task_rows,
    COUNT(DISTINCT task_id) AS distinct_task_ids,
    SUM(CASE WHEN task_id IS NULL OR TRIM(task_id) = '' OR UPPER(TRIM(task_id)) = 'N/A' THEN 1 ELSE 0 END) AS missing_task_ids,
    SUM(CASE WHEN source_task_id IS NULL OR TRIM(source_task_id) = '' OR UPPER(TRIM(source_task_id)) = 'N/A' THEN 1 ELSE 0 END) AS missing_source_task_ids,
    SUM(CASE WHEN COALESCE(pickup_task, FALSE) THEN 1 ELSE 0 END) AS pickup_task_rows,
    SUM(CASE WHEN COALESCE(pickup_task, FALSE) = FALSE THEN 1 ELSE 0 END) AS non_pickup_task_rows,
    SUM(CASE WHEN client_id IS NULL OR TRIM(client_id) = '' OR UPPER(TRIM(client_id)) = 'N/A' THEN 1 ELSE 0 END) AS tasks_without_client_id,
    SUM(CASE WHEN worker_id IS NULL OR TRIM(worker_id) = '' OR UPPER(TRIM(worker_id)) = 'N/A' OR worker_reference_missing THEN 1 ELSE 0 END) AS tasks_without_worker_reference,
    SUM(CASE WHEN completion_result IS NULL OR TRIM(completion_result) = '' OR UPPER(TRIM(completion_result)) = 'N/A' THEN 1 ELSE 0 END) AS tasks_without_completion_result,
    SUM(CASE WHEN completion_time_utc IS NULL THEN 1 ELSE 0 END) AS tasks_without_completion_time
FROM silver_t.onfleet_deliveries_t;


-- Query 1B: duplicate task keys (expect zero rows).
SELECT task_id, COUNT(*) AS rows_for_task_id
FROM silver_t.onfleet_deliveries_t
GROUP BY task_id
HAVING task_id IS NULL OR COUNT(*) > 1
ORDER BY rows_for_task_id DESC
LIMIT 50;

-- Query 1C: inspect task-state / pickup / outcome / service profile.
SELECT
    pickup_task,
    task_state,
    completion_result,
    service_type,
    COUNT(*) AS task_count
FROM silver_t.onfleet_deliveries_t
GROUP BY pickup_task, task_state, completion_result, service_type
ORDER BY task_count DESC;


/* -------------------------------------------------------------------------
2. Client, time-valid contract, and applicable service-rate coverage

The event date uses completion_time_utc where available, otherwise
 task_created_at_utc. This is a diagnostics-only fallback for open/incomplete
 tasks; the business model should retain the underlying timestamps separately.

Contract candidates are selected by shared client_id and date validity.
Rate candidates additionally require a service_type match, compatible zone
(when a rate is zone-specific), and effective_date not later than the event.
The query reports candidate coverage and ambiguity; it does not decide a rate.
--------------------------------------------------------------------------- */
WITH delivery_base AS (
    SELECT
        task_id,
        NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') AS client_id,
        LOWER(TRIM(service_type)) AS normalized_service_type,
        NULLIF(NULLIF(TRIM(zone_id), ''), 'N/A') AS zone_id,
        CAST(COALESCE(completion_time_utc, task_created_at_utc) AS DATE) AS event_date
    FROM silver_t.onfleet_deliveries_t
),
account_counts AS (
    SELECT
        d.task_id,
        COUNT(DISTINCT a.account_id) AS account_match_count
    FROM delivery_base d
    LEFT JOIN silver_t.salesforce_accounts_t a
      ON d.client_id = NULLIF(NULLIF(TRIM(a.client_id), ''), 'N/A')
    GROUP BY d.task_id
),
contract_candidates AS (
    SELECT DISTINCT
        d.task_id,
        c.contract_id
    FROM delivery_base d
    JOIN silver_t.salesforce_contracts_t c
      ON d.client_id = NULLIF(NULLIF(TRIM(c.client_id), ''), 'N/A')
     AND d.event_date IS NOT NULL
     AND c.start_date IS NOT NULL
     AND d.event_date >= c.start_date
     AND (c.end_date IS NULL OR d.event_date <= c.end_date)
),
contract_counts AS (
    SELECT task_id, COUNT(DISTINCT contract_id) AS active_contract_match_count
    FROM contract_candidates
    GROUP BY task_id
),
rate_candidates AS (
    SELECT DISTINCT
        d.task_id,
        r.contract_rate_id,
        r.effective_date
    FROM delivery_base d
    JOIN contract_candidates cc
      ON cc.task_id = d.task_id
    JOIN silver_t.salesforce_contract_rates_t r
      ON r.contract_id = cc.contract_id
     AND d.normalized_service_type IS NOT NULL
     AND LOWER(TRIM(r.service_type)) = d.normalized_service_type
     AND r.effective_date IS NOT NULL
     AND d.event_date >= r.effective_date
     AND (
           r.is_zone_specific = FALSE
           OR (
                r.is_zone_specific = TRUE
                AND d.zone_id IS NOT NULL
                AND NULLIF(NULLIF(TRIM(r.zone_id), ''), 'N/A') = d.zone_id
           )
         )
),
rate_counts AS (
    SELECT
        task_id,
        COUNT(*) AS applicable_rate_candidate_count,
        SUM(CASE WHEN effective_date = latest_effective_date THEN 1 ELSE 0 END) AS candidates_at_latest_effective_date
    FROM (
        SELECT
            task_id,
            contract_rate_id,
            effective_date,
            MAX(effective_date) OVER (PARTITION BY task_id) AS latest_effective_date
        FROM rate_candidates
    ) ranked_candidates
    GROUP BY task_id
),
per_delivery AS (
    SELECT
        d.task_id,
        COALESCE(a.account_match_count, 0) AS account_match_count,
        COALESCE(c.active_contract_match_count, 0) AS active_contract_match_count,
        COALESCE(r.applicable_rate_candidate_count, 0) AS applicable_rate_candidate_count,
        COALESCE(r.candidates_at_latest_effective_date, 0) AS candidates_at_latest_effective_date
    FROM delivery_base d
    LEFT JOIN account_counts a ON a.task_id = d.task_id
    LEFT JOIN contract_counts c ON c.task_id = d.task_id
    LEFT JOIN rate_counts r ON r.task_id = d.task_id
)
SELECT
    COUNT(*) AS deliveries_profiled,
    SUM(CASE WHEN account_match_count = 0 THEN 1 ELSE 0 END) AS deliveries_without_account_match,
    SUM(CASE WHEN account_match_count > 1 THEN 1 ELSE 0 END) AS deliveries_with_multiple_account_matches,
    SUM(CASE WHEN active_contract_match_count = 0 THEN 1 ELSE 0 END) AS deliveries_without_date_valid_contract,
    SUM(CASE WHEN active_contract_match_count > 1 THEN 1 ELSE 0 END) AS deliveries_with_multiple_date_valid_contracts,
    SUM(CASE WHEN applicable_rate_candidate_count = 0 THEN 1 ELSE 0 END) AS deliveries_without_applicable_rate_candidate,
    SUM(CASE WHEN candidates_at_latest_effective_date > 1 THEN 1 ELSE 0 END) AS deliveries_with_multiple_rates_at_latest_effective_date
FROM per_delivery;

-- Query 2B: show which service types/zones are missing exact rate coverage.
WITH delivery_base AS (
    SELECT
        task_id,
        NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') AS client_id,
        LOWER(TRIM(service_type)) AS normalized_service_type,
        NULLIF(NULLIF(TRIM(zone_id), ''), 'N/A') AS zone_id,
        CAST(COALESCE(completion_time_utc, task_created_at_utc) AS DATE) AS event_date
    FROM silver_t.onfleet_deliveries_t
),
valid_contracts AS (
    SELECT DISTINCT d.task_id, c.contract_id
    FROM delivery_base d
    JOIN silver_t.salesforce_contracts_t c
      ON d.client_id = NULLIF(NULLIF(TRIM(c.client_id), ''), 'N/A')
     AND d.event_date IS NOT NULL
     AND c.start_date IS NOT NULL
     AND d.event_date >= c.start_date
     AND (c.end_date IS NULL OR d.event_date <= c.end_date)
)
SELECT
    d.normalized_service_type AS service_type,
    COALESCE(d.zone_id, 'N/A') AS zone_id,
    COUNT(DISTINCT d.task_id) AS delivery_count,
    COUNT(DISTINCT CASE WHEN r.contract_rate_id IS NOT NULL THEN d.task_id END) AS deliveries_with_rate_candidate,
    COUNT(DISTINCT CASE WHEN r.contract_rate_id IS NULL THEN d.task_id END) AS deliveries_without_rate_candidate
FROM delivery_base d
LEFT JOIN valid_contracts c ON c.task_id = d.task_id
LEFT JOIN silver_t.salesforce_contract_rates_t r
  ON r.contract_id = c.contract_id
 AND LOWER(TRIM(r.service_type)) = d.normalized_service_type
 AND r.effective_date IS NOT NULL
 AND d.event_date >= r.effective_date
 AND (
       r.is_zone_specific = FALSE
       OR (r.is_zone_specific = TRUE AND d.zone_id IS NOT NULL AND NULLIF(NULLIF(TRIM(r.zone_id), ''), 'N/A') = d.zone_id)
     )
GROUP BY d.normalized_service_type, COALESCE(d.zone_id, 'N/A')
ORDER BY delivery_count DESC;


/* -------------------------------------------------------------------------
3. Can Onfleet deliveries be linked to Linnworks customer orders?

Tests three candidate methods within the same SwiftRoute client:
1) an order reference appears in Onfleet task_notes;
2) recipient phone equals Linnworks shipping address phone;
3) destination postal code + city + street match Linnworks shipping address.

The counts are evidence for choosing a matching rule, not permission to join
all candidate matches. Multiple orders matching one delivery are ambiguous.
--------------------------------------------------------------------------- */
WITH delivery_base AS (
    SELECT
        task_id,
        NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') AS client_id,
        task_notes,
        recipient_phone,
        destination_postal_code,
        destination_city,
        destination_street,
        destination_number
    FROM silver_t.onfleet_deliveries_t
),
orders_base AS (
    SELECT
        order_id,
        NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') AS client_id,
        source_order_id,
        reference_num,
        external_reference,
        secondary_reference,
        CAST(num_order_id AS STRING) AS num_order_id,
        address_phone,
        address_post_code,
        address_city,
        address1
    FROM silver_t.linnworks_orders_t
),
order_tokens AS (
    SELECT DISTINCT
        order_id,
        client_id,
        TRIM(reference_token) AS reference_token
    FROM orders_base
    LATERAL VIEW EXPLODE(ARRAY(
        CAST(source_order_id AS STRING),
        CAST(reference_num AS STRING),
        CAST(external_reference AS STRING),
        CAST(secondary_reference AS STRING),
        CAST(num_order_id AS STRING)
    )) tokens AS reference_token
    WHERE reference_token IS NOT NULL
      AND TRIM(reference_token) <> ''
      AND UPPER(TRIM(reference_token)) <> 'N/A'
      AND LENGTH(TRIM(reference_token)) >= 4
),
reference_matches AS (
    SELECT DISTINCT d.task_id, o.order_id, 'TASK_NOTES_REFERENCE' AS match_method
    FROM delivery_base d
    JOIN order_tokens o
      ON o.client_id = d.client_id
     AND INSTR(LOWER(COALESCE(d.task_notes, '')), LOWER(o.reference_token)) > 0
),
phone_matches AS (
    SELECT DISTINCT d.task_id, o.order_id, 'CLIENT_AND_PHONE' AS match_method
    FROM delivery_base d
    JOIN orders_base o
      ON o.client_id = d.client_id
     AND LENGTH(REGEXP_REPLACE(COALESCE(d.recipient_phone, ''), '[^0-9]', '')) >= 7
     AND REGEXP_REPLACE(COALESCE(d.recipient_phone, ''), '[^0-9]', '')
         = REGEXP_REPLACE(COALESCE(o.address_phone, ''), '[^0-9]', '')
),
address_matches AS (
    SELECT DISTINCT d.task_id, o.order_id, 'CLIENT_POSTAL_CITY_STREET' AS match_method
    FROM delivery_base d
    JOIN orders_base o
      ON o.client_id = d.client_id
     AND LENGTH(TRIM(COALESCE(d.destination_postal_code, ''))) >= 3
     AND UPPER(TRIM(d.destination_postal_code)) = UPPER(TRIM(o.address_post_code))
     AND UPPER(TRIM(COALESCE(d.destination_city, ''))) = UPPER(TRIM(COALESCE(o.address_city, '')))
     AND LENGTH(TRIM(COALESCE(d.destination_street, ''))) >= 4
     AND REGEXP_REPLACE(UPPER(TRIM(d.destination_street)), '[^A-Z0-9]', '')
         = REGEXP_REPLACE(UPPER(TRIM(o.address1)), '[^A-Z0-9]', '')
),
all_matches AS (
    SELECT * FROM reference_matches
    UNION ALL
    SELECT * FROM phone_matches
    UNION ALL
    SELECT * FROM address_matches
),
per_method_delivery AS (
    SELECT match_method, task_id, COUNT(DISTINCT order_id) AS candidate_order_count
    FROM all_matches
    GROUP BY match_method, task_id
)
SELECT
    match_method,
    COUNT(*) AS deliveries_with_at_least_one_candidate,
    SUM(CASE WHEN candidate_order_count = 1 THEN 1 ELSE 0 END) AS deliveries_with_one_candidate,
    SUM(CASE WHEN candidate_order_count > 1 THEN 1 ELSE 0 END) AS deliveries_with_ambiguous_candidates,
    MAX(candidate_order_count) AS maximum_candidates_for_one_delivery
FROM per_method_delivery
GROUP BY match_method
ORDER BY match_method;

-- Query 3B: show a sample of ambiguous order matches, if any.
WITH delivery_base AS (
    SELECT task_id, NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') AS client_id, task_notes
    FROM silver_t.onfleet_deliveries_t
),
orders_base AS (
    SELECT order_id, NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') AS client_id,
           source_order_id, reference_num, external_reference, secondary_reference,
           CAST(num_order_id AS STRING) AS num_order_id
    FROM silver_t.linnworks_orders_t
),
order_tokens AS (
    SELECT DISTINCT order_id, client_id, TRIM(reference_token) AS reference_token
    FROM orders_base
    LATERAL VIEW EXPLODE(ARRAY(
        CAST(source_order_id AS STRING), CAST(reference_num AS STRING),
        CAST(external_reference AS STRING), CAST(secondary_reference AS STRING),
        CAST(num_order_id AS STRING)
    )) tokens AS reference_token
    WHERE reference_token IS NOT NULL AND TRIM(reference_token) <> ''
      AND UPPER(TRIM(reference_token)) <> 'N/A' AND LENGTH(TRIM(reference_token)) >= 4
),
reference_matches AS (
    SELECT DISTINCT d.task_id, o.order_id
    FROM delivery_base d
    JOIN order_tokens o ON o.client_id = d.client_id
      AND INSTR(LOWER(COALESCE(d.task_notes, '')), LOWER(o.reference_token)) > 0
)
SELECT task_id, COUNT(DISTINCT order_id) AS matching_orders
FROM reference_matches
GROUP BY task_id
HAVING COUNT(DISTINCT order_id) > 1
ORDER BY matching_orders DESC
LIMIT 100;


/* -------------------------------------------------------------------------
4. Driver, employee, vehicle, trip, and payroll mapping feasibility

No compensation amounts are proposed for copying onto delivery rows. Payroll
and trip measures are higher-grain and would be repeated across deliveries.
This checks whether identity/time context can be attached safely.
--------------------------------------------------------------------------- */
WITH delivery_worker_matches AS (
    SELECT DISTINCT d.task_id, d.worker_id, w.worker_id AS matched_worker_id
    FROM silver_t.onfleet_deliveries_t d
    LEFT JOIN silver_t.onfleet_workers_t w
      ON NULLIF(NULLIF(TRIM(d.worker_id), ''), 'N/A') = w.worker_id
),
worker_employee_matches AS (
    SELECT
        w.worker_id,
        COUNT(DISTINCT e.employee_uuid) AS employee_match_count
    FROM silver_t.onfleet_workers_t w
    LEFT JOIN silver_t.gusto_employees_t e
      ON w.worker_id = NULLIF(NULLIF(TRIM(e.onfleet_worker_id), ''), 'N/A')
    GROUP BY w.worker_id
),
worker_vehicle_matches AS (
    SELECT
        w.worker_id,
        COUNT(DISTINCT v.vehicle_id) AS samsara_vehicle_matches
    FROM silver_t.onfleet_workers_t w
    LEFT JOIN silver_t.samsara_vehicles_t v
      ON (
           NULLIF(NULLIF(TRIM(w.vehicle_id), ''), 'N/A') IS NOT NULL
           AND TRIM(w.vehicle_id) = TRIM(v.vehicle_id)
         )
      OR (
           NULLIF(NULLIF(TRIM(w.vehicle_license_plate), ''), 'N/A') IS NOT NULL
           AND REGEXP_REPLACE(UPPER(TRIM(w.vehicle_license_plate)), '[^A-Z0-9]', '')
               = REGEXP_REPLACE(UPPER(TRIM(v.license_plate)), '[^A-Z0-9]', '')
         )
    GROUP BY w.worker_id
),
worker_samsara_name_matches AS (
    SELECT DISTINCT w.worker_id, s.driver_id
    FROM silver_t.onfleet_workers_t w
    JOIN silver_t.samsara_driver_summary_t s
      ON UPPER(TRIM(COALESCE(NULLIF(NULLIF(TRIM(w.worker_name), ''), 'N/A'), NULLIF(NULLIF(TRIM(w.display_name), ''), 'N/A'), ''))) = UPPER(TRIM(s.driver_name))
     AND LENGTH(TRIM(COALESCE(NULLIF(NULLIF(TRIM(w.worker_name), ''), 'N/A'), NULLIF(NULLIF(TRIM(w.display_name), ''), 'N/A'), ''))) > 0
),
base_metrics AS (
    SELECT 'DELIVERY_TO_ONFLEET_WORKER' AS relationship,
           COUNT(DISTINCT task_id) AS source_entities,
           COUNT(DISTINCT CASE WHEN matched_worker_id IS NOT NULL THEN task_id END) AS matched_entities,
           CAST(NULL AS BIGINT) AS ambiguous_entities
    FROM delivery_worker_matches
    UNION ALL
    SELECT 'ONFLEET_WORKER_TO_GUSTO_EMPLOYEE',
           COUNT(DISTINCT worker_id),
           COUNT(DISTINCT CASE WHEN employee_match_count > 0 THEN worker_id END),
           COUNT(DISTINCT CASE WHEN employee_match_count > 1 THEN worker_id END)
    FROM worker_employee_matches
    UNION ALL
    SELECT 'ONFLEET_WORKER_TO_SAMSARA_VEHICLE',
           COUNT(DISTINCT worker_id),
           COUNT(DISTINCT CASE WHEN samsara_vehicle_matches > 0 THEN worker_id END),
           COUNT(DISTINCT CASE WHEN samsara_vehicle_matches > 1 THEN worker_id END)
    FROM worker_vehicle_matches
    UNION ALL
    SELECT 'ONFLEET_WORKER_TO_SAMSARA_DRIVER_NAME',
           (SELECT COUNT(DISTINCT worker_id) FROM silver_t.onfleet_workers_t),
           COUNT(DISTINCT worker_id),
           CAST(NULL AS BIGINT)
    FROM worker_samsara_name_matches
)
SELECT
    relationship,
    source_entities,
    matched_entities,
    source_entities - matched_entities AS unmatched_entities,
    ambiguous_entities
FROM base_metrics
ORDER BY relationship;

-- Query 4B: can delivery completion timestamps be placed inside a vehicle trip?
-- This uses current Onfleet worker vehicle details as a candidate bridge only.
WITH delivery_base AS (
    SELECT task_id, worker_id, completion_time_utc
    FROM silver_t.onfleet_deliveries_t
    WHERE completion_time_utc IS NOT NULL
      AND COALESCE(pickup_task, FALSE) = FALSE
),
worker_vehicle AS (
    SELECT
        w.worker_id,
        v.vehicle_id AS samsara_vehicle_id
    FROM silver_t.onfleet_workers_t w
    JOIN silver_t.samsara_vehicles_t v
      ON NULLIF(NULLIF(TRIM(w.vehicle_license_plate), ''), 'N/A') IS NOT NULL
     AND REGEXP_REPLACE(UPPER(TRIM(w.vehicle_license_plate)), '[^A-Z0-9]', '')
         = REGEXP_REPLACE(UPPER(TRIM(v.license_plate)), '[^A-Z0-9]', '')
),
trip_candidates AS (
    SELECT DISTINCT d.task_id, t.trip_id
    FROM delivery_base d
    JOIN worker_vehicle w ON w.worker_id = d.worker_id
    JOIN silver_t.samsara_trips_t t ON t.vehicle_id = w.samsara_vehicle_id
      AND d.completion_time_utc >= t.trip_start_at_utc
      AND d.completion_time_utc <= t.trip_end_at_utc
),
per_delivery AS (
    SELECT d.task_id, COUNT(DISTINCT t.trip_id) AS trip_candidate_count
    FROM delivery_base d
    LEFT JOIN trip_candidates t ON t.task_id = d.task_id
    GROUP BY d.task_id
)
SELECT
    COUNT(*) AS completed_non_pickup_deliveries,
    SUM(CASE WHEN trip_candidate_count = 0 THEN 1 ELSE 0 END) AS deliveries_without_trip_candidate,
    SUM(CASE WHEN trip_candidate_count = 1 THEN 1 ELSE 0 END) AS deliveries_with_one_trip_candidate,
    SUM(CASE WHEN trip_candidate_count > 1 THEN 1 ELSE 0 END) AS deliveries_with_multiple_trip_candidates,
    MAX(trip_candidate_count) AS maximum_trip_candidates_per_delivery
FROM per_delivery;

-- Query 4C: for a delivery's driver/pay period, how many Gusto compensation
-- records overlap the delivery date? This is only a grain/match diagnostic.
WITH delivery_base AS (
    SELECT
        d.task_id,
        CAST(COALESCE(d.completion_time_utc, d.task_created_at_utc) AS DATE) AS event_date,
        w.worker_id
    FROM silver_t.onfleet_deliveries_t d
    LEFT JOIN silver_t.onfleet_workers_t w
      ON NULLIF(NULLIF(TRIM(d.worker_id), ''), 'N/A') = w.worker_id
    WHERE COALESCE(d.pickup_task, FALSE) = FALSE
),
delivery_employee AS (
    SELECT d.task_id, d.event_date, e.employee_uuid
    FROM delivery_base d
    LEFT JOIN silver_t.gusto_employees_t e
      ON d.worker_id = NULLIF(NULLIF(TRIM(e.onfleet_worker_id), ''), 'N/A')
),
compensation_candidates AS (
    SELECT DISTINCT d.task_id, p.payroll_compensation_id
    FROM delivery_employee d
    JOIN silver_t.gusto_payroll_compensations_t p
      ON p.employee_uuid = d.employee_uuid
     AND d.event_date IS NOT NULL
     AND d.event_date >= p.pay_period_start_date
     AND d.event_date <= p.pay_period_end_date
),
per_delivery AS (
    SELECT d.task_id, COUNT(DISTINCT p.payroll_compensation_id) AS compensation_row_count
    FROM delivery_employee d
    LEFT JOIN compensation_candidates p ON p.task_id = d.task_id
    GROUP BY d.task_id
)
SELECT
    COUNT(*) AS deliveries_profiled,
    SUM(CASE WHEN compensation_row_count = 0 THEN 1 ELSE 0 END) AS deliveries_without_matching_payroll_period,
    SUM(CASE WHEN compensation_row_count = 1 THEN 1 ELSE 0 END) AS deliveries_with_one_payroll_period_record,
    SUM(CASE WHEN compensation_row_count > 1 THEN 1 ELSE 0 END) AS deliveries_with_multiple_payroll_period_records
FROM per_delivery;


/* -------------------------------------------------------------------------
5. QuickBooks client-month context and payment-to-invoice link coverage

Invoices do not contain a delivery ID. This tests whether client_id + the
calendar month of the delivery provides usable billing-period context.
Payment presence is determined only from explicit Payment line links to an
Invoice. Amounts are deliberately not allocated to deliveries.
--------------------------------------------------------------------------- */
WITH payment_invoice_links AS (
    SELECT DISTINCT
        p.payment_id,
        CAST(linked_transaction.transaction_id AS STRING) AS invoice_id
    FROM silver_t.quickbooks_payments_t p
    LATERAL VIEW OUTER EXPLODE(p.lines) payment_lines AS payment_line
    LATERAL VIEW OUTER EXPLODE(payment_line.linked_transactions) linked_lines AS linked_transaction
    WHERE LOWER(TRIM(COALESCE(linked_transaction.transaction_type, ''))) = 'invoice'
      AND linked_transaction.transaction_id IS NOT NULL
      AND TRIM(linked_transaction.transaction_id) <> ''
),
invoices_by_client_month AS (
    SELECT
        NULLIF(NULLIF(TRIM(i.client_id), ''), 'N/A') AS client_id,
        NULLIF(NULLIF(TRIM(i.billing_period), ''), 'N/A') AS billing_period,
        COUNT(DISTINCT i.invoice_id) AS invoice_count,
        COUNT(DISTINCT CASE WHEN pil.payment_id IS NOT NULL THEN i.invoice_id END) AS invoices_with_explicit_payment_link
    FROM silver_t.quickbooks_invoices_t i
    LEFT JOIN payment_invoice_links pil
      ON CAST(pil.invoice_id AS STRING) = CAST(i.invoice_id AS STRING)
    WHERE NULLIF(NULLIF(TRIM(i.client_id), ''), 'N/A') IS NOT NULL
      AND NULLIF(NULLIF(TRIM(i.billing_period), ''), 'N/A') IS NOT NULL
    GROUP BY
        NULLIF(NULLIF(TRIM(i.client_id), ''), 'N/A'),
        NULLIF(NULLIF(TRIM(i.billing_period), ''), 'N/A')
),
deliveries_by_client_month AS (
    SELECT
        NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') AS client_id,
        DATE_FORMAT(CAST(COALESCE(completion_time_utc, task_created_at_utc) AS DATE), 'yyyy-MM') AS billing_period,
        COUNT(DISTINCT task_id) AS delivery_count
    FROM silver_t.onfleet_deliveries_t
    WHERE COALESCE(pickup_task, FALSE) = FALSE
      AND NULLIF(NULLIF(TRIM(client_id), ''), 'N/A') IS NOT NULL
      AND COALESCE(completion_time_utc, task_created_at_utc) IS NOT NULL
    GROUP BY
        NULLIF(NULLIF(TRIM(client_id), ''), 'N/A'),
        DATE_FORMAT(CAST(COALESCE(completion_time_utc, task_created_at_utc) AS DATE), 'yyyy-MM')
)
SELECT
    d.client_id,
    d.billing_period,
    d.delivery_count,
    COALESCE(i.invoice_count, 0) AS invoice_count_for_client_month,
    COALESCE(i.invoices_with_explicit_payment_link, 0) AS invoices_with_explicit_payment_link
FROM deliveries_by_client_month d
LEFT JOIN invoices_by_client_month i
  ON i.client_id = d.client_id
 AND i.billing_period = d.billing_period
ORDER BY d.billing_period, d.client_id;

-- Query 5B: overall payment-to-invoice allocation coverage (context only).
WITH payment_invoice_links AS (
    SELECT DISTINCT
        p.payment_id,
        CAST(linked_transaction.transaction_id AS STRING) AS invoice_id
    FROM silver_t.quickbooks_payments_t p
    LATERAL VIEW OUTER EXPLODE(p.lines) payment_lines AS payment_line
    LATERAL VIEW OUTER EXPLODE(payment_line.linked_transactions) linked_lines AS linked_transaction
    WHERE LOWER(TRIM(COALESCE(linked_transaction.transaction_type, ''))) = 'invoice'
      AND linked_transaction.transaction_id IS NOT NULL
      AND TRIM(linked_transaction.transaction_id) <> ''
),
per_invoice AS (
    SELECT
        i.invoice_id,
        COUNT(DISTINCT p.payment_id) AS linked_payment_count
    FROM silver_t.quickbooks_invoices_t i
    LEFT JOIN payment_invoice_links p
      ON CAST(p.invoice_id AS STRING) = CAST(i.invoice_id AS STRING)
    GROUP BY i.invoice_id
)
SELECT
    COUNT(*) AS invoice_count,
    SUM(CASE WHEN linked_payment_count = 0 THEN 1 ELSE 0 END) AS invoices_without_explicit_payment_link,
    SUM(CASE WHEN linked_payment_count = 1 THEN 1 ELSE 0 END) AS invoices_with_one_linked_payment,
    SUM(CASE WHEN linked_payment_count > 1 THEN 1 ELSE 0 END) AS invoices_with_multiple_linked_payments,
    MAX(linked_payment_count) AS maximum_linked_payments_per_invoice
FROM per_invoice;
