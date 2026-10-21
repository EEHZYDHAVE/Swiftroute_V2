{{
    config(
        materialized = 'table',
        schema = 'silver_b',
        alias = 'silver_business_lastmile_delivery',
        file_format = 'delta'
    )
}}

/*
    Silver Business: Last-mile delivery

    Grain: one row per non-pickup Onfleet task (the customer-facing delivery task).
    The Onfleet pickupTask flag identified 31,487 pickup tasks in the validation
    run. They remain available in Silver Technical, but are excluded here so they
    do not inflate customer-delivery counts. All 59,144 non-pickup tasks are kept,
    including incomplete tasks; the current validation found 58,645 completed and
    499 not yet completed.

    Important grain safeguards:
      - Delivery identity is task_id from the Onfleet Silver Technical model,
        renamed to delivery_id. source_task_id is not propagated.
      - Contract and rate candidates are date-valid and reduced to a unique match.
      - The data currently has no reliable Onfleet-to-Linnworks order match, so
        Linnworks orders are deliberately not joined.
      - No Samsara vehicle or trip match was found, so trip measures and Samsara
        vehicle attributes are not attached to deliveries.
      - Samsara driver-summary metrics are matched only on a unique normalized
        name on both sides. They are driver-level context, not delivery measures.
      - QuickBooks amounts are client-month context repeated across deliveries.
        Do not sum these fields across delivery rows. Payments are represented by
        explicit-link counts only; payment amounts are not allocated to invoices.
      - Payroll contributes pay-period dates/status only. Payroll amounts are not
        copied onto each delivery.
*/

WITH task_source AS (
    SELECT
        CAST(t.task_id AS STRING) AS delivery_id,
        CAST(t.task_created_at_utc AS TIMESTAMP) AS task_created_at_utc,
        CAST(t.source_updated_at_utc AS TIMESTAMP) AS source_updated_at_utc,
        CAST(t.task_state AS INT) AS task_state_code,
        CAST(t.completion_result AS STRING) AS source_completion_result,
        CAST(t.completion_failure_reason AS STRING) AS completion_failure_reason,
        CAST(t.completion_time_utc AS TIMESTAMP) AS completion_time_utc,
        CAST(t.complete_after_utc AS TIMESTAMP) AS complete_after_utc,
        CAST(t.complete_before_utc AS TIMESTAMP) AS complete_before_utc,
        CAST(t.estimated_arrival_time_utc AS TIMESTAMP) AS estimated_arrival_time_utc,
        CAST(t.estimated_completion_time_utc AS TIMESTAMP) AS estimated_completion_time_utc,
        CAST(t.pickup_task AS BOOLEAN) AS pickup_task,
        CAST(t.worker_id AS STRING) AS source_worker_id,
        COALESCE(CAST(t.worker_reference_missing AS BOOLEAN), FALSE) AS worker_reference_missing,
        NULLIF(NULLIF(TRIM(CAST(t.client_id AS STRING)), ''), 'N/A') AS client_id_key,
        NULLIF(NULLIF(TRIM(CAST(t.service_type AS STRING)), ''), 'N/A') AS service_type_key,
        NULLIF(NULLIF(TRIM(CAST(t.zone_id AS STRING)), ''), 'N/A') AS zone_id_key,
        NULLIF(NULLIF(TRIM(CAST(t.vehicle_type AS STRING)), ''), 'N/A') AS task_vehicle_type,
        TRY_CAST(t.quantity AS BIGINT) AS task_quantity,
        TRY_CAST(t.service_time AS BIGINT) AS service_time,
        TRY_CAST(t.order_value AS DECIMAL(18, 2)) AS order_value,
        CAST(t.destination_id AS STRING) AS destination_id,
        TRY_CAST(t.longitude AS DOUBLE) AS destination_longitude,
        TRY_CAST(t.latitude AS DOUBLE) AS destination_latitude,
        CAST(t.destination_city AS STRING) AS destination_city,
        CAST(t.destination_state AS STRING) AS destination_state,
        CAST(t.destination_postal_code AS STRING) AS destination_postal_code,
        CAST(t.destination_country AS STRING) AS destination_country,
        TRY_CAST(t.completion_distance AS DOUBLE) AS completion_distance,
        TRY_CAST(t.failed_scan_count AS BIGINT) AS failed_scan_count,
        CAST(t.organization_name AS STRING) AS organization_name
    FROM {{ ref('onfleet_deliveries_t') }} AS t
    WHERE COALESCE(CAST(t.pickup_task AS BOOLEAN), FALSE) = FALSE
),

delivery_base AS (
    SELECT
        task_source.*,
        COALESCE(completion_time_utc, task_created_at_utc) AS delivery_event_at_utc,
        CAST(COALESCE(completion_time_utc, task_created_at_utc) AS DATE) AS delivery_event_date,
        DATE_FORMAT(CAST(COALESCE(completion_time_utc, task_created_at_utc) AS DATE), 'yyyy-MM') AS delivery_billing_period,
        LOWER(TRIM(source_completion_result)) AS normalized_completion_result,
        CASE
            WHEN source_worker_id IS NULL
              OR TRIM(source_worker_id) = ''
              OR UPPER(TRIM(source_worker_id)) = 'N/A'
            THEN NULL
            ELSE TRIM(source_worker_id)
        END AS worker_id_key
    FROM task_source AS task_source
),

/* Match a client only when there is exactly one Salesforce account for client_id. */
account_candidates AS (
    SELECT
        CAST(a.client_id AS STRING) AS client_id_key,
        CAST(a.account_id AS STRING) AS account_id,
        CAST(a.account_name AS STRING) AS account_name,
        CAST(a.account_type AS STRING) AS account_type,
        CAST(a.industry AS STRING) AS industry,
        CAST(a.account_tier AS STRING) AS account_tier,
        CAST(a.primary_city AS STRING) AS account_primary_city,
        CAST(a.contract_type AS STRING) AS account_contract_type,
        CAST(a.is_fulfillment_client AS BOOLEAN) AS is_fulfillment_client,
        TRY_CAST(a.net_payment_terms AS INT) AS account_net_payment_terms
    FROM {{ ref('salesforce_accounts_t') }} AS a
    WHERE a.client_id IS NOT NULL
      AND TRIM(CAST(a.client_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(a.client_id AS STRING))) <> 'N/A'
),

account_candidate_counts AS (
    SELECT client_id_key, COUNT(DISTINCT account_id) AS account_match_count
    FROM account_candidates
    GROUP BY client_id_key
),

account_one_row AS (
    SELECT *
    FROM (
        SELECT
            account_candidates.*,
            ROW_NUMBER() OVER (
                PARTITION BY client_id_key
                ORDER BY account_id
            ) AS account_row_number
        FROM account_candidates
    ) ranked_accounts
    WHERE account_row_number = 1
),

/* Select only a single contract valid on the delivery event date. */
contract_candidates AS (
    SELECT DISTINCT
        d.delivery_id,
        CAST(c.contract_id AS STRING) AS contract_id,
        CAST(c.contract_status AS STRING) AS contract_status,
        CAST(c.start_date AS DATE) AS contract_start_date,
        CAST(c.end_date AS DATE) AS contract_end_date,
        CAST(c.contract_type AS STRING) AS contract_type,
        TRY_CAST(c.committed_monthly_volume AS BIGINT) AS committed_monthly_volume,
        TRY_CAST(c.net_payment_terms AS INT) AS contract_net_payment_terms,
        CAST(c.account_tier AS STRING) AS contract_account_tier,
        CAST(c.primary_city AS STRING) AS contract_primary_city
    FROM delivery_base AS d
    INNER JOIN {{ ref('salesforce_contracts_t') }} AS c
        ON d.client_id_key = NULLIF(NULLIF(TRIM(CAST(c.client_id AS STRING)), ''), 'N/A')
       AND d.delivery_event_date IS NOT NULL
       AND c.start_date IS NOT NULL
       AND d.delivery_event_date >= CAST(c.start_date AS DATE)
       AND (c.end_date IS NULL OR d.delivery_event_date <= CAST(c.end_date AS DATE))
),

contract_candidate_counts AS (
    SELECT delivery_id, COUNT(DISTINCT contract_id) AS contract_match_count
    FROM contract_candidates
    GROUP BY delivery_id
),

contract_ranked AS (
    SELECT
        contract_candidates.*,
        ROW_NUMBER() OVER (
            PARTITION BY delivery_id
            ORDER BY contract_start_date DESC, contract_id ASC
        ) AS contract_row_number
    FROM contract_candidates
),

contract_one_row AS (
    SELECT c.*
    FROM contract_ranked AS c
    INNER JOIN contract_candidate_counts AS cc
        ON cc.delivery_id = c.delivery_id
       AND cc.contract_match_count = 1
    WHERE c.contract_row_number = 1
),

/* Find effective service/zone rates for the uniquely matched contract. */
rate_candidates AS (
    SELECT DISTINCT
        d.delivery_id,
        CAST(r.contract_rate_id AS STRING) AS contract_rate_id,
        CAST(r.contract_id AS STRING) AS rate_contract_id,
        LOWER(TRIM(CAST(r.service_type AS STRING))) AS rate_service_type,
        NULLIF(NULLIF(TRIM(CAST(r.zone_id AS STRING)), ''), 'N/A') AS rate_zone_id,
        CAST(r.is_zone_specific AS BOOLEAN) AS is_zone_specific,
        TRY_CAST(r.base_rate AS DECIMAL(18, 4)) AS base_rate,
        TRY_CAST(r.net_rate AS DECIMAL(18, 4)) AS net_rate,
        CAST(r.rate_unit AS STRING) AS rate_unit,
        CAST(r.effective_date AS DATE) AS rate_effective_date
    FROM delivery_base AS d
    INNER JOIN contract_one_row AS c
        ON c.delivery_id = d.delivery_id
    INNER JOIN {{ ref('salesforce_contract_rates_t') }} AS r
        ON CAST(r.contract_id AS STRING) = c.contract_id
       AND d.service_type_key IS NOT NULL
       AND LOWER(TRIM(CAST(r.service_type AS STRING))) = LOWER(TRIM(d.service_type_key))
       AND r.effective_date IS NOT NULL
       AND d.delivery_event_date >= CAST(r.effective_date AS DATE)
       AND (
            CAST(r.is_zone_specific AS BOOLEAN) = FALSE
            OR (
                CAST(r.is_zone_specific AS BOOLEAN) = TRUE
                AND d.zone_id_key IS NOT NULL
                AND NULLIF(NULLIF(TRIM(CAST(r.zone_id AS STRING)), ''), 'N/A') = d.zone_id_key
            )
       )
),

rate_candidates_with_latest AS (
    SELECT
        rate_candidates.*,
        MAX(rate_effective_date) OVER (PARTITION BY delivery_id) AS latest_effective_date
    FROM rate_candidates
),

rate_candidate_counts AS (
    SELECT
        delivery_id,
        COUNT(*) AS rate_candidate_count,
        SUM(CASE WHEN rate_effective_date = latest_effective_date THEN 1 ELSE 0 END) AS latest_rate_match_count
    FROM rate_candidates_with_latest
    GROUP BY delivery_id
),

rate_ranked AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY delivery_id
            ORDER BY rate_effective_date DESC, contract_rate_id ASC
        ) AS rate_row_number
    FROM rate_candidates_with_latest AS r
),

rate_one_row AS (
    SELECT r.*
    FROM rate_ranked AS r
    INNER JOIN rate_candidate_counts AS rc
        ON rc.delivery_id = r.delivery_id
       AND rc.latest_rate_match_count = 1
    WHERE r.rate_row_number = 1
),

/* Onfleet worker details are the reliable direct driver bridge. */
worker_candidates AS (
    SELECT DISTINCT
        CAST(w.worker_id AS STRING) AS worker_id,
        CAST(w.worker_name AS STRING) AS worker_name,
        CAST(w.display_name AS STRING) AS worker_display_name,
        CAST(w.employment_type AS STRING) AS worker_employment_type,
        CAST(w.vehicle_id AS STRING) AS onfleet_vehicle_id,
        CAST(w.vehicle_type AS STRING) AS onfleet_vehicle_type,
        CAST(w.vehicle_description AS STRING) AS onfleet_vehicle_description,
        CAST(w.vehicle_license_plate AS STRING) AS onfleet_vehicle_license_plate,
        CAST(w.vehicle_color AS STRING) AS onfleet_vehicle_color
    FROM {{ ref('onfleet_workers_t') }} AS w
),

worker_name_profiles AS (
    SELECT
        worker_candidates.*,
        LOWER(TRIM(COALESCE(
            NULLIF(NULLIF(TRIM(worker_name), ''), 'N/A'),
            NULLIF(NULLIF(TRIM(worker_display_name), ''), 'N/A'),
            ''
        ))) AS normalized_worker_name,
        COUNT(*) OVER (
            PARTITION BY LOWER(TRIM(COALESCE(
                NULLIF(NULLIF(TRIM(worker_name), ''), 'N/A'),
                NULLIF(NULLIF(TRIM(worker_display_name), ''), 'N/A'),
                ''
            )))
        ) AS workers_with_same_name
    FROM worker_candidates
),

samsara_driver_profiles AS (
    SELECT
        CAST(s.driver_id AS STRING) AS samsara_driver_id,
        CAST(s.driver_name AS STRING) AS samsara_driver_name,
        LOWER(TRIM(CAST(s.driver_name AS STRING))) AS normalized_samsara_driver_name,
        CAST(s.city AS STRING) AS samsara_driver_city,
        TRY_CAST(s.total_distance_meters AS DECIMAL(20, 2)) AS samsara_driver_summary_total_distance_meters,
        TRY_CAST(s.total_drive_time_ms AS BIGINT) AS samsara_driver_summary_total_drive_time_ms,
        TRY_CAST(s.total_idle_time_ms AS BIGINT) AS samsara_driver_summary_total_idle_time_ms,
        TRY_CAST(s.total_trips AS BIGINT) AS samsara_driver_summary_total_trips,
        TRY_CAST(s.speeding_count AS BIGINT) AS samsara_driver_summary_speeding_count,
        TRY_CAST(s.harsh_braking_count AS BIGINT) AS samsara_driver_summary_harsh_braking_count,
        TRY_CAST(s.harsh_acceleration_count AS BIGINT) AS samsara_driver_summary_harsh_acceleration_count,
        TRY_CAST(s.safety_score AS DECIMAL(6, 2)) AS samsara_driver_summary_safety_score,
        TRY_CAST(s.hos_violation_count AS INT) AS samsara_driver_summary_hos_violation_count,
        CAST(s.has_hos_violations AS BOOLEAN) AS samsara_driver_summary_has_hos_violations,
        COUNT(*) OVER (PARTITION BY LOWER(TRIM(CAST(s.driver_name AS STRING)))) AS samsara_rows_with_same_name
    FROM {{ ref('samsara_driver_summary_t') }} AS s
    WHERE s.driver_name IS NOT NULL
      AND TRIM(CAST(s.driver_name AS STRING)) <> ''
),

samsara_driver_match_candidates AS (
    SELECT
        w.worker_id,
        w.normalized_worker_name,
        w.workers_with_same_name,
        s.*
    FROM worker_name_profiles AS w
    INNER JOIN samsara_driver_profiles AS s
        ON w.normalized_worker_name = s.normalized_samsara_driver_name
       AND w.normalized_worker_name <> ''
),

samsara_match_counts AS (
    SELECT worker_id, COUNT(DISTINCT samsara_driver_id) AS samsara_candidate_count
    FROM samsara_driver_match_candidates
    GROUP BY worker_id
),

samsara_driver_one_row AS (
    SELECT c.*
    FROM samsara_driver_match_candidates AS c
    INNER JOIN samsara_match_counts AS mc
        ON mc.worker_id = c.worker_id
       AND mc.samsara_candidate_count = 1
    WHERE c.workers_with_same_name = 1
      AND c.samsara_rows_with_same_name = 1
),

/* Gusto's explicit Onfleet Worker ID custom field is the employee mapping. */
employee_candidates AS (
    SELECT
        CAST(e.employee_uuid AS STRING) AS employee_uuid,
        NULLIF(NULLIF(TRIM(CAST(e.onfleet_worker_id AS STRING)), ''), 'N/A') AS onfleet_worker_id,
        CAST(e.employment_status AS STRING) AS employee_employment_status,
        TRY_CAST(e.hire_date AS DATE) AS employee_hire_date,
        TRY_CAST(e.termination_date AS DATE) AS employee_termination_date,
        CAST(e.department AS STRING) AS employee_department,
        CAST(e.job_title AS STRING) AS employee_job_title,
        CAST(e.is_driver AS BOOLEAN) AS employee_is_driver
    FROM {{ ref('gusto_employees_t') }} AS e
    WHERE e.employee_uuid IS NOT NULL
),

employee_candidate_counts AS (
    SELECT onfleet_worker_id, COUNT(DISTINCT employee_uuid) AS employee_match_count
    FROM employee_candidates
    WHERE onfleet_worker_id IS NOT NULL
    GROUP BY onfleet_worker_id
),

employee_ranked AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY onfleet_worker_id
            ORDER BY employee_uuid
        ) AS employee_row_number
    FROM employee_candidates AS e
    WHERE onfleet_worker_id IS NOT NULL
),

employee_one_row AS (
    SELECT e.*
    FROM employee_ranked AS e
    INNER JOIN employee_candidate_counts AS ec
        ON ec.onfleet_worker_id = e.onfleet_worker_id
       AND ec.employee_match_count = 1
    WHERE e.employee_row_number = 1
),

/* Reduce payroll rows to employee/pay-period grain before matching by delivery date. */
payroll_periods AS (
    SELECT
        CAST(p.employee_uuid AS STRING) AS employee_uuid,
        CAST(p.source_payroll_id AS STRING) AS source_payroll_id,
        CAST(p.payroll_version AS STRING) AS payroll_version,
        CAST(p.pay_period_start_date AS DATE) AS pay_period_start_date,
        CAST(p.pay_period_end_date AS DATE) AS pay_period_end_date,
        MAX(CAST(p.check_date AS DATE)) AS check_date,
        MAX(CASE WHEN CAST(p.is_processed AS BOOLEAN) THEN 1 ELSE 0 END) AS is_processed_int,
        COUNT(DISTINCT p.payroll_compensation_id) AS compensation_entry_count
    FROM {{ ref('gusto_payroll_compensations_t') }} AS p
    WHERE p.employee_uuid IS NOT NULL
      AND p.pay_period_start_date IS NOT NULL
      AND p.pay_period_end_date IS NOT NULL
    GROUP BY
        CAST(p.employee_uuid AS STRING),
        CAST(p.source_payroll_id AS STRING),
        CAST(p.payroll_version AS STRING),
        CAST(p.pay_period_start_date AS DATE),
        CAST(p.pay_period_end_date AS DATE)
),

payroll_candidates AS (
    SELECT
        d.delivery_id,
        p.source_payroll_id,
        p.payroll_version,
        p.pay_period_start_date,
        p.pay_period_end_date,
        p.check_date,
        CAST(p.is_processed_int = 1 AS BOOLEAN) AS payroll_is_processed,
        p.compensation_entry_count,
        COUNT(*) OVER (PARTITION BY d.delivery_id) AS payroll_period_match_count,
        ROW_NUMBER() OVER (
            PARTITION BY d.delivery_id
            ORDER BY p.check_date DESC NULLS LAST, p.pay_period_end_date DESC, p.source_payroll_id
        ) AS payroll_match_row_number
    FROM delivery_base AS d
    INNER JOIN employee_one_row AS e
        ON e.onfleet_worker_id = d.worker_id_key
    INNER JOIN payroll_periods AS p
        ON p.employee_uuid = e.employee_uuid
       AND d.delivery_event_date >= p.pay_period_start_date
       AND d.delivery_event_date <= p.pay_period_end_date
),

payroll_candidate_counts AS (
    SELECT delivery_id, MAX(payroll_period_match_count) AS payroll_period_match_count
    FROM payroll_candidates
    GROUP BY delivery_id
),

payroll_one_row AS (
    SELECT *
    FROM payroll_candidates
    WHERE payroll_match_row_number = 1
      AND payroll_period_match_count = 1
),

/* Explicit payment-to-invoice references only; no payment amount allocation. */
payment_invoice_links AS (
    SELECT DISTINCT
        CAST(p.payment_id AS STRING) AS payment_id,
        CAST(linked_transaction.transaction_id AS STRING) AS invoice_id
    FROM {{ ref('quickbooks_payments_t') }} AS p
    LATERAL VIEW OUTER EXPLODE(p.lines) payment_lines AS payment_line
    LATERAL VIEW OUTER EXPLODE(payment_line.linked_transactions) linked_transactions AS linked_transaction
    WHERE LOWER(TRIM(CAST(linked_transaction.transaction_type AS STRING))) = 'invoice'
      AND linked_transaction.transaction_id IS NOT NULL
),

invoice_month_financials AS (
    SELECT
        NULLIF(NULLIF(TRIM(CAST(i.client_id AS STRING)), ''), 'N/A') AS client_id_key,
        TRIM(CAST(i.billing_period AS STRING)) AS billing_period,
        COUNT(DISTINCT i.invoice_id) AS client_month_invoice_count,
        SUM(TRY_CAST(i.total_amount AS DECIMAL(18, 2))) AS client_month_invoice_total_amount,
        SUM(TRY_CAST(i.balance_amount AS DECIMAL(18, 2))) AS client_month_invoice_balance_amount
    FROM {{ ref('quickbooks_invoices_t') }} AS i
    WHERE i.client_id IS NOT NULL
      AND TRIM(CAST(i.client_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(i.client_id AS STRING))) <> 'N/A'
      AND i.billing_period IS NOT NULL
      AND TRIM(CAST(i.billing_period AS STRING)) RLIKE '^[0-9]{4}-[0-9]{2}$'
    GROUP BY
        NULLIF(NULLIF(TRIM(CAST(i.client_id AS STRING)), ''), 'N/A'),
        TRIM(CAST(i.billing_period AS STRING))
),

invoice_month_payment_coverage AS (
    SELECT
        NULLIF(NULLIF(TRIM(CAST(i.client_id AS STRING)), ''), 'N/A') AS client_id_key,
        TRIM(CAST(i.billing_period AS STRING)) AS billing_period,
        COUNT(DISTINCT CASE WHEN p.invoice_id IS NOT NULL THEN i.invoice_id END)
            AS client_month_invoices_with_explicit_payment_link,
        COUNT(DISTINCT p.payment_id) AS client_month_unique_explicitly_linked_payment_count
    FROM {{ ref('quickbooks_invoices_t') }} AS i
    LEFT JOIN payment_invoice_links AS p
        ON CAST(i.invoice_id AS STRING) = p.invoice_id
    WHERE i.client_id IS NOT NULL
      AND TRIM(CAST(i.client_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(i.client_id AS STRING))) <> 'N/A'
      AND i.billing_period IS NOT NULL
      AND TRIM(CAST(i.billing_period AS STRING)) RLIKE '^[0-9]{4}-[0-9]{2}$'
    GROUP BY
        NULLIF(NULLIF(TRIM(CAST(i.client_id AS STRING)), ''), 'N/A'),
        TRIM(CAST(i.billing_period AS STRING))
),

client_month_billing AS (
    SELECT
        f.client_id_key,
        f.billing_period,
        f.client_month_invoice_count,
        f.client_month_invoice_total_amount,
        f.client_month_invoice_balance_amount,
        COALESCE(p.client_month_invoices_with_explicit_payment_link, 0)
            AS client_month_invoices_with_explicit_payment_link,
        COALESCE(f.client_month_invoice_count, 0)
          - COALESCE(p.client_month_invoices_with_explicit_payment_link, 0)
            AS client_month_invoices_without_explicit_payment_link,
        COALESCE(p.client_month_unique_explicitly_linked_payment_count, 0)
            AS client_month_unique_explicitly_linked_payment_count
    FROM invoice_month_financials AS f
    LEFT JOIN invoice_month_payment_coverage AS p
        ON p.client_id_key = f.client_id_key
       AND p.billing_period = f.billing_period
),

/* Payment receipts are grouped by their own transaction month, not allocated to invoices. */
client_month_cash_receipts AS (
    SELECT
        NULLIF(NULLIF(TRIM(CAST(p.client_id AS STRING)), ''), 'N/A') AS client_id_key,
        DATE_FORMAT(CAST(p.transaction_date AS DATE), 'yyyy-MM') AS payment_transaction_month,
        COUNT(DISTINCT p.payment_id) AS client_month_payment_transaction_count_by_payment_date,
        SUM(TRY_CAST(p.total_amount AS DECIMAL(18, 2))) AS client_month_payment_receipts_total_amount_by_payment_date
    FROM {{ ref('quickbooks_payments_t') }} AS p
    WHERE p.client_id IS NOT NULL
      AND TRIM(CAST(p.client_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(p.client_id AS STRING))) <> 'N/A'
      AND p.transaction_date IS NOT NULL
    GROUP BY
        NULLIF(NULLIF(TRIM(CAST(p.client_id AS STRING)), ''), 'N/A'),
        DATE_FORMAT(CAST(p.transaction_date AS DATE), 'yyyy-MM')
)

SELECT
    /* Delivery identity, timestamps and lifecycle */
    d.delivery_id,
    d.organization_name,
    d.task_created_at_utc,
    d.source_updated_at_utc,
    d.delivery_event_at_utc,
    d.delivery_event_date,
    d.delivery_billing_period,
    d.task_state_code,
    CASE d.task_state_code
        WHEN 0 THEN 'UNASSIGNED'
        WHEN 1 THEN 'ASSIGNED'
        WHEN 2 THEN 'ACTIVE'
        WHEN 3 THEN 'COMPLETED'
        ELSE 'UNKNOWN'
    END AS task_status,
    CASE
        WHEN d.normalized_completion_result = 'success' THEN 'SUCCESS'
        WHEN d.normalized_completion_result = 'failure' THEN 'FAILURE'
        WHEN d.task_state_code = 3 THEN 'COMPLETED_OUTCOME_UNKNOWN'
        ELSE 'INCOMPLETE'
    END AS delivery_outcome,
    CASE WHEN d.task_state_code = 3 THEN TRUE ELSE FALSE END AS is_completed,
    CASE
        WHEN d.normalized_completion_result = 'success' THEN TRUE
        WHEN d.normalized_completion_result = 'failure' THEN FALSE
        ELSE CAST(NULL AS BOOLEAN)
    END AS is_successful_delivery,
    d.completion_time_utc,
    d.complete_after_utc,
    d.complete_before_utc,
    d.estimated_arrival_time_utc,
    d.estimated_completion_time_utc,
    CASE
        WHEN d.normalized_completion_result = 'failure' THEN FALSE
        WHEN d.normalized_completion_result = 'success'
         AND d.completion_time_utc IS NOT NULL
         AND d.complete_before_utc IS NOT NULL
            THEN d.completion_time_utc <= d.complete_before_utc
        ELSE CAST(NULL AS BOOLEAN)
    END AS is_within_completion_window,
    d.completion_failure_reason,
    d.completion_distance,
    d.failed_scan_count,

    /* Client and Salesforce account context */
    d.client_id_key AS client_id,
    a.account_id,
    a.account_name,
    a.account_type,
    a.industry AS account_industry,
    a.account_tier,
    a.account_primary_city,
    a.account_contract_type,
    a.is_fulfillment_client,
    a.account_net_payment_terms,
    CASE
        WHEN d.client_id_key IS NULL THEN 'MISSING_CLIENT_ID'
        WHEN COALESCE(acc.account_match_count, 0) = 0 THEN 'ACCOUNT_NOT_FOUND'
        WHEN acc.account_match_count > 1 THEN 'AMBIGUOUS_ACCOUNT_MATCH'
        ELSE 'ACCOUNT_MATCHED'
    END AS account_match_status,

    /* Commercial contract and applicable rate context */
    c.contract_id,
    c.contract_status,
    c.contract_start_date,
    c.contract_end_date,
    c.contract_type,
    c.committed_monthly_volume,
    c.contract_net_payment_terms,
    c.contract_account_tier,
    c.contract_primary_city,
    CASE
        WHEN d.client_id_key IS NULL THEN 'MISSING_CLIENT_ID'
        WHEN d.delivery_event_date IS NULL THEN 'MISSING_EVENT_DATE'
        WHEN COALESCE(cc.contract_match_count, 0) = 0 THEN 'NO_DATE_VALID_CONTRACT'
        WHEN cc.contract_match_count > 1 THEN 'AMBIGUOUS_CONTRACT_MATCH'
        ELSE 'CONTRACT_MATCHED'
    END AS contract_match_status,
    r.contract_rate_id,
    r.rate_service_type AS matched_rate_service_type,
    r.rate_zone_id AS matched_rate_zone_id,
    r.is_zone_specific AS matched_rate_is_zone_specific,
    r.base_rate AS matched_contract_base_rate,
    r.net_rate AS matched_contract_net_rate,
    r.rate_unit AS matched_contract_rate_unit,
    r.rate_effective_date AS matched_contract_rate_effective_date,
    CASE
        WHEN c.contract_id IS NULL THEN 'NO_UNIQUE_DATE_VALID_CONTRACT'
        WHEN d.service_type_key IS NULL THEN 'MISSING_SERVICE_TYPE'
        WHEN COALESCE(rc.rate_candidate_count, 0) = 0 THEN 'NO_APPLICABLE_RATE'
        WHEN COALESCE(rc.latest_rate_match_count, 0) > 1 THEN 'AMBIGUOUS_LATEST_RATE'
        WHEN r.contract_rate_id IS NOT NULL THEN 'RATE_MATCHED'
        ELSE 'RATE_UNRESOLVED'
    END AS rate_match_status,

    /* Delivery/service attributes */
    d.service_type_key AS service_type,
    d.zone_id_key AS zone_id,
    d.task_vehicle_type AS task_vehicle_type,
    d.task_quantity,
    d.service_time,
    d.order_value,
    d.destination_id,
    d.destination_city,
    d.destination_state,
    d.destination_postal_code,
    d.destination_country,
    d.destination_longitude,
    d.destination_latitude,

    /* Assigned Onfleet worker and its current vehicle profile */
    CASE WHEN wc.worker_id IS NOT NULL THEN d.worker_id_key ELSE CAST(NULL AS STRING) END AS worker_id,
    wc.worker_name AS driver_name,
    wc.worker_display_name AS driver_display_name,
    wc.worker_employment_type AS onfleet_employment_type,
    NULLIF(NULLIF(TRIM(wc.onfleet_vehicle_id), ''), 'N/A') AS onfleet_vehicle_id,
    NULLIF(NULLIF(TRIM(wc.onfleet_vehicle_type), ''), 'N/A') AS onfleet_vehicle_type,
    NULLIF(NULLIF(TRIM(wc.onfleet_vehicle_description), ''), 'N/A') AS onfleet_vehicle_description,
    NULLIF(NULLIF(TRIM(wc.onfleet_vehicle_license_plate), ''), 'N/A') AS onfleet_vehicle_license_plate,
    CASE
        WHEN d.worker_id_key IS NULL THEN 'MISSING_WORKER_REFERENCE'
        WHEN wc.worker_id IS NULL THEN 'WORKER_NOT_FOUND'
        ELSE 'WORKER_MATCHED'
    END AS worker_match_status,

    /* Gusto employee attributes from the explicit Onfleet Worker ID mapping */
    e.employee_uuid AS gusto_employee_uuid,
    e.employee_employment_status AS gusto_employment_status,
    e.employee_hire_date AS gusto_hire_date,
    e.employee_termination_date AS gusto_termination_date,
    e.employee_department AS gusto_department,
    e.employee_job_title AS gusto_job_title,
    e.employee_is_driver AS gusto_is_driver,
    CASE
        WHEN wc.worker_id IS NULL THEN 'NO_ONFLEET_WORKER'
        WHEN COALESCE(ec.employee_match_count, 0) = 0 THEN 'NO_GUSTO_EMPLOYEE_MATCH'
        WHEN ec.employee_match_count > 1 THEN 'AMBIGUOUS_GUSTO_EMPLOYEE_MATCH'
        ELSE 'GUSTO_EMPLOYEE_MATCHED'
    END AS gusto_employee_match_status,

    /* Unique Samsara driver-name context only; not delivery-level measures. */
    sd.samsara_driver_id,
    sd.samsara_driver_city,
    sd.samsara_driver_summary_total_distance_meters,
    sd.samsara_driver_summary_total_drive_time_ms,
    sd.samsara_driver_summary_total_idle_time_ms,
    sd.samsara_driver_summary_total_trips,
    sd.samsara_driver_summary_speeding_count,
    sd.samsara_driver_summary_harsh_braking_count,
    sd.samsara_driver_summary_harsh_acceleration_count,
    sd.samsara_driver_summary_safety_score,
    sd.samsara_driver_summary_hos_violation_count,
    sd.samsara_driver_summary_has_hos_violations,
    CASE
        WHEN wc.worker_id IS NULL THEN 'NO_ONFLEET_WORKER'
        WHEN wn.normalized_worker_name IS NULL OR wn.normalized_worker_name = '' THEN 'NO_WORKER_NAME'
        WHEN COALESCE(sm.samsara_candidate_count, 0) = 0 THEN 'NO_SAMSARA_NAME_MATCH'
        WHEN sm.samsara_candidate_count > 1
          OR wn.workers_with_same_name > 1
          OR sd.samsara_rows_with_same_name > 1 THEN 'AMBIGUOUS_SAMSARA_NAME_MATCH'
        WHEN sd.samsara_driver_id IS NOT NULL THEN 'SAMSARA_DRIVER_SUMMARY_MATCHED'
        ELSE 'SAMSARA_MATCH_NOT_RESOLVED'
    END AS samsara_driver_match_status,

    /* Employee/pay-period context only. No payroll amounts are attached. */
    pp.source_payroll_id,
    pp.payroll_version,
    pp.pay_period_start_date,
    pp.pay_period_end_date,
    pp.check_date AS payroll_check_date,
    pp.payroll_is_processed,
    pp.compensation_entry_count AS payroll_compensation_entry_count,
    CASE
        WHEN e.employee_uuid IS NULL THEN 'NO_EMPLOYEE_MAPPING'
        WHEN d.delivery_event_date IS NULL THEN 'NO_EVENT_DATE'
        WHEN COALESCE(pc.payroll_period_match_count, 0) = 0 THEN 'NO_OVERLAPPING_PAY_PERIOD'
        WHEN pc.payroll_period_match_count > 1 THEN 'AMBIGUOUS_PAYROLL_PERIOD_MATCH'
        ELSE 'PAYROLL_PERIOD_MATCHED'
    END AS payroll_period_match_status,

    /* QuickBooks client-month context: amount fields repeat for that client/month. */
    b.billing_period AS invoice_billing_period,
    COALESCE(b.client_month_invoice_count, 0) AS client_month_invoice_count,
    b.client_month_invoice_total_amount,
    b.client_month_invoice_balance_amount,
    COALESCE(b.client_month_invoices_with_explicit_payment_link, 0)
        AS client_month_invoices_with_explicit_payment_link,
    COALESCE(b.client_month_invoices_without_explicit_payment_link, 0)
        AS client_month_invoices_without_explicit_payment_link,
    COALESCE(b.client_month_unique_explicitly_linked_payment_count, 0)
        AS client_month_unique_explicitly_linked_payment_count,
    COALESCE(pcr.client_month_payment_transaction_count_by_payment_date, 0)
        AS client_month_payment_transaction_count_by_payment_date,
    pcr.client_month_payment_receipts_total_amount_by_payment_date,
    CASE
        WHEN d.client_id_key IS NULL THEN 'MISSING_CLIENT_ID'
        WHEN b.client_id_key IS NULL THEN 'NO_INVOICE_FOR_CLIENT_MONTH'
        ELSE 'CLIENT_MONTH_INVOICE_CONTEXT_MATCHED'
    END AS billing_context_match_status,
    CASE
        WHEN d.client_id_key IS NULL THEN 'MISSING_CLIENT_ID'
        WHEN pcr.client_id_key IS NULL THEN 'NO_PAYMENT_RECEIPT_IN_CLIENT_MONTH'
        ELSE 'CLIENT_MONTH_PAYMENT_ACTIVITY_MATCHED'
    END AS payment_activity_context_status,

    CURRENT_TIMESTAMP() AS processed_at_utc

FROM delivery_base AS d
LEFT JOIN account_candidate_counts AS acc
    ON acc.client_id_key = d.client_id_key
LEFT JOIN account_one_row AS a
    ON a.client_id_key = d.client_id_key
   AND COALESCE(acc.account_match_count, 0) = 1
LEFT JOIN contract_candidate_counts AS cc
    ON cc.delivery_id = d.delivery_id
LEFT JOIN contract_one_row AS c
    ON c.delivery_id = d.delivery_id
LEFT JOIN rate_candidate_counts AS rc
    ON rc.delivery_id = d.delivery_id
LEFT JOIN rate_one_row AS r
    ON r.delivery_id = d.delivery_id
LEFT JOIN worker_candidates AS wc
    ON wc.worker_id = d.worker_id_key
LEFT JOIN worker_name_profiles AS wn
    ON wn.worker_id = d.worker_id_key
LEFT JOIN employee_candidate_counts AS ec
    ON ec.onfleet_worker_id = d.worker_id_key
LEFT JOIN employee_one_row AS e
    ON e.onfleet_worker_id = d.worker_id_key
LEFT JOIN samsara_driver_one_row AS sd
    ON sd.worker_id = d.worker_id_key
LEFT JOIN samsara_match_counts AS sm
    ON sm.worker_id = d.worker_id_key
LEFT JOIN payroll_one_row AS pp
    ON pp.delivery_id = d.delivery_id
LEFT JOIN payroll_candidate_counts AS pc
    ON pc.delivery_id = d.delivery_id
LEFT JOIN client_month_billing AS b
    ON b.client_id_key = d.client_id_key
   AND b.billing_period = d.delivery_billing_period
LEFT JOIN client_month_cash_receipts AS pcr
    ON pcr.client_id_key = d.client_id_key
   AND pcr.payment_transaction_month = d.delivery_billing_period
