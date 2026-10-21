{{
    config(
        schema = 'gold_last_mile',
        alias = 'fact_delivery'
    )
}}

/*
    Gold Last-Mile — central delivery fact.

    Grain: one row per delivery_id in silver_business_lastmile_delivery.
    This is the ONLY fact table in the Gold Last-Mile mart.

    The view materialization is inherited from dbt_project.yml.

    Design safeguards:
      - Only delivery-grain measures and statuses are exposed as additive/row-level data.
      - Client-month invoice/payment totals, payroll amounts, and driver-level Samsara
        summary totals are deliberately excluded: they repeat at higher grains in Silver.
      - Applicable contract rate is retained as contextual pricing; it is NOT revenue and
        must not be summed across deliveries without a validated rate-unit rule.
      - There is no reliable route ID or historical delivery-to-vehicle/trip relationship
        in the current data. No route/vehicle FK is manufactured.
      - task_vehicle_type is task metadata; it does not prove which physical vehicle ran it.
*/

WITH delivery_source AS (
    SELECT *
    FROM {{ ref('silver_business_lastmile_delivery') }}
),
normalized AS (
    SELECT
        d.*,
        CASE
            WHEN NULLIF(NULLIF(TRIM(CAST(d.account_id AS STRING)), ''), 'N/A') IS NULL THEN 'N/A'
            ELSE TRIM(CAST(d.account_id AS STRING))
        END AS customer_key,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(d.client_id AS STRING)), ''), 'N/A'), 'N/A') AS normalized_client_id,
        CASE
            WHEN NULLIF(NULLIF(TRIM(CAST(d.worker_id AS STRING)), ''), 'N/A') IS NULL THEN 'N/A'
            ELSE TRIM(CAST(d.worker_id AS STRING))
        END AS driver_key,
        CASE
            WHEN NULLIF(NULLIF(TRIM(CAST(d.zone_id AS STRING)), ''), 'N/A') IS NULL THEN 'N/A'
            ELSE TRIM(CAST(d.zone_id AS STRING))
        END AS zone_key,
        CASE
            WHEN NULLIF(NULLIF(LOWER(TRIM(CAST(d.service_type AS STRING))), ''), 'n/a') IS NULL THEN 'N/A'
            ELSE LOWER(TRIM(CAST(d.service_type AS STRING)))
        END AS service_key,
        CASE
            WHEN NULLIF(NULLIF(TRIM(CAST(d.contract_id AS STRING)), ''), 'N/A') IS NULL THEN 'N/A'
            ELSE TRIM(CAST(d.contract_id AS STRING))
        END AS contract_key,
        CASE
            WHEN NULLIF(NULLIF(TRIM(CAST(d.destination_city AS STRING)), ''), 'N/A') IS NULL THEN 'N/A'
            ELSE CONCAT(
                UPPER(TRIM(CAST(d.destination_city AS STRING))), '|',
                COALESCE(NULLIF(NULLIF(UPPER(TRIM(CAST(d.destination_state AS STRING))), ''), 'N/A'), 'N/A'), '|',
                COALESCE(NULLIF(NULLIF(UPPER(TRIM(CAST(d.destination_country AS STRING))), ''), 'N/A'), 'N/A')
            )
        END AS city_key
    FROM delivery_source d
)

SELECT
    /* Delivery identity and dimensional keys */
    CAST(delivery_id AS STRING) AS delivery_id,
    customer_key,
    normalized_client_id AS client_id,
    COALESCE(CAST(date_format(CAST(delivery_event_date AS DATE), 'yyyyMMdd') AS INT), -1) AS delivery_event_date_key,
    COALESCE(CAST(date_format(CAST(task_created_at_utc AS DATE), 'yyyyMMdd') AS INT), -1) AS task_created_date_key,
    COALESCE(CAST(date_format(CAST(completion_time_utc AS DATE), 'yyyyMMdd') AS INT), -1) AS completion_date_key,
    driver_key,
    city_key,
    zone_key,
    service_key,
    contract_key,

    /* Lifecycle and delivery performance */
    CAST(task_status AS STRING) AS task_status,
    CAST(task_state_code AS INT) AS task_state_code,
    CAST(delivery_outcome AS STRING) AS delivery_outcome,
    CAST(is_completed AS BOOLEAN) AS is_completed,
    CAST(is_successful_delivery AS BOOLEAN) AS is_successful_delivery,
    CAST(is_within_completion_window AS BOOLEAN) AS is_within_completion_window,
    CAST(task_created_at_utc AS TIMESTAMP) AS task_created_at_utc,
    CAST(completion_time_utc AS TIMESTAMP) AS completion_time_utc,
    CAST(complete_after_utc AS TIMESTAMP) AS complete_after_utc,
    CAST(complete_before_utc AS TIMESTAMP) AS complete_before_utc,
    CAST(estimated_arrival_time_utc AS TIMESTAMP) AS estimated_arrival_time_utc,
    CAST(estimated_completion_time_utc AS TIMESTAMP) AS estimated_completion_time_utc,
    CAST(completion_failure_reason AS STRING) AS completion_failure_reason,

    /* Additive row count and delivery-level measures */
    CAST(1 AS INT) AS delivery_count,
    TRY_CAST(task_quantity AS BIGINT) AS task_quantity,
    TRY_CAST(service_time AS BIGINT) AS service_time,
    TRY_CAST(order_value AS DECIMAL(18, 2)) AS reported_order_value,
    TRY_CAST(completion_distance AS DOUBLE) AS completion_distance,
    TRY_CAST(failed_scan_count AS BIGINT) AS failed_scan_count,

    /* Attributes that are operationally useful but do not establish a route/vehicle link */
    CAST(task_vehicle_type AS STRING) AS task_vehicle_type,
    CAST(destination_postal_code AS STRING) AS destination_postal_code,
    TRY_CAST(destination_longitude AS DOUBLE) AS destination_longitude,
    TRY_CAST(destination_latitude AS DOUBLE) AS destination_latitude,

    /* Commercial context. A rate is non-additive and is not realized revenue. */
    CAST(contract_rate_id AS STRING) AS contract_rate_id,
    CAST(matched_contract_rate_unit AS STRING) AS matched_contract_rate_unit,
    TRY_CAST(matched_contract_base_rate AS DECIMAL(18, 4)) AS matched_contract_base_rate,
    TRY_CAST(matched_contract_net_rate AS DECIMAL(18, 4)) AS matched_contract_net_rate,
    CAST(matched_contract_rate_effective_date AS DATE) AS matched_contract_rate_effective_date,

    /* Relationship/data-quality statuses for slicing and investigation */
    CAST(account_match_status AS STRING) AS account_match_status,
    CAST(contract_match_status AS STRING) AS contract_match_status,
    CAST(rate_match_status AS STRING) AS rate_match_status,
    CAST(worker_match_status AS STRING) AS worker_match_status,
    CAST(gusto_employee_match_status AS STRING) AS gusto_employee_match_status,
    CAST(samsara_driver_match_status AS STRING) AS samsara_driver_match_status,
    CAST(payroll_period_match_status AS STRING) AS payroll_period_match_status,
    CAST(billing_context_match_status AS STRING) AS billing_context_match_status,
    CAST(payment_activity_context_status AS STRING) AS payment_activity_context_status
FROM normalized
