/*
    Singular dbt test: a selected contract must be valid on the delivery event
    date, and a selected rate must belong to that contract, match service/zone,
    and already be effective by the delivery event date.

    A returned row indicates an invalid enrichment.
*/
WITH model_rows AS (
    SELECT *
    FROM {{ ref('silver_business_lastmile_delivery') }}
),

invalid_contracts AS (
    SELECT
        delivery_id,
        'CONTRACT_OUTSIDE_VALID_DATES' AS issue_type
    FROM model_rows
    WHERE contract_id IS NOT NULL
      AND (
            delivery_event_date IS NULL
            OR contract_start_date IS NULL
            OR delivery_event_date < contract_start_date
            OR (contract_end_date IS NOT NULL AND delivery_event_date > contract_end_date)
      )
),

invalid_rates AS (
    SELECT
        m.delivery_id,
        'RATE_NOT_VALID_FOR_DELIVERY' AS issue_type
    FROM model_rows AS m
    INNER JOIN {{ ref('salesforce_contract_rates_t') }} AS r
        ON r.contract_rate_id = m.contract_rate_id
    WHERE m.contract_rate_id IS NOT NULL
      AND (
            m.contract_id IS NULL
            OR CAST(r.contract_id AS STRING) <> CAST(m.contract_id AS STRING)
            OR m.delivery_event_date IS NULL
            OR r.effective_date IS NULL
            OR CAST(r.effective_date AS DATE) > m.delivery_event_date
            OR LOWER(TRIM(CAST(r.service_type AS STRING))) <> LOWER(TRIM(m.service_type))
            OR (
                COALESCE(CAST(r.is_zone_specific AS BOOLEAN), FALSE) = TRUE
                AND NULLIF(NULLIF(TRIM(CAST(r.zone_id AS STRING)), ''), 'N/A')
                    <> NULLIF(NULLIF(TRIM(m.zone_id), ''), 'N/A')
            )
      )
)

SELECT delivery_id, issue_type FROM invalid_contracts
UNION ALL
SELECT delivery_id, issue_type FROM invalid_rates
