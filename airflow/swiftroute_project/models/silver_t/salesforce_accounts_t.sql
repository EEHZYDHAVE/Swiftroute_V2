{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'source_account_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Salesforce Accounts

    Grain: one row per Salesforce Account record.

    Keys:
      - account_id: readable, persistent surrogate used by other Silver Salesforce models.
      - source_account_id: original Salesforce Account.Id, retained for traceability.
      - client_id: SwiftRoute's shared customer identifier; use this to connect this
        customer to QuickBooks and operational systems. It is not the Salesforce PK.

    Incremental watermark: Salesforce LastModifiedDate. LastActivityDate is retained
    as a business date but is not used as a freshness or engagement watermark because
    formal CRM activity is incompletely logged in this simulation.

    Address and Owner compound objects are flattened. Salesforce attributes metadata
    and Bronze audit columns are intentionally not emitted.
*/

WITH bronze_accounts AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'salesforce_accounts') }}
),

source_account_rows AS (
    SELECT
        CAST(id AS STRING) AS source_account_id,
        CAST(name AS STRING) AS account_name,
        CAST(account_type AS STRING) AS account_type,
        CAST(industry AS STRING) AS industry,
        CAST(phone AS STRING) AS phone,
        CAST(website AS STRING) AS website,
        CAST(billing_address.street AS STRING) AS billing_street,
        CAST(billing_address.city AS STRING) AS billing_city,
        CAST(billing_address.state AS STRING) AS billing_state,
        CAST(billing_address.postal_code AS STRING) AS billing_postal_code,
        CAST(billing_address.country AS STRING) AS billing_country,
        CAST(billing_address.state_code AS STRING) AS billing_state_code,
        CAST(billing_address.country_code AS STRING) AS billing_country_code,
        TRY_CAST(annual_revenue AS DECIMAL(18, 2)) AS annual_revenue,
        TRY_CAST(number_of_employees AS INT) AS number_of_employees,
        TRY_CAST(last_activity_date AS DATE) AS last_activity_date,
        TRY_CAST(last_modified_date AS TIMESTAMP) AS source_updated_at_utc,
        TRY_CAST(created_date AS TIMESTAMP) AS created_at_utc,
        CAST(owner_id AS STRING) AS owner_id,
        CAST(owner.name AS STRING) AS owner_name,
        CAST(owner.email AS STRING) AS owner_email,
        CAST(swiftroute_client_id AS STRING) AS client_id_source,
        CAST(account_tier AS STRING) AS account_tier,
        CAST(primary_city AS STRING) AS primary_city,
        CAST(contract_type AS STRING) AS contract_type,
        TRY_CAST(contracted_monthly_volume AS BIGINT) AS contracted_monthly_volume,
        TRY_CAST(discount_rate AS DECIMAL(8, 4)) AS discount_rate,
        CAST(is_fulfillment_client AS BOOLEAN) AS is_fulfillment_client,
        TRY_CAST(net_payment_terms AS INT) AS net_payment_terms
    FROM bronze_accounts
),

eligible_source_accounts AS (
    SELECT *
    FROM source_account_rows
    {% if is_incremental() %}
    WHERE source_updated_at_utc IS NULL
       OR source_updated_at_utc >= (
            SELECT COALESCE(
                MAX(target.source_updated_at_utc),
                CAST('1900-01-01 00:00:00' AS TIMESTAMP)
            )
            FROM {{ this }} AS target
       )
    {% endif %}
),

existing_account_mapping AS (
    {% if is_incremental() %}
    SELECT
        CAST(source_account_id AS STRING) AS source_account_id,
        CAST(account_id AS STRING) AS account_id
    FROM {{ this }}
    {% else %}
    SELECT CAST(NULL AS STRING) AS source_account_id,
           CAST(NULL AS STRING) AS account_id
    WHERE 1 = 0
    {% endif %}
),

current_max_account_number AS (
    SELECT COALESCE(
        MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(account_id, '^account_id_([0-9]+)$', 1), '') AS BIGINT)),
        0
    ) AS max_account_number
    FROM existing_account_mapping
),

new_account_ids AS (
    SELECT DISTINCT eligible.source_account_id
    FROM eligible_source_accounts AS eligible
    LEFT ANTI JOIN existing_account_mapping AS existing
        ON existing.source_account_id <=> eligible.source_account_id
    WHERE eligible.source_account_id IS NOT NULL
),

new_account_assignments AS (
    SELECT
        new_ids.source_account_id,
        CONCAT(
            'account_id_',
            CAST(current_max.max_account_number + ROW_NUMBER() OVER (ORDER BY new_ids.source_account_id) AS STRING)
        ) AS account_id
    FROM new_account_ids AS new_ids
    CROSS JOIN current_max_account_number AS current_max
),

account_id_mapping AS (
    SELECT source_account_id, account_id FROM existing_account_mapping
    UNION ALL
    SELECT source_account_id, account_id FROM new_account_assignments
)

SELECT
    mapping.account_id AS account_id,
    source.source_account_id AS source_account_id,
    COALESCE(NULLIF(TRIM(source.account_name), ''), 'N/A') AS account_name,
    COALESCE(NULLIF(TRIM(source.account_type), ''), 'N/A') AS account_type,
    COALESCE(NULLIF(TRIM(source.industry), ''), 'N/A') AS industry,
    COALESCE(NULLIF(TRIM(source.phone), ''), 'N/A') AS phone,
    COALESCE(NULLIF(TRIM(source.website), ''), 'N/A') AS website,

    /* BillingAddress is a compound object, flattened without losing source fields. */
    COALESCE(NULLIF(TRIM(source.billing_street), ''), 'N/A') AS billing_street,
    COALESCE(NULLIF(TRIM(source.billing_city), ''), 'N/A') AS billing_city,
    COALESCE(NULLIF(TRIM(source.billing_state), ''), 'N/A') AS billing_state,
    COALESCE(NULLIF(TRIM(source.billing_postal_code), ''), 'N/A') AS billing_postal_code,
    COALESCE(NULLIF(TRIM(source.billing_country), ''), 'N/A') AS billing_country,
    COALESCE(NULLIF(TRIM(source.billing_state_code), ''), 'N/A') AS billing_state_code,
    COALESCE(NULLIF(TRIM(source.billing_country_code), ''), 'N/A') AS billing_country_code,

    source.annual_revenue AS annual_revenue,
    source.number_of_employees AS number_of_employees,
    source.last_activity_date AS last_activity_date,
    source.created_at_utc AS created_at_utc,
    source.source_updated_at_utc AS source_updated_at_utc,

    /* owner_id is a Salesforce User reference, not an Account/client identifier. */
    COALESCE(NULLIF(TRIM(source.owner_id), ''), 'N/A') AS owner_id,
    COALESCE(NULLIF(TRIM(source.owner_name), ''), 'N/A') AS owner_name,
    COALESCE(NULLIF(TRIM(source.owner_email), ''), 'N/A') AS owner_email,

    COALESCE(NULLIF(TRIM(source.client_id_source), ''), 'N/A') AS client_id,
    COALESCE(NULLIF(TRIM(source.account_tier), ''), 'N/A') AS account_tier,
    COALESCE(NULLIF(TRIM(source.primary_city), ''), 'N/A') AS primary_city,
    COALESCE(NULLIF(TRIM(source.contract_type), ''), 'N/A') AS contract_type,
    source.contracted_monthly_volume AS contracted_monthly_volume,
    source.discount_rate AS discount_rate,
    source.is_fulfillment_client AS is_fulfillment_client,
    source.net_payment_terms AS net_payment_terms,

    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_source_accounts AS source
LEFT JOIN account_id_mapping AS mapping
    ON mapping.source_account_id <=> source.source_account_id
