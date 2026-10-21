{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'source_contract_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Salesforce Contracts

    Grain: one row per Salesforce Contract record.

    contract_id is a readable persistent surrogate. source_contract_id retains the
    Salesforce Contract.Id. account_id is the readable FK to silver_salesforce_accounts;
    source_account_id preserves the original Salesforce AccountId. client_id is derived
    from the linked Silver Account so it matches QuickBooks' client_id convention.

    Three contracts in the simulation have no AccountId and no rate rows. They are kept
    and flagged as orphans; their source_account_id and account_id remain NULL so they
    cannot accidentally join to an invented account.

    Incremental watermark: Salesforce LastModifiedDate.
*/

WITH bronze_contracts AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'salesforce_contracts') }}
),

source_contract_rows AS (
    SELECT
        CAST(id AS STRING) AS source_contract_id,
        CAST(account_id AS STRING) AS source_account_id,
        CAST(account.name AS STRING) AS related_account_name,
        CAST(account.swiftroute_client_id AS STRING) AS related_account_client_id,
        CAST(status AS STRING) AS status,
        TRY_CAST(start_date AS DATE) AS start_date,
        TRY_CAST(end_date AS DATE) AS end_date,
        TRY_CAST(contract_term AS INT) AS contract_term_months,
        TRY_CAST(signed_date AS DATE) AS signed_date,
        CAST(contract_type AS STRING) AS contract_type,
        TRY_CAST(committed_monthly_volume AS BIGINT) AS committed_monthly_volume,
        TRY_CAST(discount_rate AS DECIMAL(8, 4)) AS discount_rate,
        TRY_CAST(net_payment_terms AS INT) AS net_payment_terms,
        CAST(auto_renewal AS BOOLEAN) AS auto_renewal,
        TRY_CAST(termination_notice_days AS INT) AS termination_notice_days,
        CAST(primary_city AS STRING) AS primary_city,
        CAST(account_tier AS STRING) AS account_tier,
        CAST(rate_note AS STRING) AS rate_note,
        TRY_CAST(created_date AS TIMESTAMP) AS created_at_utc,
        TRY_CAST(last_modified_date AS TIMESTAMP) AS source_updated_at_utc,
        CAST(owner_id AS STRING) AS owner_id
    FROM bronze_contracts
),

eligible_source_contracts AS (
    SELECT *
    FROM source_contract_rows
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

existing_contract_mapping AS (
    {% if is_incremental() %}
    SELECT
        CAST(source_contract_id AS STRING) AS source_contract_id,
        CAST(contract_id AS STRING) AS contract_id
    FROM {{ this }}
    {% else %}
    SELECT CAST(NULL AS STRING) AS source_contract_id,
           CAST(NULL AS STRING) AS contract_id
    WHERE 1 = 0
    {% endif %}
),

current_max_contract_number AS (
    SELECT COALESCE(
        MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(contract_id, '^contract_id_([0-9]+)$', 1), '') AS BIGINT)),
        0
    ) AS max_contract_number
    FROM existing_contract_mapping
),

new_contract_ids AS (
    SELECT DISTINCT eligible.source_contract_id
    FROM eligible_source_contracts AS eligible
    LEFT ANTI JOIN existing_contract_mapping AS existing
        ON existing.source_contract_id <=> eligible.source_contract_id
    WHERE eligible.source_contract_id IS NOT NULL
),

new_contract_assignments AS (
    SELECT
        new_ids.source_contract_id,
        CONCAT(
            'contract_id_',
            CAST(current_max.max_contract_number + ROW_NUMBER() OVER (ORDER BY new_ids.source_contract_id) AS STRING)
        ) AS contract_id
    FROM new_contract_ids AS new_ids
    CROSS JOIN current_max_contract_number AS current_max
),

contract_id_mapping AS (
    SELECT source_contract_id, contract_id FROM existing_contract_mapping
    UNION ALL
    SELECT source_contract_id, contract_id FROM new_contract_assignments
),

account_lookup AS (
    SELECT
        CAST(source_account_id AS STRING) AS source_account_id,
        CAST(account_id AS STRING) AS account_id,
        CAST(account_name AS STRING) AS account_name,
        CAST(client_id AS STRING) AS client_id
    FROM {{ ref('salesforce_accounts_t') }}
)

SELECT
    contract_keys.contract_id AS contract_id,
    source.source_contract_id AS source_contract_id,
    account.account_id AS account_id,
    source.source_account_id AS source_account_id,
    COALESCE(NULLIF(TRIM(account.account_name), ''), 'N/A') AS account_name,
    COALESCE(NULLIF(TRIM(account.client_id), ''), 'N/A') AS client_id,

    /* Keep the nested relationship snapshot for comparison with the canonical Account. */
    COALESCE(NULLIF(TRIM(source.related_account_name), ''), 'N/A') AS related_account_name,
    COALESCE(NULLIF(TRIM(source.related_account_client_id), ''), 'N/A') AS related_account_client_id,
    (source.source_account_id IS NULL) AS is_orphaned_contract,
    (source.source_account_id IS NOT NULL AND account.account_id IS NULL) AS account_reference_unmatched,

    COALESCE(NULLIF(TRIM(source.status), ''), 'N/A') AS contract_status,
    source.start_date AS start_date,
    source.end_date AS end_date,
    source.contract_term_months AS contract_term_months,
    source.signed_date AS signed_date,
    COALESCE(NULLIF(TRIM(source.contract_type), ''), 'N/A') AS contract_type,
    source.committed_monthly_volume AS committed_monthly_volume,
    source.discount_rate AS discount_rate,
    source.net_payment_terms AS net_payment_terms,
    source.auto_renewal AS auto_renewal,
    source.termination_notice_days AS termination_notice_days,
    COALESCE(NULLIF(TRIM(source.primary_city), ''), 'N/A') AS primary_city,
    COALESCE(NULLIF(TRIM(source.account_tier), ''), 'N/A') AS account_tier,
    COALESCE(NULLIF(TRIM(source.rate_note), ''), 'N/A') AS rate_note,
    source.created_at_utc AS created_at_utc,
    source.source_updated_at_utc AS source_updated_at_utc,
    COALESCE(NULLIF(TRIM(source.owner_id), ''), 'N/A') AS owner_id,

    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_source_contracts AS source
LEFT JOIN contract_id_mapping AS contract_keys
    ON contract_keys.source_contract_id <=> source.source_contract_id
LEFT JOIN account_lookup AS account
    ON account.source_account_id <=> source.source_account_id
