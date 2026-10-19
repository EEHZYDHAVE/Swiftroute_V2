{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'source_contract_rate_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Salesforce Contract Rates

    Grain: one row per Contract_Rate__c record. Do not flatten or aggregate rate rows:
    multiple services, zones, and effective dates can belong to one contract.

    contract_rate_id is a readable persistent surrogate. source_contract_rate_id preserves
    the source Id. contract_id and account_id use readable surrogate keys from the parent
    Silver models, while source_contract_id/source_account_id preserve the Salesforce IDs.

    client_id is standardized to the same name used by QuickBooks and other sources. The
    canonical value comes from the linked Salesforce Account (via the Contract relationship
    first); source_client_id preserves SwiftRoute_Client_ID__c from the rate object. Flags
    expose mismatches instead of silently hiding them.

    Incremental watermark: CreatedDate, because the supplied Bronze schema has no
    LastModifiedDate for Contract_Rate__c. This detects new rate records but cannot reliably
    detect edits to older records; rebuild the model when existing rate details may have changed.
*/

WITH bronze_contract_rates AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'salesforce_contract_rates') }}
),

source_rate_rows AS (
    SELECT
        CAST(id AS STRING) AS source_contract_rate_id,
        CAST(contract_id AS STRING) AS source_contract_id,
        CAST(account_id AS STRING) AS source_account_id,
        CAST(swiftroute_client_id AS STRING) AS source_client_id,
        CAST(service_type AS STRING) AS service_type,
        CAST(zone_id AS STRING) AS zone_id,
        CAST(zone_name AS STRING) AS zone_name,
        TRY_CAST(base_rate AS DECIMAL(18, 4)) AS base_rate,
        TRY_CAST(discount_rate AS DECIMAL(8, 4)) AS discount_rate,
        TRY_CAST(net_rate AS DECIMAL(18, 4)) AS net_rate,
        CAST(unit AS STRING) AS rate_unit,
        TRY_CAST(effective_date AS DATE) AS effective_date,
        TRY_CAST(created_date AS TIMESTAMP) AS created_at_utc
    FROM bronze_contract_rates
),

eligible_source_rates AS (
    SELECT *
    FROM source_rate_rows
    {% if is_incremental() %}
    WHERE created_at_utc IS NULL
       OR created_at_utc >= (
            SELECT COALESCE(
                MAX(target.created_at_utc),
                CAST('1900-01-01 00:00:00' AS TIMESTAMP)
            )
            FROM {{ this }} AS target
       )
    {% endif %}
),

existing_rate_mapping AS (
    {% if is_incremental() %}
    SELECT
        CAST(source_contract_rate_id AS STRING) AS source_contract_rate_id,
        CAST(contract_rate_id AS STRING) AS contract_rate_id
    FROM {{ this }}
    {% else %}
    SELECT CAST(NULL AS STRING) AS source_contract_rate_id,
           CAST(NULL AS STRING) AS contract_rate_id
    WHERE 1 = 0
    {% endif %}
),

current_max_rate_number AS (
    SELECT COALESCE(
        MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(contract_rate_id, '^contract_rate_id_([0-9]+)$', 1), '') AS BIGINT)),
        0
    ) AS max_rate_number
    FROM existing_rate_mapping
),

new_rate_ids AS (
    SELECT DISTINCT eligible.source_contract_rate_id
    FROM eligible_source_rates AS eligible
    LEFT ANTI JOIN existing_rate_mapping AS existing
        ON existing.source_contract_rate_id <=> eligible.source_contract_rate_id
    WHERE eligible.source_contract_rate_id IS NOT NULL
),

new_rate_assignments AS (
    SELECT
        new_ids.source_contract_rate_id,
        CONCAT(
            'contract_rate_id_',
            CAST(current_max.max_rate_number + ROW_NUMBER() OVER (ORDER BY new_ids.source_contract_rate_id) AS STRING)
        ) AS contract_rate_id
    FROM new_rate_ids AS new_ids
    CROSS JOIN current_max_rate_number AS current_max
),

contract_rate_id_mapping AS (
    SELECT source_contract_rate_id, contract_rate_id FROM existing_rate_mapping
    UNION ALL
    SELECT source_contract_rate_id, contract_rate_id FROM new_rate_assignments
),

account_lookup AS (
    SELECT
        CAST(source_account_id AS STRING) AS source_account_id,
        CAST(account_id AS STRING) AS account_id,
        CAST(client_id AS STRING) AS account_client_id
    FROM {{ ref('salesforce_accounts_t') }}
),

contract_lookup AS (
    SELECT
        CAST(source_contract_id AS STRING) AS source_contract_id,
        CAST(contract_id AS STRING) AS contract_id,
        CAST(source_account_id AS STRING) AS contract_source_account_id,
        CAST(account_id AS STRING) AS contract_account_id,
        CAST(client_id AS STRING) AS contract_client_id
    FROM {{ ref('salesforce_contracts_t') }}
)

SELECT
    rate_keys.contract_rate_id AS contract_rate_id,
    source.source_contract_rate_id AS source_contract_rate_id,
    contract.contract_id AS contract_id,
    source.source_contract_id AS source_contract_id,
    account.account_id AS account_id,
    source.source_account_id AS source_account_id,

    COALESCE(
        NULLIF(NULLIF(TRIM(contract.contract_client_id), ''), 'N/A'),
        NULLIF(NULLIF(TRIM(account.account_client_id), ''), 'N/A'),
        'N/A'
    ) AS client_id,
    COALESCE(NULLIF(TRIM(source.source_client_id), ''), 'N/A') AS source_client_id,
    (
        source.source_client_id IS NOT NULL
        AND TRIM(source.source_client_id) <> ''
        AND COALESCE(
            NULLIF(NULLIF(TRIM(contract.contract_client_id), ''), 'N/A'),
            NULLIF(NULLIF(TRIM(account.account_client_id), ''), 'N/A')
        ) IS NOT NULL
        AND source.source_client_id <> COALESCE(
            NULLIF(NULLIF(TRIM(contract.contract_client_id), ''), 'N/A'),
            NULLIF(NULLIF(TRIM(account.account_client_id), ''), 'N/A')
        )
    ) AS client_id_mismatch,
    (
        source.source_account_id IS NOT NULL
        AND contract.contract_source_account_id IS NOT NULL
        AND source.source_account_id <> contract.contract_source_account_id
    ) AS contract_account_mismatch,
    (source.source_contract_id IS NOT NULL AND contract.contract_id IS NULL) AS contract_reference_unmatched,
    (source.source_account_id IS NOT NULL AND account.account_id IS NULL) AS account_reference_unmatched,

    COALESCE(NULLIF(TRIM(source.service_type), ''), 'N/A') AS service_type,
    COALESCE(NULLIF(TRIM(source.zone_id), ''), 'N/A') AS zone_id,
    COALESCE(NULLIF(TRIM(source.zone_name), ''), 'N/A') AS zone_name,
    (source.zone_id IS NOT NULL AND TRIM(source.zone_id) <> '') AS is_zone_specific,
    source.base_rate AS base_rate,
    source.discount_rate AS discount_rate,
    source.net_rate AS net_rate,
    COALESCE(NULLIF(TRIM(source.rate_unit), ''), 'N/A') AS rate_unit,
    source.effective_date AS effective_date,
    source.created_at_utc AS created_at_utc,

    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_source_rates AS source
LEFT JOIN contract_rate_id_mapping AS rate_keys
    ON rate_keys.source_contract_rate_id <=> source.source_contract_rate_id
LEFT JOIN account_lookup AS account
    ON account.source_account_id <=> source.source_account_id
LEFT JOIN contract_lookup AS contract
    ON contract.source_contract_id <=> source.source_contract_id
