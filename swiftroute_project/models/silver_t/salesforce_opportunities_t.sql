{{
    config(
        materialized = 'table',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Salesforce Opportunities

    Grain: one row per source opportunity row. The simulation contains duplicate Salesforce
    Id values, so source_opportunity_id is preserved for traceability but is NOT used as a
    unique key and records are NOT deduplicated solely on that field.

    opportunity_id is a readable generated key assigned deterministically from the current
    full source snapshot. It may change if the snapshot's sorted contents change; the model
    therefore rebuilds as a table. With no reliable unique source identity and no
    LastModifiedDate in the provided source schema, incremental merge/upsert behavior cannot
    be made safe. This dataset is small (57 sample records), so a full rebuild is preferred.

    account_id maps AccountId to the readable Silver Account surrogate. client_id is sourced
    from the linked canonical Salesforce Account and follows the same cross-system name used
    by QuickBooks. CloseDate is retained as close_date; it must not be interpreted as a
    renewal start date. LastActivityDate is preserved but is not a reliable engagement proxy.
*/

WITH bronze_opportunities AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'salesforce_opportunities') }}
),

source_opportunity_rows AS (
    SELECT
        CAST(id AS STRING) AS source_opportunity_id,
        CAST(account_id AS STRING) AS source_account_id,
        CAST(account.name AS STRING) AS related_account_name,
        CAST(account.swiftroute_client_id AS STRING) AS related_account_client_id,
        CAST(opportunity_name AS STRING) AS opportunity_name,
        CAST(stage_name AS STRING) AS stage_name,
        TRY_CAST(amount AS DECIMAL(18, 2)) AS amount,
        TRY_CAST(close_date AS DATE) AS close_date,
        TRY_CAST(probability AS DECIMAL(5, 2)) AS probability_percent,
        CAST(opportunity_type AS STRING) AS opportunity_type,
        CAST(lead_source AS STRING) AS lead_source,
        CAST(owner_id AS STRING) AS owner_id,
        TRY_CAST(created_date AS TIMESTAMP) AS created_at_utc,
        TRY_CAST(last_activity_date AS DATE) AS last_activity_date,
        CAST(contract_type AS STRING) AS contract_type,
        CAST(primary_city AS STRING) AS primary_city,
        CAST(account_tier AS STRING) AS account_tier
    FROM bronze_opportunities
),

ranked_opportunities AS (
    SELECT
        source_opportunity_rows.*,
        COUNT(*) OVER (PARTITION BY source_opportunity_id) AS source_id_occurrences,
        ROW_NUMBER() OVER (
            ORDER BY
                source_opportunity_id ASC NULLS FIRST,
                source_account_id ASC NULLS FIRST,
                opportunity_name ASC NULLS FIRST,
                stage_name ASC NULLS FIRST,
                close_date ASC NULLS FIRST,
                amount ASC NULLS FIRST,
                probability_percent ASC NULLS FIRST,
                opportunity_type ASC NULLS FIRST,
                lead_source ASC NULLS FIRST,
                owner_id ASC NULLS FIRST,
                created_at_utc ASC NULLS FIRST,
                contract_type ASC NULLS FIRST,
                primary_city ASC NULLS FIRST,
                account_tier ASC NULLS FIRST
        ) AS generated_id_number
    FROM source_opportunity_rows
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
    CONCAT('opportunity_id_', CAST(source.generated_id_number AS STRING)) AS opportunity_id,
    source.source_opportunity_id AS source_opportunity_id,
    account.account_id AS account_id,
    source.source_account_id AS source_account_id,
    COALESCE(NULLIF(TRIM(account.account_name), ''), 'N/A') AS account_name,
    COALESCE(NULLIF(TRIM(account.client_id), ''), 'N/A') AS client_id,
    COALESCE(NULLIF(TRIM(source.related_account_name), ''), 'N/A') AS related_account_name,
    COALESCE(NULLIF(TRIM(source.related_account_client_id), ''), 'N/A') AS related_account_client_id,
    (source.source_id_occurrences > 1) AS source_opportunity_id_is_duplicated,
    (source.source_account_id IS NOT NULL AND account.account_id IS NULL) AS account_reference_unmatched,

    COALESCE(NULLIF(TRIM(source.opportunity_name), ''), 'N/A') AS opportunity_name,
    COALESCE(NULLIF(TRIM(source.stage_name), ''), 'N/A') AS stage_name,
    source.amount AS amount,
    source.close_date AS close_date,
    source.probability_percent AS probability_percent,
    COALESCE(NULLIF(TRIM(source.opportunity_type), ''), 'N/A') AS opportunity_type,
    COALESCE(NULLIF(TRIM(source.lead_source), ''), 'N/A') AS lead_source,
    COALESCE(NULLIF(TRIM(source.owner_id), ''), 'N/A') AS owner_id,
    source.created_at_utc AS created_at_utc,
    source.last_activity_date AS last_activity_date,
    COALESCE(NULLIF(TRIM(source.contract_type), ''), 'N/A') AS contract_type,
    COALESCE(NULLIF(TRIM(source.primary_city), ''), 'N/A') AS primary_city,
    COALESCE(NULLIF(TRIM(source.account_tier), ''), 'N/A') AS account_tier,

    CURRENT_TIMESTAMP() AS processed_at_utc
FROM ranked_opportunities AS source
LEFT JOIN account_lookup AS account
    ON account.source_account_id <=> source.source_account_id
