{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'employee_uuid',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Gusto employees

    Grain: one row per Gusto employee UUID.

    Incremental strategy: merge using employee_uuid. The source does not expose a reliable
    source last-modified timestamp, so the model scans the source employee snapshot on each
    run rather than using hire_date or another business date as a false watermark. This picks
    up updates to existing employees as well as newly added employees. Source deletions require
    a full refresh or a separate deletion-handling process.

    The Onfleet Worker ID custom field is extracted safely with GET(..., 0), which returns NULL
    when FILTER returns an empty array.
*/

WITH bronze_employees AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'gusto_employees') }}
),

normalized_employees AS (
    SELECT
        CAST(NULLIF(TRIM(CAST(uuid AS STRING)), '') AS STRING) AS employee_uuid,
        COALESCE(NULLIF(TRIM(CAST(company_uuid AS STRING)), ''), 'N/A') AS company_uuid,
        COALESCE(NULLIF(TRIM(CAST(company_id AS STRING)), ''), 'N/A') AS company_id,
        COALESCE(NULLIF(TRIM(CAST(first_name AS STRING)), ''), 'N/A') AS first_name,
        COALESCE(NULLIF(TRIM(CAST(last_name AS STRING)), ''), 'N/A') AS last_name,
        COALESCE(NULLIF(TRIM(CAST(email AS STRING)), ''), 'N/A') AS email,
        COALESCE(NULLIF(TRIM(CAST(department AS STRING)), ''), 'N/A') AS department,
        COALESCE(NULLIF(TRIM(CAST(job_title AS STRING)), ''), 'N/A') AS job_title,
        TRY_CAST(NULLIF(TRIM(CAST(job_rate AS STRING)), '') AS DECIMAL(18, 2)) AS job_rate,
        COALESCE(NULLIF(TRIM(CAST(job_payment_unit AS STRING)), ''), 'N/A') AS job_payment_unit,
        TRY_CAST(NULLIF(TRIM(CAST(job_hire_date AS STRING)), '') AS DATE) AS hire_date,
        COALESCE(NULLIF(TRIM(CAST(employment_status AS STRING)), ''), 'N/A') AS employment_status,
        TRY_CAST(NULLIF(TRIM(CAST(termination_date AS STRING)), '') AS DATE) AS termination_date,
        COALESCE(NULLIF(TRIM(CAST(phone AS STRING)), ''), 'N/A') AS phone,
        TRY_CAST(NULLIF(TRIM(CAST(date_of_birth AS STRING)), '') AS DATE) AS date_of_birth,
        TRY_CAST(NULLIF(TRIM(CAST(_annual_salary AS STRING)), '') AS DECIMAL(18, 2)) AS annual_salary,
        TRY_CAST(NULLIF(TRIM(CAST(_bi_weekly_gross AS STRING)), '') AS DECIMAL(18, 2)) AS bi_weekly_gross,
        TRY_CAST(_is_driver AS BOOLEAN) AS is_driver,
        TRY_CAST(NULLIF(TRIM(CAST(_bonus_max AS STRING)), '') AS DECIMAL(18, 2)) AS bonus_max,
        COALESCE(
            TRANSFORM(
                custom_fields,
                custom_field -> NAMED_STRUCT(
                    'field_id', COALESCE(NULLIF(TRIM(CAST(custom_field.id AS STRING)), ''), 'N/A'),
                    'field_label', COALESCE(NULLIF(TRIM(CAST(custom_field.label AS STRING)), ''), 'N/A'),
                    'field_value', COALESCE(NULLIF(TRIM(CAST(custom_field.value AS STRING)), ''), 'N/A')
                )
            ),
            CAST(ARRAY() AS ARRAY<STRUCT<field_id: STRING, field_label: STRING, field_value: STRING>>)
        ) AS custom_fields,
        COALESCE(
            NULLIF(
                TRIM(
                    CAST(
                        GET(
                            FILTER(
                                custom_fields,
                                custom_field -> LOWER(TRIM(CAST(custom_field.label AS STRING))) = 'onfleet worker id'
                            ),
                            0
                        ).value AS STRING
                    )
                ),
                ''
            ),
            'N/A'
        ) AS onfleet_worker_id
    FROM bronze_employees
)

SELECT
    /* Employee identity */
    employee_uuid,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,

    /* Employment and job details */
    company_uuid,
    company_id,
    department,
    job_title,
    employment_status,
    hire_date,
    termination_date,

    /* Compensation attributes supplied on the employee profile */
    job_rate,
    job_payment_unit,
    annual_salary,
    bi_weekly_gross,
    bonus_max,
    is_driver,

    /* Cross-system mapping and source custom fields */
    onfleet_worker_id,
    custom_fields,

    /* Silver audit */
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM normalized_employees