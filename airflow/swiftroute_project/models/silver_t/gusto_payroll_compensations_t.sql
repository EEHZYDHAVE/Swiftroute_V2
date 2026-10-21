{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'payroll_compensation_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Gusto payroll employee compensations

    Grain: one row per employee-compensation entry inside a payroll run.

    Bronze splits the source payroll API response into two relations:
      - gusto_payroll_runs: one parent row with payroll status, dates, and run-level totals.
      - gusto_payroll_compensations: employee-compensation rows, each carrying payroll_run_uuid.

    The model joins those relations on gusto_payroll_compensations.payroll_run_uuid =
    gusto_payroll_runs.uuid, reconstructing the run context without changing the compensation
    grain. Parent-run totals are therefore repeated across employee-compensation rows and must
    not be summed at this grain.

    Incremental strategy: merge on payroll_compensation_id. No reliable source-modified
    timestamp exists. Each run reads the current joined source snapshot and merges by a stable
    key built from payroll_run_uuid plus compensation_ordinal (with a deterministic fallback
    for missing ordinals), so historical rows can be updated without an unsafe date watermark.
    Source deletions require a full refresh or a separate deletion-handling process.

    Arrays stay nested. variable_compensations_json and paid_time_off_json are parsed from JSON.
*/

WITH bronze_payroll_compensations AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'gusto_payroll_compensations') }}
),

bronze_payroll_runs AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'gusto_payroll_runs') }}
),

joined_source AS (
    SELECT
        COALESCE(NULLIF(TRIM(CAST(c.payroll_run_uuid AS STRING)), ''), 'N/A') AS source_payroll_id,
        COALESCE(
            NULLIF(TRIM(CAST(r.version AS STRING)), ''),
            NULLIF(TRIM(CAST(c.payroll_version AS STRING)), ''),
            'N/A'
        ) AS payroll_version,
        COALESCE(
            NULLIF(TRIM(CAST(r.company_uuid AS STRING)), ''),
            NULLIF(TRIM(CAST(c.company_uuid AS STRING)), ''),
            'N/A'
        ) AS company_uuid,
        COALESCE(
            NULLIF(TRIM(CAST(r.company_id AS STRING)), ''),
            NULLIF(TRIM(CAST(c.company_id AS STRING)), ''),
            'N/A'
        ) AS company_id,
        COALESCE(
            TRY_CAST(NULLIF(TRIM(CAST(r.payroll_deadline AS STRING)), '') AS DATE),
            TRY_CAST(NULLIF(TRIM(CAST(c.payroll_deadline AS STRING)), '') AS DATE)
        ) AS payroll_deadline,
        COALESCE(
            TRY_CAST(NULLIF(TRIM(CAST(r.check_date AS STRING)), '') AS DATE),
            TRY_CAST(NULLIF(TRIM(CAST(c.check_date AS STRING)), '') AS DATE)
        ) AS check_date,
        TRY_CAST(r.processed AS BOOLEAN) AS is_processed,
        COALESCE(
            TRY_CAST(NULLIF(TRIM(CAST(r.pay_period_start_date AS STRING)), '') AS DATE),
            TRY_CAST(NULLIF(TRIM(CAST(c.pay_period_start_date AS STRING)), '') AS DATE)
        ) AS pay_period_start_date,
        COALESCE(
            TRY_CAST(NULLIF(TRIM(CAST(r.pay_period_end_date AS STRING)), '') AS DATE),
            TRY_CAST(NULLIF(TRIM(CAST(c.pay_period_end_date AS STRING)), '') AS DATE)
        ) AS pay_period_end_date,

        /* Payroll-level totals come from the parent payroll-runs table. */
        TRY_CAST(NULLIF(TRIM(CAST(r.totals_company_debit AS STRING)), '') AS DECIMAL(18, 2)) AS payroll_company_debit,
        TRY_CAST(NULLIF(TRIM(CAST(r.totals_gross_pay AS STRING)), '') AS DECIMAL(18, 2)) AS payroll_gross_pay,
        TRY_CAST(NULLIF(TRIM(CAST(r.totals_net_pay AS STRING)), '') AS DECIMAL(18, 2)) AS payroll_net_pay,
        TRY_CAST(NULLIF(TRIM(CAST(r.totals_employee_taxes AS STRING)), '') AS DECIMAL(18, 2)) AS payroll_employee_taxes,
        TRY_CAST(NULLIF(TRIM(CAST(r.totals_employer_taxes AS STRING)), '') AS DECIMAL(18, 2)) AS payroll_employer_taxes,
        TRY_CAST(NULLIF(TRIM(CAST(r.totals_employee_deductions AS STRING)), '') AS DECIMAL(18, 2)) AS payroll_employee_deductions,

        /* Employee details: one record in this table per employee compensation item. */
        COALESCE(NULLIF(TRIM(CAST(c.employee_uuid AS STRING)), ''), 'N/A') AS employee_uuid,
        COALESCE(NULLIF(TRIM(CAST(c.employee_first_name AS STRING)), ''), 'N/A') AS employee_first_name,
        COALESCE(NULLIF(TRIM(CAST(c.employee_last_name AS STRING)), ''), 'N/A') AS employee_last_name,
        COALESCE(NULLIF(TRIM(CAST(c.department AS STRING)), ''), 'N/A') AS department,
        NULLIF(TRIM(CAST(c.compensation_ordinal AS STRING)), '') AS source_compensation_ordinal,

        COALESCE(
            TRANSFORM(
                c.fixed_compensations,
                compensation -> NAMED_STRUCT(
                    'name', COALESCE(NULLIF(TRIM(CAST(compensation.name AS STRING)), ''), 'N/A'),
                    'amount', TRY_CAST(NULLIF(TRIM(CAST(compensation.amount AS STRING)), '') AS DECIMAL(18, 2)),
                    'job_uuid', COALESCE(NULLIF(TRIM(CAST(compensation.job_uuid AS STRING)), ''), 'N/A')
                )
            ),
            CAST(ARRAY() AS ARRAY<STRUCT<name: STRING, amount: DECIMAL(18, 2), job_uuid: STRING>>)
        ) AS fixed_compensations,

        COALESCE(
            TRANSFORM(
                FROM_JSON(
                    NULLIF(TRIM(CAST(c.variable_compensations_json AS STRING)), ''),
                    'ARRAY<STRUCT<name: STRING, amount: STRING, job_uuid: STRING>>'
                ),
                compensation -> NAMED_STRUCT(
                    'name', COALESCE(NULLIF(TRIM(CAST(compensation.name AS STRING)), ''), 'N/A'),
                    'amount', TRY_CAST(NULLIF(TRIM(CAST(compensation.amount AS STRING)), '') AS DECIMAL(18, 2)),
                    'job_uuid', COALESCE(NULLIF(TRIM(CAST(compensation.job_uuid AS STRING)), ''), 'N/A')
                )
            ),
            CAST(ARRAY() AS ARRAY<STRUCT<name: STRING, amount: DECIMAL(18, 2), job_uuid: STRING>>)
        ) AS variable_compensations,

        /* PTO shape is not defined by a non-empty sample; use a flexible key/value struct. */
        COALESCE(
            FROM_JSON(
                NULLIF(TRIM(CAST(c.paid_time_off_json AS STRING)), ''),
                'ARRAY<MAP<STRING, STRING>>'
            ),
            CAST(ARRAY() AS ARRAY<MAP<STRING, STRING>>)
        ) AS paid_time_off,

        COALESCE(
            TRANSFORM(
                c.benefits,
                benefit -> NAMED_STRUCT(
                    'name', COALESCE(NULLIF(TRIM(CAST(benefit.name AS STRING)), ''), 'N/A'),
                    'employee_deduction', TRY_CAST(NULLIF(TRIM(CAST(benefit.employee_deduction AS STRING)), '') AS DECIMAL(18, 2)),
                    'company_contribution', TRY_CAST(NULLIF(TRIM(CAST(benefit.company_contribution AS STRING)), '') AS DECIMAL(18, 2)),
                    'imputed', TRY_CAST(CAST(benefit.imputed AS STRING) AS BOOLEAN)
                )
            ),
            CAST(ARRAY() AS ARRAY<STRUCT<name: STRING, employee_deduction: DECIMAL(18, 2), company_contribution: DECIMAL(18, 2), imputed: BOOLEAN>>)
        ) AS benefits,

        COALESCE(
            TRANSFORM(
                c.employee_deductions,
                deduction -> NAMED_STRUCT(
                    'name', COALESCE(NULLIF(TRIM(CAST(deduction.name AS STRING)), ''), 'N/A'),
                    'amount', TRY_CAST(NULLIF(TRIM(CAST(deduction.amount AS STRING)), '') AS DECIMAL(18, 2)),
                    'pre_tax', TRY_CAST(CAST(deduction.pre_tax AS STRING) AS BOOLEAN)
                )
            ),
            CAST(ARRAY() AS ARRAY<STRUCT<name: STRING, amount: DECIMAL(18, 2), pre_tax: BOOLEAN>>)
        ) AS employee_deductions,

        COALESCE(
            TRANSFORM(
                c.taxes,
                tax -> NAMED_STRUCT(
                    'name', COALESCE(NULLIF(TRIM(CAST(tax.name AS STRING)), ''), 'N/A'),
                    'is_employer_tax', TRY_CAST(CAST(tax.employer AS STRING) AS BOOLEAN),
                    'amount', TRY_CAST(NULLIF(TRIM(CAST(tax.amount AS STRING)), '') AS DECIMAL(18, 2))
                )
            ),
            CAST(ARRAY() AS ARRAY<STRUCT<name: STRING, is_employer_tax: BOOLEAN, amount: DECIMAL(18, 2)>>)
        ) AS taxes

    FROM bronze_payroll_compensations AS c
    LEFT JOIN bronze_payroll_runs AS r
        ON CAST(c.payroll_run_uuid AS STRING) = CAST(r.uuid AS STRING)
),

key_components AS (
    SELECT
        joined_source.*,
        ROW_NUMBER() OVER (
            PARTITION BY source_payroll_id
            ORDER BY
                employee_uuid,
                employee_first_name,
                employee_last_name,
                TO_JSON(NAMED_STRUCT(
                    'fixed_compensations', fixed_compensations,
                    'variable_compensations', variable_compensations,
                    'paid_time_off', paid_time_off,
                    'benefits', benefits,
                    'employee_deductions', employee_deductions,
                    'taxes', taxes
                ))
        ) AS fallback_compensation_ordinal
    FROM joined_source
),

keyed_payroll_compensations AS (
    SELECT
        key_components.*,
        CONCAT(
            'payroll_comp_',
            SUBSTRING(
                SHA2(
                    CONCAT_WS(
                        '||',
                        source_payroll_id,
                        CASE
                            WHEN source_compensation_ordinal IS NOT NULL
                                THEN CONCAT('source_ordinal_', source_compensation_ordinal)
                            ELSE CONCAT('fallback_ordinal_', CAST(fallback_compensation_ordinal AS STRING))
                        END
                    ),
                    256
                ),
                1,
                32
            )
        ) AS payroll_compensation_id
    FROM key_components
)

SELECT
    /* Unique compensation row key and source payroll reference */
    payroll_compensation_id,
    source_payroll_id,
    payroll_version,

    /* Payroll run and pay-period dates/status */
    company_uuid,
    company_id,
    payroll_deadline,
    check_date,
    is_processed,
    pay_period_start_date,
    pay_period_end_date,

    /* Repeated payroll-run totals; do not sum across compensation rows */
    payroll_company_debit,
    payroll_gross_pay,
    payroll_net_pay,
    payroll_employee_taxes,
    payroll_employer_taxes,
    payroll_employee_deductions,

    /* Employee compensation details */
    employee_uuid,
    employee_first_name,
    employee_last_name,
    department,
    fixed_compensations,
    variable_compensations,
    paid_time_off,
    benefits,
    employee_deductions,
    taxes,

    /* Silver audit */
    CURRENT_TIMESTAMP() AS processed_at_utc
FROM keyed_payroll_compensations