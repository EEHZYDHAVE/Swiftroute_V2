{{
    config(
        schema = 'gold_last_mile',
        alias = 'dim_driver'
    )
}}

/*
    Driver dimension keyed by the Onfleet worker ID used by Silver Business deliveries.
    Gusto details are joined only through the explicit onfleet_worker_id mapping and
    only where that mapping identifies one employee. No payroll money is included.
*/
WITH worker_source AS (
    SELECT
        CAST(w.worker_id AS STRING) AS worker_id,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(w.worker_name AS STRING)), ''), 'N/A'), 'N/A') AS driver_name,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(w.display_name AS STRING)), ''), 'N/A'), 'N/A') AS driver_display_name,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(w.employment_type AS STRING)), ''), 'N/A'), 'N/A') AS onfleet_employment_type,
        CAST(w.on_duty AS BOOLEAN) AS current_on_duty
    FROM {{ ref('onfleet_workers_t') }} w
    WHERE w.worker_id IS NOT NULL
      AND TRIM(CAST(w.worker_id AS STRING)) <> ''
      AND UPPER(TRIM(CAST(w.worker_id AS STRING))) <> 'N/A'
),
employees AS (
    SELECT
        CAST(employee_uuid AS STRING) AS employee_uuid,
        NULLIF(NULLIF(TRIM(CAST(onfleet_worker_id AS STRING)), ''), 'N/A') AS onfleet_worker_id,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(employment_status AS STRING)), ''), 'N/A'), 'N/A') AS employment_status,
        TRY_CAST(hire_date AS DATE) AS hire_date,
        TRY_CAST(termination_date AS DATE) AS termination_date,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(department AS STRING)), ''), 'N/A'), 'N/A') AS department,
        COALESCE(NULLIF(NULLIF(TRIM(CAST(job_title AS STRING)), ''), 'N/A'), 'N/A') AS job_title,
        CAST(is_driver AS BOOLEAN) AS is_driver
    FROM {{ ref('gusto_employees_t') }}
    WHERE employee_uuid IS NOT NULL
),
employee_counts AS (
    SELECT onfleet_worker_id, COUNT(DISTINCT employee_uuid) AS employee_match_count
    FROM employees
    WHERE onfleet_worker_id IS NOT NULL
    GROUP BY onfleet_worker_id
),
employee_one AS (
    SELECT e.*
    FROM employees e
    INNER JOIN employee_counts c
      ON c.onfleet_worker_id = e.onfleet_worker_id
     AND c.employee_match_count = 1
)
SELECT
    w.worker_id AS driver_key,
    w.worker_id,
    w.driver_name,
    w.driver_display_name,
    w.onfleet_employment_type,
    e.employee_uuid AS gusto_employee_uuid,
    COALESCE(e.employment_status, 'N/A') AS gusto_employment_status,
    e.hire_date AS gusto_hire_date,
    e.termination_date AS gusto_termination_date,
    COALESCE(e.department, 'N/A') AS gusto_department,
    COALESCE(e.job_title, 'N/A') AS gusto_job_title,
    e.is_driver AS is_gusto_driver,
    w.current_on_duty,
    CASE WHEN e.employee_uuid IS NOT NULL THEN TRUE ELSE FALSE END AS has_gusto_employee_match,
    FALSE AS is_unknown_driver
FROM worker_source w
LEFT JOIN employee_one e ON e.onfleet_worker_id = w.worker_id

UNION ALL

SELECT
    'N/A' AS driver_key,
    'N/A' AS worker_id,
    'Unknown Driver' AS driver_name,
    'Unknown Driver' AS driver_display_name,
    'N/A' AS onfleet_employment_type,
    CAST(NULL AS STRING) AS gusto_employee_uuid,
    'N/A' AS gusto_employment_status,
    CAST(NULL AS DATE) AS gusto_hire_date,
    CAST(NULL AS DATE) AS gusto_termination_date,
    'N/A' AS gusto_department,
    'N/A' AS gusto_job_title,
    CAST(NULL AS BOOLEAN) AS is_gusto_driver,
    CAST(NULL AS BOOLEAN) AS current_on_duty,
    FALSE AS has_gusto_employee_match,
    TRUE AS is_unknown_driver
