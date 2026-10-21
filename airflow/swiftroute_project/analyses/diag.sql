SELECT COUNT(*), COUNT(DISTINCT(id))
FROM {{ source('swiftroute_databricks', 'quickbooks_payments') }}

WITH duplicate_ids AS (
    SELECT id
    FROM {{ source('swiftroute_databricks', 'quickbooks_expenses') }}
    GROUP BY id
    HAVING COUNT(*) > 1
)
SELECT e.*
FROM {{ source('swiftroute_databricks', 'quickbooks_expenses') }} e
INNER JOIN duplicate_ids d
    ON e.id = d.id
ORDER BY e.id;


SELECT *
FROM {{ source('swiftroute_databricks', 'quickbooks_invoices') }}

SELECT *
FROM {{ source('swiftroute_databricks', 'quickbooks_payments') }}



SELECT *
FROM {{source('swiftroute_databricks','salesforce_accounts')}}

SELECT *
FROM {{source('swiftroute_databricks','salesforce_contract_rates')}}

SELECT *
FROM {{source('swiftroute_databricks','salesforce_contracts')}}

SELECT *
FROM {{source('swiftroute_databricks','salesforce_opportunities')}}



SELECT *
FROM {{ source('swiftroute_databricks', 'salesforce_opportunities') }}
WHERE account_id ='0058ckITgaFSoLeHGK'




WITH duplicate_ids AS (
    SELECT account_id
    FROM {{ source('swiftroute_databricks', 'salesforce_opportunities') }}
    GROUP BY account_id
    HAVING COUNT(*) > 1
)
SELECT e.*
FROM {{ source('swiftroute_databricks', 'salesforce_opportunities') }} e
INNER JOIN duplicate_ids d
    ON e.account_id = d.account_id
ORDER BY e.account_id;















SELECT *
FROM {{source('swiftroute_databricks','samsara_vehicles')}}

SELECT DISTINCT city
FROM {{source('swiftroute_databricks','samsara_driver_summary')}}


SELECT COUNT(*) AS total_count, COUNT(DISTINCT(driver_id)) AS distinct_id_count
FROM {{source('swiftroute_databricks','samsara_driver_summary')}}

SELECT COUNT(*) AS total_count, COUNT(DISTINCT(trip_id)) AS distinct_id_count
FROM {{source('swiftroute_databricks','samsara_trips')}}

SELECT COUNT(*) AS total_count, COUNT(DISTINCT(vehicle_id)) AS distinct_id_count
FROM {{source('swiftroute_databricks','samsara_vehicles')}}














SELECT COUNT(*) AS total_count
FROM {{ref('onfleet_workers_t')}}








SELECT *
FROM {{source('swiftroute_databricks','gusto_employees')}}

SELECT *
FROM {{source('swiftroute_databricks','gusto_payroll_runs')}}

SELECT *
FROM {{source('swiftroute_databricks','gusto_payroll_compensations')}}
WHERE payroll_run_uuid = 'e0361ada-5a8d-4f4a-b08a-9c4d3bc240b7'



SELECT COUNT(*) AS total_count, COUNT(DISTINCT(payroll_run_uuid)) AS distinct_id_count
FROM {{source('swiftroute_databricks','gusto_payroll_compensations')}}


WITH duplicate_ids AS (
    SELECT payroll_run_uuid
    FROM {{ source('swiftroute_databricks', 'gusto_payroll_compensations') }}
    GROUP BY payroll_run_uuid
    HAVING COUNT(*) > 1
)
SELECT e.*
FROM {{ source('swiftroute_databricks', 'gusto_payroll_compensations') }} e
INNER JOIN duplicate_ids d
    ON e.payroll_run_uuid = d.payroll_run_uuid
ORDER BY e.payroll_run_uuid;






SELECT table_name, column_name
FROM `swiftroute`.`information_schema`.`columns`
WHERE table_name IN ('gusto_payroll_runs', 'gusto_payroll_compensations')
ORDER BY table_name, ordinal_position;





SELECT
    table_schema,
    table_name,
    ordinal_position,
    column_name,
    data_type,
    full_data_type,
    is_nullable
FROM `swiftroute`.`information_schema`.`columns`
WHERE LOWER(table_schema) = 'silver_t'
ORDER BY
    table_name,
    ordinal_position;