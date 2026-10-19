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



dbt build --select salesforce_accounts_t salesforce_contracts_t salesforce_contract_rates_t salesforce_opportunities_t