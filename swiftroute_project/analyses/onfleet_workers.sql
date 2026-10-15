SELECT *
FROM {{ source('swiftroute_databricks', 'onfleet_workers') }}