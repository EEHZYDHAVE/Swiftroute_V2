SELECT * From {{ source('swiftroute_databricks', 'linnworks_inventory') }};

SELECT * From {{ source('swiftroute_databricks', 'linnworks_orders') }};

SELECT * From {{ source('swiftroute_databricks', 'linnworks_stock_transactions') }};