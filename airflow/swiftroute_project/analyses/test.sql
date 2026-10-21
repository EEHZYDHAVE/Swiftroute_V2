SELECT * FROM {{ source('swiftroute_databricks', 'linnworks_inventory') }};

SELECT * FROM {{ source('swiftroute_databricks', 'linnworks_orders') }};

SELECT * FROM {{ source('swiftroute_databricks', 'linnworks_stock_transactions') }};


SELECT 
COUNT(*) AS all,
COUNT(pk_order_id IS NULL) AS pk_order_i,
COUNT(num_order_id IS NULL) AS num_order_id,
COUNT(reference_num IS NULL) AS reference_num,
COUNT(external_reference IS NULL) AS external_reference,
COUNT(secondary_reference IS NULL) AS secondary_reference

FROM {{ source('swiftroute_databricks', 'linnworks_orders') }};



SELECT o.*,
       COUNT(*) OVER (PARTITION BY pk_order_id) AS duplicate_count
FROM {{ source('swiftroute_databricks', 'linnworks_orders') }} o
WHERE pk_order_id IN (
    SELECT pk_order_id
    FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
    GROUP BY pk_order_id
    HAVING COUNT(*) > 1
)
ORDER BY pk_order_id;




SELECT *
FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
WHERE pk_order_id IN (
    SELECT pk_order_id
    FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
    GROUP BY pk_order_id
    HAVING COUNT(*) > 1
)
ORDER BY pk_order_id;


SELECT pk_order_id,
       COUNT(DISTINCT num_order_id) AS num_order_variants,
       COUNT(DISTINCT reference_num) AS reference_variants,
       COUNT(DISTINCT external_reference) AS external_variants
FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
GROUP BY pk_order_id
HAVING COUNT(*) > 1
ORDER BY num_order_variants DESC, reference_variants DESC

WITH irregular_records AS (
    SELECT pk_order_id,
       COUNT(DISTINCT num_order_id) AS num_order_variants,
       COUNT(DISTINCT reference_num) AS reference_variants,
       COUNT(DISTINCT external_reference) AS external_variants
    FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
    GROUP BY pk_order_id
    HAVING COUNT(*) > 1
    ORDER BY num_order_variants DESC, reference_variants DESC
)

SELECT i.num_order_variants, i.reference_variants, i.external_variants, o.*
FROM {{ source('swiftroute_databricks', 'linnworks_orders') }} o
INNER JOIN irregular_records i
    ON o.pk_order_id = i.pk_order_id
ORDER BY i.num_order_variants DESC



SELECT *
FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
WHERE num_order_id IN (
    800253,
    114271,
    389701,
    408327,
    749522,
    524734
)
ORDER BY num_order_id;




SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN num_order_id = '' THEN 1 END) AS empty_num_order_id,
    COUNT(CASE WHEN reference_num = '' THEN 1 END) AS empty_reference_num,
    COUNT(CASE WHEN external_reference = '' THEN 1 END) AS empty_external_reference,
    COUNT(CASE WHEN secondary_reference = '' THEN 1 END) AS empty_secondary_reference
FROM {{ source('swiftroute_databricks', 'linnworks_orders') }};



SELECT *
FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
WHERE num_order_id IS NULL
   OR reference_num IS NULL
   OR external_reference IS NULL
   OR secondary_reference IS NULL;


SELECT COUNT(DISTINCT(pk_stock_transaction_id))
FROM {{ source('swiftroute_databricks', 'linnworks_stock_transactions') }};



WITH duplicate_ids AS (
    SELECT pk_stock_transaction_id
    FROM {{ source('swiftroute_databricks', 'linnworks_stock_transactions') }}
    GROUP BY pk_stock_transaction_id
    HAVING COUNT(*) > 1
)
SELECT o.*
FROM {{ source('swiftroute_databricks', 'linnworks_stock_transactions') }} o
INNER JOIN duplicate_ids d
    ON o.pk_stock_transaction_id = d.pk_stock_transaction_id
ORDER BY o.pk_stock_transaction_id;
