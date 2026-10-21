SELECT
    c.table_catalog,
    c.table_schema,
    c.table_name,
    t.table_type,
    c.ordinal_position,
    c.column_name,
    c.data_type,
    c.full_data_type,
    c.is_nullable,
    c.comment
FROM {{ source('swiftroute', 'information_schema_columns') }} AS c
LEFT JOIN {{ source('swiftroute', 'information_schema_tables') }} AS t
    ON  c.table_catalog = t.table_catalog
    AND c.table_schema = t.table_schema
    AND c.table_name = t.table_name
WHERE c.table_schema LIKE 'gold%'
  AND LEFT(c.table_name, 4) = 'dim_'
ORDER BY
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.ordinal_position;









SELECT
    c.table_catalog,
    c.table_schema,
    c.table_name,
    t.table_type,
    c.ordinal_position,
    c.column_name,
    c.data_type,
    c.full_data_type,
    c.is_nullable,
    c.comment
FROM {{ target.database }}.information_schema.columns AS c
LEFT JOIN {{ target.database }}.information_schema.tables AS t
    ON  c.table_catalog = t.table_catalog
    AND c.table_schema = t.table_schema
    AND c.table_name = t.table_name
WHERE c.table_schema LIKE 'gold%'
  AND LEFT(c.table_name, 4) = 'dim_'
ORDER BY
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.ordinal_position;













SELECT
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    c.full_data_type
FROM {{ target.database }}.information_schema.columns AS c
WHERE c.table_schema LIKE 'gold%'
  AND LEFT(c.table_name, 4) = 'dim_'
  AND c.data_type IN (
      'TIMESTAMP',
      'TIMESTAMP_NTZ',
      'DATE'
  )
ORDER BY
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.ordinal_position;










SELECT
    'gold_fulfillment.dim_contract' AS dimension_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT contract_key) AS distinct_keys,
    SUM(CASE WHEN contract_key IS NULL THEN 1 ELSE 0 END) AS null_keys,
    SUM(CASE WHEN source_updated_at_utc IS NULL THEN 1 ELSE 0 END)
        AS null_updated_at
FROM swiftroute.gold_fulfillment.dim_contract

UNION ALL

SELECT
    'gold_fulfillment.dim_customer',
    COUNT(*),
    COUNT(DISTINCT customer_key),
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN source_updated_at_utc IS NULL THEN 1 ELSE 0 END)
FROM swiftroute.gold_fulfillment.dim_customer

UNION ALL

SELECT
    'gold_fulfillment.dim_product',
    COUNT(*),
    COUNT(DISTINCT product_key),
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN product_source_updated_at_utc IS NULL THEN 1 ELSE 0 END)
FROM swiftroute.gold_fulfillment.dim_product;