{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'source_order_id',
        file_format = 'delta'
    )
}}

/*
    Silver Technical: Linnworks processed orders

    Grain: one row per source pkOrderID.

    The source keeps the order's existing identifiers (NumOrderId, ReferenceNum,
    ExternalReference, SecondaryReference) and adds a readable order_id surrogate.
    Items remain an array of structs to preserve order grain.

    Customer-profile rule for duplicate pkOrderID values:
      - Count complete customer/address profiles within each order ID.
      - Keep one row from the most frequent profile.
      - If top profiles tie, choose deterministically and expose customer_profile_conflict.
        A tie is not evidence that the selected profile is the true customer.

    Incremental watermark: greatest of received, processed, and dispatched timestamps.
    This is an event-time proxy because the source has no dedicated last-modified field.
*/

WITH bronze_orders AS (
    SELECT *
    FROM {{ source('swiftroute_databricks', 'linnworks_orders') }}
),

source_order_rows AS (
    SELECT
        b.*,
        CAST(b.pk_order_id AS STRING) AS source_order_id,
        CAST(b.received_date AS TIMESTAMP) AS received_at_utc,
        CAST(b.processed_on AS TIMESTAMP) AS order_processed_at_utc,
        CAST(b.dispatched_on AS TIMESTAMP) AS dispatched_at_utc,
        GREATEST(
            CAST(b.received_date AS TIMESTAMP),
            CAST(b.processed_on AS TIMESTAMP),
            CAST(b.dispatched_on AS TIMESTAMP)
        ) AS order_activity_at_utc
    FROM bronze_orders AS b
),

customer_profile_keys AS (
    SELECT
        source_order_rows.*,
        TO_JSON(NAMED_STRUCT(
            'customer_name', COALESCE(NULLIF(TRIM(CAST(customer_name AS STRING)), ''), 'N/A'),
            'customer_email', COALESCE(NULLIF(TRIM(CAST(customer_email AS STRING)), ''), 'N/A'),
            'address_full_name', COALESCE(NULLIF(TRIM(CAST(address.full_name AS STRING)), ''), 'N/A'),
            'address_company', COALESCE(NULLIF(TRIM(CAST(address.company AS STRING)), ''), 'N/A'),
            'address1', COALESCE(NULLIF(TRIM(CAST(address.address1 AS STRING)), ''), 'N/A'),
            'address2', COALESCE(NULLIF(TRIM(CAST(address.address2 AS STRING)), ''), 'N/A'),
            'city', COALESCE(NULLIF(TRIM(CAST(address.city AS STRING)), ''), 'N/A'),
            'region', COALESCE(NULLIF(TRIM(CAST(address.region AS STRING)), ''), 'N/A'),
            'post_code', COALESCE(NULLIF(TRIM(CAST(address.post_code AS STRING)), ''), 'N/A'),
            'country', COALESCE(NULLIF(TRIM(CAST(address.country AS STRING)), ''), 'N/A'),
            'country_code', COALESCE(NULLIF(TRIM(CAST(address.country_code AS STRING)), ''), 'N/A'),
            'phone', COALESCE(NULLIF(TRIM(CAST(address.phone AS STRING)), ''), 'N/A')
        )) AS customer_profile_key
    FROM source_order_rows
),

customer_profile_counts AS (
    SELECT
        source_order_id,
        customer_profile_key,
        COUNT(*) AS profile_occurrences
    FROM customer_profile_keys
    GROUP BY source_order_id, customer_profile_key
),

maximum_profile_frequency AS (
    SELECT
        source_order_id,
        MAX(profile_occurrences) AS maximum_profile_occurrences
    FROM customer_profile_counts
    GROUP BY source_order_id
),

customer_profile_tie_summary AS (
    SELECT
        counts.source_order_id,
        SUM(CASE WHEN counts.profile_occurrences = maximum.maximum_profile_occurrences THEN 1 ELSE 0 END) AS number_of_top_profiles
    FROM customer_profile_counts AS counts
    INNER JOIN maximum_profile_frequency AS maximum
        ON maximum.source_order_id <=> counts.source_order_id
    GROUP BY counts.source_order_id
),

profiled_order_rows AS (
    SELECT
        profile.*,
        counts.profile_occurrences,
        ties.number_of_top_profiles,
        ROW_NUMBER() OVER (
            PARTITION BY profile.source_order_id
            ORDER BY
                counts.profile_occurrences DESC,
                profile.customer_profile_key ASC,
                profile.received_at_utc ASC NULLS LAST,
                profile.order_processed_at_utc ASC NULLS LAST,
                profile.dispatched_at_utc ASC NULLS LAST
        ) AS customer_profile_row_number
    FROM customer_profile_keys AS profile
    INNER JOIN customer_profile_counts AS counts
        ON counts.source_order_id <=> profile.source_order_id
       AND counts.customer_profile_key = profile.customer_profile_key
    INNER JOIN customer_profile_tie_summary AS ties
        ON ties.source_order_id <=> profile.source_order_id
),

deduplicated_source_orders AS (
    SELECT *
    FROM profiled_order_rows
    WHERE customer_profile_row_number = 1
),

eligible_source_orders AS (
    SELECT *
    FROM deduplicated_source_orders
    {% if is_incremental() %}
    WHERE order_activity_at_utc >= (
        SELECT COALESCE(
            MAX(target.order_activity_at_utc),
            CAST('1900-01-01 00:00:00' AS TIMESTAMP)
        )
        FROM {{ this }} AS target
    )
    {% endif %}
),

existing_order_mapping AS (
    {% if is_incremental() %}
    SELECT
        CAST(source_order_id AS STRING) AS source_order_id,
        CAST(order_id AS STRING) AS order_id
    FROM {{ this }}
    {% else %}
    SELECT
        CAST(NULL AS STRING) AS source_order_id,
        CAST(NULL AS STRING) AS order_id
    WHERE 1 = 0
    {% endif %}
),

current_max_order_number AS (
    SELECT COALESCE(
        MAX(TRY_CAST(NULLIF(REGEXP_EXTRACT(order_id, '^order_id_([0-9]+)$', 1), '') AS BIGINT)),
        0
    ) AS max_order_number
    FROM existing_order_mapping
),

new_order_assignments AS (
    SELECT
        eligible.source_order_id,
        CONCAT(
            'order_id_',
            CAST(
                current_max.max_order_number
                + ROW_NUMBER() OVER (ORDER BY eligible.source_order_id ASC)
                AS STRING
            )
        ) AS order_id
    FROM eligible_source_orders AS eligible
    LEFT ANTI JOIN existing_order_mapping AS existing
        ON existing.source_order_id <=> eligible.source_order_id
    CROSS JOIN current_max_order_number AS current_max
    WHERE eligible.source_order_id IS NOT NULL
),

order_id_mapping AS (
    SELECT source_order_id, order_id FROM existing_order_mapping
    UNION ALL
    SELECT source_order_id, order_id FROM new_order_assignments
),

stock_item_lookup AS (
    SELECT MAP_FROM_ENTRIES(
        COLLECT_LIST(NAMED_STRUCT(
            'key', CAST(source_stock_item_id AS STRING),
            'value', CAST(stock_item_id AS STRING)
        ))
    ) AS source_to_surrogate_map
    FROM {{ ref('linnworks_stock_items_t') }}
    WHERE source_stock_item_id IS NOT NULL
)

SELECT
    /* 1. Identity and order references: retain all original order reference columns. */
    key_mapping.order_id AS order_id,
    orders.source_order_id AS source_order_id,
    TRY_CAST(orders.num_order_id AS BIGINT) AS num_order_id,
    CASE WHEN orders.reference_num IS NULL OR TRIM(orders.reference_num) = '' THEN 'N/A' ELSE CAST(orders.reference_num AS STRING) END AS reference_num,
    CASE WHEN orders.external_reference IS NULL OR TRIM(orders.external_reference) = '' THEN 'N/A' ELSE CAST(orders.external_reference AS STRING) END AS external_reference,
    CASE WHEN orders.secondary_reference IS NULL OR TRIM(orders.secondary_reference) = '' THEN 'N/A' ELSE CAST(orders.secondary_reference AS STRING) END AS secondary_reference,

    /* 2. Sales channel and fulfillment location. */
    CASE WHEN orders.channel IS NULL OR TRIM(orders.channel) = '' THEN 'N/A' ELSE CAST(orders.channel AS STRING) END AS channel,
    CASE WHEN orders.source IS NULL OR TRIM(orders.source) = '' THEN 'N/A' ELSE CAST(orders.source AS STRING) END AS sales_source,
    CASE WHEN orders.sub_source IS NULL OR TRIM(orders.sub_source) = '' THEN 'N/A' ELSE CAST(orders.sub_source AS STRING) END AS sub_source,
    CASE WHEN orders.site_code IS NULL OR TRIM(orders.site_code) = '' THEN 'N/A' ELSE CAST(orders.site_code AS STRING) END AS site_code,
    CASE WHEN orders.fulfilment_location_name IS NULL OR TRIM(orders.fulfilment_location_name) = '' THEN 'N/A' ELSE CAST(orders.fulfilment_location_name AS STRING) END AS fulfillment_location_name,

    /* 3. Order lifecycle and incremental watermark. */
    orders.received_at_utc AS received_at_utc,
    orders.order_processed_at_utc AS order_processed_at_utc,
    orders.dispatched_at_utc AS dispatched_at_utc,
    orders.order_activity_at_utc AS order_activity_at_utc,

    /* 4. Customer profile and address. */
    CASE WHEN orders.customer_name IS NULL OR TRIM(orders.customer_name) = '' THEN 'N/A' ELSE CAST(orders.customer_name AS STRING) END AS customer_name,
    CASE WHEN orders.customer_email IS NULL OR TRIM(orders.customer_email) = '' THEN 'N/A' ELSE CAST(orders.customer_email AS STRING) END AS customer_email,
    CASE WHEN orders.address.full_name IS NULL OR TRIM(orders.address.full_name) = '' THEN 'N/A' ELSE CAST(orders.address.full_name AS STRING) END AS address_full_name,
    CASE WHEN orders.address.company IS NULL OR TRIM(orders.address.company) = '' THEN 'N/A' ELSE CAST(orders.address.company AS STRING) END AS address_company,
    CASE WHEN orders.address.address1 IS NULL OR TRIM(orders.address.address1) = '' THEN 'N/A' ELSE CAST(orders.address.address1 AS STRING) END AS address1,
    CASE WHEN orders.address.address2 IS NULL OR TRIM(orders.address.address2) = '' THEN 'N/A' ELSE CAST(orders.address.address2 AS STRING) END AS address2,
    CASE WHEN orders.address.city IS NULL OR TRIM(orders.address.city) = '' THEN 'N/A' ELSE CAST(orders.address.city AS STRING) END AS address_city,
    CASE WHEN orders.address.region IS NULL OR TRIM(orders.address.region) = '' THEN 'N/A' ELSE CAST(orders.address.region AS STRING) END AS address_region,
    CASE WHEN orders.address.post_code IS NULL OR TRIM(orders.address.post_code) = '' THEN 'N/A' ELSE CAST(orders.address.post_code AS STRING) END AS address_post_code,
    CASE WHEN orders.address.country IS NULL OR TRIM(orders.address.country) = '' THEN 'N/A' ELSE CAST(orders.address.country AS STRING) END AS address_country,
    CASE WHEN orders.address.country_code IS NULL OR TRIM(orders.address.country_code) = '' THEN 'N/A' ELSE CAST(orders.address.country_code AS STRING) END AS address_country_code,
    CASE WHEN orders.address.phone IS NULL OR TRIM(orders.address.phone) = '' THEN 'N/A' ELSE CAST(orders.address.phone AS STRING) END AS address_phone,
    (orders.number_of_top_profiles > 1) AS customer_profile_conflict,

    /* 5. Financial and payment fields. */
    TRY_CAST(orders.sub_total AS DECIMAL(18, 2)) AS subtotal,
    TRY_CAST(orders.postal_service_cost AS DECIMAL(18, 2)) AS postal_service_cost,
    TRY_CAST(orders.total_charge AS DECIMAL(18, 2)) AS total_charge,
    TRY_CAST(orders.tax_amount AS DECIMAL(18, 2)) AS tax_amount,
    CASE WHEN orders.currency IS NULL OR TRIM(orders.currency) = '' THEN 'N/A' ELSE CAST(orders.currency AS STRING) END AS currency,
    CASE WHEN orders.payment_method IS NULL OR TRIM(orders.payment_method) = '' THEN 'N/A' ELSE CAST(orders.payment_method AS STRING) END AS payment_method,
    CASE WHEN orders.payment_status IS NULL OR TRIM(orders.payment_status) = '' THEN 'N/A' ELSE CAST(orders.payment_status AS STRING) END AS payment_status,

    /* 6. General order information. */
    TRY_CAST(orders.general_info.status AS INT) AS order_status_code,
    CAST(orders.general_info.lock_for_shipping AS BOOLEAN) AS lock_for_shipping,
    TRY_CAST(orders.general_info.marker AS BIGINT) AS order_marker,
    CASE WHEN orders.general_info.notes IS NULL OR TRIM(orders.general_info.notes) = '' THEN 'N/A' ELSE CAST(orders.general_info.notes AS STRING) END AS order_notes,

    /* 7. Shipping details. */
    CASE WHEN orders.shipping_info.postal_service_name IS NULL OR TRIM(orders.shipping_info.postal_service_name) = '' THEN 'N/A' ELSE CAST(orders.shipping_info.postal_service_name AS STRING) END AS postal_service_name,
    CASE WHEN orders.shipping_info.tracking_number IS NULL OR TRIM(orders.shipping_info.tracking_number) = '' THEN 'N/A' ELSE CAST(orders.shipping_info.tracking_number AS STRING) END AS tracking_number,
    CASE WHEN orders.shipping_info.vendor IS NULL OR TRIM(orders.shipping_info.vendor) = '' THEN 'N/A' ELSE CAST(orders.shipping_info.vendor AS STRING) END AS shipping_vendor,
    CASE WHEN orders.shipping_info.postal_service_code IS NULL OR TRIM(orders.shipping_info.postal_service_code) = '' THEN 'N/A' ELSE CAST(orders.shipping_info.postal_service_code AS STRING) END AS postal_service_code,

    /* 8. Preserve order grain: each item remains a typed element inside the items array. */
    TRANSFORM(orders.items, item -> NAMED_STRUCT(
        'stock_item_id', ELEMENT_AT(stock_lookup.source_to_surrogate_map, CAST(item.stock_item_id AS STRING)),
        'source_stock_item_id', CAST(item.stock_item_id AS STRING),
        'sku', CASE WHEN item.sku IS NULL OR TRIM(CAST(item.sku AS STRING)) = '' THEN 'N/A' ELSE CAST(item.sku AS STRING) END,
        'item_title', CASE WHEN item.item_title IS NULL OR TRIM(CAST(item.item_title AS STRING)) = '' THEN 'N/A' ELSE CAST(item.item_title AS STRING) END,
        'quantity', TRY_CAST(item.quantity AS BIGINT),
        'unit_cost', TRY_CAST(item.unit_cost AS DECIMAL(18, 2)),
        'price_per_unit', TRY_CAST(item.price_per_unit AS DECIMAL(18, 2)),
        'line_total', TRY_CAST(item.line_total AS DECIMAL(18, 2)),
        'weight', TRY_CAST(item.weight AS DOUBLE),
        'is_composite', CAST(item.is_composite AS BOOLEAN),
        'bin_rack', CASE WHEN item.bin_rack IS NULL OR TRIM(CAST(item.bin_rack AS STRING)) = '' THEN 'N/A' ELSE CAST(item.bin_rack AS STRING) END
    )) AS items,

    /* 9. Swiftroute-specific attributes. */
    CASE WHEN orders._swiftroute_client_id IS NULL OR TRIM(orders._swiftroute_client_id) = '' THEN 'N/A' ELSE CAST(orders._swiftroute_client_id AS STRING) END AS client_id,
    CASE WHEN orders._swiftroute_client_name IS NULL OR TRIM(orders._swiftroute_client_name) = '' THEN 'N/A' ELSE CAST(orders._swiftroute_client_name AS STRING) END AS client_name,
    TRY_CAST(orders._swiftroute_pick_duration_mins AS BIGINT) AS pick_duration_mins,

    CURRENT_TIMESTAMP() AS processed_at_utc
FROM eligible_source_orders AS orders
LEFT JOIN order_id_mapping AS key_mapping
    ON key_mapping.source_order_id <=> orders.source_order_id
CROSS JOIN stock_item_lookup AS stock_lookup
