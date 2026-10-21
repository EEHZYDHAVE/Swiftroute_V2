{{
    config(
        materialized = 'table',
        schema = 'silver_b',
        alias = 'silver_business_fufillment',
        file_format = 'delta'
    )
}}

/*
    Silver Business: Fulfillment operations

    Grain: one row per Linnworks stock transaction, which represents an
    item/SKU-level inventory movement where the source transaction provides it.

    Source transaction IDs are preserved for lineage. Operational quantities
    remain signed. No historical inventory balance is inferred from the current
    stock-item snapshot.

    QuickBooks is used only to flag whether a client-month has a fulfillment/
    pick invoice line. Invoice totals, invoice-line amounts, balances, and
    payments are intentionally not copied onto transaction rows because those
    measures have a higher grain and would be repeated across the transaction
    rows for the same client-month.

    A contract rate may be exposed as context, but no estimated transaction
    charge is calculated: current data does not establish that individual stock
    movement quantities are the units used for the billed pick-and-pack service.
*/

WITH transaction_source AS (
    SELECT
        CAST(t.stock_transaction_id AS STRING) AS stock_transaction_id,
        CAST(t.source_stock_transaction_id AS STRING) AS source_stock_transaction_id,
        CAST(t.source_stock_item_id AS STRING) AS source_stock_item_id,
        CAST(t.stock_item_id AS STRING) AS transaction_stock_item_id,
        CAST(t.sku AS STRING) AS transaction_sku,
        CAST(t.item_title AS STRING) AS transaction_item_title,
        CAST(t.transaction_type AS STRING) AS source_transaction_type,
        CAST(t.transaction_note AS STRING) AS transaction_note,
        TRY_CAST(t.quantity AS BIGINT) AS quantity_change,
        CAST(t.source_order_reference AS STRING) AS source_order_reference,
        CAST(t.order_reference AS STRING) AS order_reference,
        CAST(t.order_id AS STRING) AS source_mapped_order_id,
        COALESCE(TRY_CAST(t.is_deleted_order_reference AS BOOLEAN), FALSE)
            AS is_deleted_order_reference,
        CAST(t.location AS STRING) AS warehouse_location,
        CAST(t.bin_rack AS STRING) AS bin_rack,
        CAST(t.transaction_at_utc AS TIMESTAMP) AS transaction_at_utc,
        TRY_CAST(t.stock_value AS DECIMAL(18, 2)) AS source_stock_value,
        CASE
            WHEN t.order_id IS NULL
              OR TRIM(CAST(t.order_id AS STRING)) = ''
              OR UPPER(TRIM(CAST(t.order_id AS STRING))) = 'N/A'
            THEN CAST(NULL AS STRING)
            ELSE TRIM(CAST(t.order_id AS STRING))
        END AS order_join_key
    FROM {{ ref('linnworks_stock_transaction_t') }} t
),

orders_ranked AS (
    SELECT
        o.*,
        ROW_NUMBER() OVER (
            PARTITION BY o.order_id
            ORDER BY
                o.order_activity_at_utc DESC NULLS LAST,
                o.order_processed_at_utc DESC NULLS LAST,
                o.received_at_utc DESC NULLS LAST,
                o.source_order_id ASC
        ) AS order_row_rank
    FROM {{ ref('linnworks_orders_t') }} o
    WHERE o.order_id IS NOT NULL
      AND TRIM(o.order_id) <> ''
      AND UPPER(TRIM(o.order_id)) <> 'N/A'
),

orders_one_row AS (
    SELECT *
    FROM orders_ranked
    WHERE order_row_rank = 1
),

stock_items_ranked AS (
    SELECT
        si.*,
        ROW_NUMBER() OVER (
            PARTITION BY si.stock_item_id
            ORDER BY
                si.source_updated_at_utc DESC NULLS LAST,
                si.stock_item_id ASC
        ) AS stock_item_row_rank
    FROM {{ ref('linnworks_stock_items_t') }} si
    WHERE si.stock_item_id IS NOT NULL
      AND TRIM(si.stock_item_id) <> ''
      AND UPPER(TRIM(si.stock_item_id)) <> 'N/A'
),

stock_items_one_row AS (
    SELECT *
    FROM stock_items_ranked
    WHERE stock_item_row_rank = 1
),

accounts_ranked AS (
    SELECT
        a.*,
        CASE
            WHEN a.client_id IS NULL
              OR TRIM(a.client_id) = ''
              OR UPPER(TRIM(a.client_id)) = 'N/A'
            THEN NULL
            ELSE TRIM(a.client_id)
        END AS normalized_client_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                CASE
                    WHEN a.client_id IS NULL
                      OR TRIM(a.client_id) = ''
                      OR UPPER(TRIM(a.client_id)) = 'N/A'
                    THEN NULL
                    ELSE TRIM(a.client_id)
                END
            ORDER BY
                a.source_updated_at_utc DESC NULLS LAST,
                a.account_id ASC
        ) AS account_row_rank
    FROM {{ ref('salesforce_accounts_t') }} a
),

accounts_one_row AS (
    SELECT *
    FROM accounts_ranked
    WHERE normalized_client_id IS NOT NULL
      AND account_row_rank = 1
),

invoice_pick_periods AS (
    SELECT
        TRIM(i.client_id) AS client_id,
        TRIM(i.billing_period) AS billing_period,
        MAX(
            CASE
                WHEN LOWER(COALESCE(line.sales_item_line_detail.item_ref.name, ''))
                         LIKE '%fulfillment%'
                  OR LOWER(COALESCE(line.sales_item_line_detail.item_ref.name, ''))
                         LIKE '%pick%'
                  OR LOWER(COALESCE(line.description, '')) LIKE '%fulfillment%'
                  OR LOWER(COALESCE(line.description, '')) LIKE '%pick%'
                THEN 1
                ELSE 0
            END
        ) AS has_fulfillment_pick_invoice_line
    FROM {{ ref('quickbooks_invoices_t') }} i
    LATERAL VIEW OUTER EXPLODE(i.lines) exploded_lines AS line
    WHERE i.client_id IS NOT NULL
      AND TRIM(i.client_id) <> ''
      AND UPPER(TRIM(i.client_id)) <> 'N/A'
      AND i.billing_period IS NOT NULL
      AND TRIM(i.billing_period) <> ''
      AND UPPER(TRIM(i.billing_period)) <> 'N/A'
    GROUP BY TRIM(i.client_id), TRIM(i.billing_period)
),

operational_join AS (
    SELECT
        t.*,

        o.order_id AS matched_order_id,
        o.source_order_id AS matched_source_order_id,
        o.num_order_id,
        o.reference_num,
        o.external_reference,
        o.secondary_reference,
        o.channel AS order_channel,
        o.sales_source AS order_sales_source,
        o.sub_source AS order_sub_source,
        o.site_code AS order_site_code,
        o.fulfillment_location_name AS order_fulfillment_location,
        o.received_at_utc AS order_received_at_utc,
        o.order_processed_at_utc,
        o.dispatched_at_utc AS order_dispatched_at_utc,
        o.order_status_code,
        o.lock_for_shipping AS order_locked_for_shipping,
        o.postal_service_name,
        o.tracking_number,
        o.shipping_vendor,
        o.pick_duration_mins,
        o.customer_profile_conflict,
        o.items AS order_items,
        CASE
            WHEN o.client_id IS NULL
              OR TRIM(o.client_id) = ''
              OR UPPER(TRIM(o.client_id)) = 'N/A'
            THEN NULL
            ELSE TRIM(o.client_id)
        END AS order_client_id,
        CASE
            WHEN o.client_name IS NULL OR TRIM(o.client_name) = '' THEN NULL
            ELSE TRIM(o.client_name)
        END AS linnworks_order_client_name,

        si.stock_item_id AS matched_stock_item_id,
        si.source_stock_item_id AS matched_source_stock_item_id,
        si.sku_normalized,
        si.item_number,
        si.barcode_number,
        si.item_title AS stock_item_title,
        si.category_name AS product_category,
        si.package_group_name AS product_package_group,
        si.weight AS product_weight,
        si.width AS product_width,
        si.height AS product_height,
        si.depth AS product_depth,
        si.creation_date_utc AS product_created_at_utc,
        si.source_updated_at_utc AS product_source_updated_at_utc,
        CASE
            WHEN si.client_id IS NULL
              OR TRIM(si.client_id) = ''
              OR UPPER(TRIM(si.client_id)) = 'N/A'
            THEN NULL
            ELSE TRIM(si.client_id)
        END AS stock_item_client_id,
        CASE
            WHEN si.client_name IS NULL OR TRIM(si.client_name) = '' THEN NULL
            ELSE TRIM(si.client_name)
        END AS linnworks_stock_item_client_name,

        CASE
            WHEN si.stock_item_id IS NOT NULL THEN 'MATCHED'
            ELSE 'UNMATCHED'
        END AS stock_item_match_status,

        CASE
            WHEN UPPER(TRIM(COALESCE(t.source_transaction_type, ''))) <> 'DISPATCH'
                THEN 'NOT_APPLICABLE'
            WHEN t.is_deleted_order_reference THEN 'DELETED_OR_REFERENCE_ONLY'
            WHEN t.order_join_key IS NULL THEN 'UNMATCHED'
            WHEN o.order_id IS NOT NULL THEN 'MATCHED'
            ELSE 'UNMATCHED'
        END AS order_match_status,

        CASE
            WHEN UPPER(TRIM(COALESCE(t.source_transaction_type, ''))) <> 'DISPATCH'
                THEN 'NOT_APPLICABLE'
            WHEN o.order_id IS NULL THEN 'NOT_CHECKED_NO_ORDER'
            WHEN COALESCE(
                SIZE(
                    FILTER(
                        o.items,
                        item ->
                            (t.transaction_stock_item_id IS NOT NULL
                             AND item.stock_item_id = t.transaction_stock_item_id)
                            OR
                            (t.transaction_sku IS NOT NULL
                             AND item.sku = t.transaction_sku)
                    )
                ),
                0
            ) > 0 THEN 'MATCHED'
            ELSE 'MISMATCH'
        END AS order_item_match_status

    FROM transaction_source t
    LEFT JOIN orders_one_row o
        ON t.order_join_key = o.order_id
    LEFT JOIN stock_items_one_row si
        ON t.transaction_stock_item_id = si.stock_item_id
),

client_resolved AS (
    SELECT
        o.*,
        CASE
            WHEN UPPER(TRIM(COALESCE(o.source_transaction_type, ''))) = 'DISPATCH'
             AND o.order_client_id IS NOT NULL
                THEN o.order_client_id
            WHEN o.stock_item_client_id IS NOT NULL
                THEN o.stock_item_client_id
            WHEN o.order_client_id IS NOT NULL
                THEN o.order_client_id
            ELSE NULL
        END AS resolved_client_id,
        CASE
            WHEN UPPER(TRIM(COALESCE(o.source_transaction_type, ''))) = 'DISPATCH'
             AND o.order_client_id IS NOT NULL
                THEN 'ORDER'
            WHEN o.stock_item_client_id IS NOT NULL
                THEN 'STOCK_ITEM'
            WHEN o.order_client_id IS NOT NULL
                THEN 'ORDER_FALLBACK'
            ELSE 'UNRESOLVED'
        END AS client_attribution_method,
        CASE
            WHEN o.order_client_id IS NOT NULL
             AND o.stock_item_client_id IS NOT NULL
             AND o.order_client_id <> o.stock_item_client_id
                THEN TRUE
            ELSE FALSE
        END AS client_attribution_mismatch,
        CASE
            WHEN UPPER(TRIM(COALESCE(o.source_transaction_type, ''))) = 'RECEIPT'
                THEN 'INBOUND'
            WHEN UPPER(TRIM(COALESCE(o.source_transaction_type, ''))) = 'DISPATCH'
                THEN 'DISPATCH'
            WHEN UPPER(TRIM(COALESCE(o.source_transaction_type, ''))) = 'ADJUSTMENT'
                THEN 'ADJUSTMENT'
            ELSE 'UNMAPPED'
        END AS fulfillment_transaction_type,
        CASE
            WHEN o.quantity_change > 0 THEN 'INCREASE'
            WHEN o.quantity_change < 0 THEN 'DECREASE'
            WHEN o.quantity_change = 0 THEN 'NO_CHANGE'
            ELSE 'UNKNOWN'
        END AS inventory_movement_direction,
        CAST(o.transaction_at_utc AS DATE) AS transaction_date,
        DATE_FORMAT(o.transaction_at_utc, 'yyyy-MM') AS activity_month
    FROM operational_join o
),

client_enriched AS (
    SELECT
        c.*,
        a.account_id AS salesforce_account_id,
        a.account_name AS salesforce_account_name,
        a.account_type AS salesforce_account_type,
        a.industry AS client_industry,
        a.account_tier AS client_tier,
        a.primary_city AS client_primary_city,
        a.is_fulfillment_client,
        a.contracted_monthly_volume AS account_contracted_monthly_volume,
        a.contract_type AS account_contract_type
    FROM client_resolved c
    LEFT JOIN accounts_one_row a
        ON c.resolved_client_id = a.normalized_client_id
),

contract_candidates AS (
    SELECT
        c.*,
        ct.contract_id AS matched_contract_id,
        ct.contract_status AS matched_contract_status,
        ct.start_date AS matched_contract_start_date,
        ct.end_date AS matched_contract_end_date,
        ct.contract_type AS matched_contract_type,
        ct.committed_monthly_volume AS matched_contract_committed_monthly_volume,
        ct.discount_rate AS matched_contract_discount_rate,
        ct.net_payment_terms AS matched_contract_net_payment_terms,
        ct.primary_city AS matched_contract_primary_city,
        ROW_NUMBER() OVER (
            PARTITION BY c.stock_transaction_id
            ORDER BY
                CASE WHEN ct.contract_id IS NULL THEN 1 ELSE 0 END,
                ct.start_date DESC NULLS LAST,
                ct.signed_date DESC NULLS LAST,
                ct.contract_id ASC
        ) AS contract_rank
    FROM client_enriched c
    LEFT JOIN {{ ref('salesforce_contracts_t') }} ct
        ON c.resolved_client_id IS NOT NULL
       AND ct.client_id = c.resolved_client_id
       AND c.transaction_date IS NOT NULL
       AND ct.start_date <= c.transaction_date
       AND (ct.end_date IS NULL OR ct.end_date >= c.transaction_date)
),

selected_contracts AS (
    SELECT *
    FROM contract_candidates
    WHERE contract_rank = 1
),

rate_candidates AS (
    SELECT
        c.*,
        r.contract_rate_id AS matched_contract_rate_id,
        r.service_type AS matched_rate_service_type,
        r.rate_unit AS matched_rate_unit,
        r.base_rate AS matched_rate_base_rate,
        r.discount_rate AS matched_rate_discount_rate,
        r.net_rate AS matched_rate_net_rate,
        r.effective_date AS matched_rate_effective_date,
        ROW_NUMBER() OVER (
            PARTITION BY c.stock_transaction_id
            ORDER BY
                CASE WHEN r.contract_rate_id IS NULL THEN 1 ELSE 0 END,
                r.effective_date DESC NULLS LAST,
                r.contract_rate_id ASC
        ) AS rate_rank
    FROM selected_contracts c
    LEFT JOIN {{ ref('salesforce_contract_rates_t') }} r
        ON c.fulfillment_transaction_type = 'DISPATCH'
       AND c.matched_contract_id IS NOT NULL
       AND r.contract_id = c.matched_contract_id
       AND r.client_id = c.resolved_client_id
       AND LOWER(TRIM(r.service_type)) = 'fulfillment_pick'
       AND LOWER(TRIM(r.rate_unit)) = 'per_unit'
       AND COALESCE(r.is_zone_specific, FALSE) = FALSE
       AND c.transaction_date IS NOT NULL
       AND r.effective_date <= c.transaction_date
),

selected_rates AS (
    SELECT *
    FROM rate_candidates
    WHERE rate_rank = 1
)

SELECT
    /* Transaction identity and source lineage */
    COALESCE(stock_transaction_id, 'N/A') AS fulfillment_transaction_id,
    COALESCE(source_stock_transaction_id, 'N/A') AS source_stock_transaction_id,
    COALESCE(source_stock_item_id, 'N/A') AS source_stock_item_id,

    /* Event timing and business classification */
    transaction_at_utc,
    transaction_date,
    COALESCE(activity_month, 'N/A') AS activity_month,
    COALESCE(source_transaction_type, 'N/A') AS source_transaction_type,
    fulfillment_transaction_type,
    inventory_movement_direction,
    quantity_change,
    CASE WHEN quantity_change IS NULL THEN CAST(NULL AS BIGINT)
         ELSE ABS(quantity_change)
    END AS quantity_moved,
    source_stock_value,
    COALESCE(transaction_note, 'N/A') AS transaction_note,
    COALESCE(warehouse_location, 'N/A') AS warehouse_location,
    COALESCE(bin_rack, 'N/A') AS bin_rack,

    /* Client identity and Salesforce context */
    COALESCE(resolved_client_id, 'N/A') AS client_id,
    COALESCE(
        NULLIF(TRIM(salesforce_account_name), ''),
        NULLIF(TRIM(linnworks_order_client_name), ''),
        NULLIF(TRIM(linnworks_stock_item_client_name), ''),
        'N/A'
    ) AS client_name,
    COALESCE(salesforce_account_id, 'N/A') AS account_id,
    COALESCE(salesforce_account_type, 'N/A') AS account_type,
    COALESCE(client_industry, 'N/A') AS client_industry,
    COALESCE(client_tier, 'N/A') AS client_tier,
    COALESCE(client_primary_city, 'N/A') AS client_primary_city,
    is_fulfillment_client,
    account_contracted_monthly_volume,
    COALESCE(account_contract_type, 'N/A') AS account_contract_type,
    client_attribution_method,
    client_attribution_mismatch,

    /* Product/SKU context */
    COALESCE(matched_stock_item_id, transaction_stock_item_id, 'N/A') AS stock_item_id,
    COALESCE(sku_normalized, transaction_sku, 'N/A') AS sku,
    COALESCE(transaction_sku, 'N/A') AS source_transaction_sku,
    COALESCE(
        NULLIF(TRIM(stock_item_title), ''),
        NULLIF(TRIM(transaction_item_title), ''),
        'N/A'
    ) AS product_name,
    COALESCE(item_number, 'N/A') AS item_number,
    COALESCE(barcode_number, 'N/A') AS barcode_number,
    COALESCE(product_category, 'N/A') AS product_category,
    COALESCE(product_package_group, 'N/A') AS product_package_group,
    product_weight,
    product_width,
    product_height,
    product_depth,
    product_created_at_utc,
    product_source_updated_at_utc,
    stock_item_match_status,

    /* Order context; retail order values and end-customer PII are excluded */
    COALESCE(source_mapped_order_id, matched_order_id, 'N/A') AS order_id,
    COALESCE(matched_source_order_id, 'N/A') AS source_order_id,
    COALESCE(source_order_reference, 'N/A') AS source_order_reference,
    COALESCE(order_reference, 'N/A') AS order_reference,
    is_deleted_order_reference,
    order_match_status,
    order_item_match_status,
    COALESCE(reference_num, 'N/A') AS order_reference_num,
    COALESCE(external_reference, 'N/A') AS order_external_reference,
    COALESCE(secondary_reference, 'N/A') AS order_secondary_reference,
    COALESCE(order_channel, 'N/A') AS order_channel,
    COALESCE(order_sales_source, 'N/A') AS order_sales_source,
    COALESCE(order_sub_source, 'N/A') AS order_sub_source,
    COALESCE(order_site_code, 'N/A') AS order_site_code,
    COALESCE(order_fulfillment_location, 'N/A') AS order_fulfillment_location,
    order_received_at_utc,
    order_processed_at_utc,
    order_dispatched_at_utc,
    order_status_code,
    order_locked_for_shipping,
    COALESCE(postal_service_name, 'N/A') AS postal_service_name,
    COALESCE(tracking_number, 'N/A') AS tracking_number,
    COALESCE(shipping_vendor, 'N/A') AS shipping_vendor,
    pick_duration_mins,
    customer_profile_conflict,

    /* Contract context as of the inventory movement date */
    COALESCE(matched_contract_id, 'N/A') AS contract_id,
    COALESCE(matched_contract_status, 'N/A') AS contract_status,
    matched_contract_start_date AS contract_start_date,
    matched_contract_end_date AS contract_end_date,
    COALESCE(matched_contract_type, 'N/A') AS contract_type,
    matched_contract_committed_monthly_volume AS contract_committed_monthly_volume,
    matched_contract_discount_rate AS contract_discount_rate,
    matched_contract_net_payment_terms AS contract_net_payment_terms_days,
    COALESCE(matched_contract_primary_city, 'N/A') AS contract_primary_city,
    CASE
        WHEN matched_contract_id IS NOT NULL THEN 'MATCHED_ACTIVE_ON_TRANSACTION_DATE'
        ELSE 'NO_ACTIVE_CONTRACT_ON_TRANSACTION_DATE'
    END AS contract_match_status,

    /* Pricing context only: no transaction charge is inferred */
    COALESCE(matched_contract_rate_id, 'N/A') AS fulfillment_pick_rate_id,
    COALESCE(matched_rate_service_type, 'N/A') AS fulfillment_pick_service_type,
    COALESCE(matched_rate_unit, 'N/A') AS fulfillment_pick_rate_unit,
    matched_rate_base_rate AS fulfillment_pick_base_rate,
    matched_rate_discount_rate AS fulfillment_pick_discount_rate,
    matched_rate_net_rate AS fulfillment_pick_net_rate,
    matched_rate_effective_date AS fulfillment_pick_rate_effective_date,
    CASE
        WHEN fulfillment_transaction_type <> 'DISPATCH'
            THEN 'NOT_APPLICABLE_TO_TRANSACTION_TYPE'
        WHEN matched_contract_id IS NULL
            THEN 'NO_ACTIVE_CONTRACT'
        WHEN matched_contract_rate_id IS NULL
            THEN 'NO_APPLICABLE_FULFILLMENT_PICK_RATE'
        ELSE 'RATE_CONTEXT_AVAILABLE_NOT_TRANSACTION_PRICED'
    END AS fulfillment_rate_context_status,

    /* Billing-period presence indicator only; not a transaction allocation */
    COALESCE(ip.has_fulfillment_pick_invoice_line, 0) = 1
        AS has_fulfillment_pick_invoice_for_client_month,

    CURRENT_TIMESTAMP() AS processed_at_utc

FROM selected_rates sr
LEFT JOIN invoice_pick_periods ip
    ON sr.resolved_client_id = ip.client_id
   AND sr.activity_month = ip.billing_period
