{{
    config(
        schema = 'gold_fulfillment',
        alias = 'fact_fulfillment_transaction'
    )
}}

/*
    Gold Fulfillment Operations — transaction fact.

    Grain: one row per Silver Business fulfillment transaction (one Linnworks
    stock transaction). Quantity measures describe stock movement, not billable
    pick quantities or delivery counts.

    Dimension keys are stable business keys because these models are views:
      date_key       = YYYYMMDD integer, or -1 for unknown date
      customer_key   = shared client_id, or N/A
      product_key    = stock_item_id, or N/A
      warehouse_key  = normalized inventory location, or N/A
      contract_key   = contract_id active on transaction date, or N/A

    Invoice presence is retained as a non-additive Boolean context flag. Invoice
    amounts and contract rate amounts are not copied onto transaction rows.
*/

WITH source_transactions AS (
    SELECT *
    FROM {{ ref('silver_business_fufillment') }}
)

SELECT
    /* Event identity and foreign keys */
    fulfillment_transaction_id,
    source_stock_transaction_id,
    source_stock_item_id,
    CASE
        WHEN transaction_date IS NULL THEN -1
        ELSE CAST(DATE_FORMAT(CAST(transaction_date AS DATE), 'yyyyMMdd') AS INT)
    END AS date_key,
    CASE
        WHEN client_id IS NULL OR TRIM(client_id) = '' THEN 'N/A'
        ELSE TRIM(client_id)
    END AS customer_key,
    CASE
        WHEN stock_item_id IS NULL OR TRIM(stock_item_id) = '' THEN 'N/A'
        ELSE TRIM(stock_item_id)
    END AS product_key,
    CASE
        WHEN warehouse_location IS NULL OR TRIM(warehouse_location) = '' THEN 'N/A'
        WHEN UPPER(TRIM(warehouse_location)) = 'N/A' THEN 'N/A'
        ELSE UPPER(TRIM(warehouse_location))
    END AS warehouse_key,
    CASE
        WHEN contract_id IS NULL OR TRIM(contract_id) = '' THEN 'N/A'
        ELSE TRIM(contract_id)
    END AS contract_key,

    /* Event timing and movement measures */
    transaction_at_utc,
    transaction_date,
    activity_month,
    source_transaction_type,
    fulfillment_transaction_type,
    inventory_movement_direction,
    quantity_change,
    quantity_moved,
    source_stock_value,
    transaction_note,

    /* Inventory location detail */
    warehouse_location,
    bin_rack,

    /* Degenerate order identifiers and order context */
    order_id,
    source_order_id,
    source_order_reference,
    order_reference,
    order_reference_num,
    order_external_reference,
    order_secondary_reference,
    order_channel,
    order_sales_source,
    order_sub_source,
    order_site_code,
    order_fulfillment_location,
    order_received_at_utc,
    order_processed_at_utc,
    order_dispatched_at_utc,
    order_status_code,
    order_locked_for_shipping,
    postal_service_name,
    tracking_number,
    shipping_vendor,
    pick_duration_mins,

    /* Operational quality indicators */
    client_attribution_method,
    client_attribution_mismatch,
    stock_item_match_status,
    order_match_status,
    order_item_match_status,
    contract_match_status,
    fulfillment_rate_context_status,
    fulfillment_pick_rate_id,
    fulfillment_pick_service_type,
    fulfillment_pick_rate_unit,
    fulfillment_pick_rate_effective_date,
    has_fulfillment_pick_invoice_for_client_month,
    customer_profile_conflict,
    is_deleted_order_reference,

    /* Audit */
    processed_at_utc
FROM source_transactions
