{{ config(materialized='view', schema='gold_finance') }}

/*
Purpose:
  Normalize QuickBooks invoice lines, payment receipts/payment applications, and
  expense lines for reporting while keeping the financial roles distinct.

Grain:
  - One row per invoice line (or a single header fallback when the invoice has no lines).
  - One row per payment header for cash received, plus one row per payment application line.
  - One row per expense line (or a single header fallback when the expense has no lines).

Do not sum `amount` across all amount_role values indiscriminately:
  billed_revenue, cash_received, payment_application, unresolved_payment_line, and
  operating_expense are separate business measures. Cash received is not additional revenue.
*/

WITH accounts_ranked AS (
    SELECT
        TRIM(client_id) AS client_id,
        primary_city,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(client_id)
            ORDER BY source_updated_at_utc DESC NULLS LAST, account_id
        ) AS row_num
    FROM {{ ref('salesforce_accounts_t') }}
    WHERE client_id IS NOT NULL
      AND LOWER(TRIM(client_id)) NOT IN ('', 'n/a', 'null')
),

accounts AS (
    SELECT client_id, primary_city
    FROM accounts_ranked
    WHERE row_num = 1
),

invoice_ids AS (
    SELECT DISTINCT invoice_id
    FROM {{ ref('quickbooks_invoices_t') }}
    WHERE invoice_id IS NOT NULL
),

invoice_lines_expanded AS (
    SELECT i.*, line_position, line
    FROM {{ ref('quickbooks_invoices_t') }} i
    LATERAL VIEW OUTER POSEXPLODE(i.lines) exploded_invoice_lines AS line_position, line
),

payment_lines_expanded AS (
    SELECT p.*, line_position, line
    FROM {{ ref('quickbooks_payments_t') }} p
    LATERAL VIEW OUTER POSEXPLODE(p.lines) exploded_payment_lines AS line_position, line
),

expense_lines_expanded AS (
    SELECT e.*, line_position, line
    FROM {{ ref('quickbooks_expenses_t') }} e
    LATERAL VIEW OUTER POSEXPLODE(e.lines) exploded_expense_lines AS line_position, line
),

invoice_line_rows AS (
    SELECT
        SHA2(CONCAT_WS('||', 'invoice', i.invoice_id, COALESCE(CAST(line_position AS STRING), 'HEADER_FALLBACK')), 256) AS transaction_line_key,
        'invoice' AS transaction_type,
        CASE WHEN line_position IS NULL THEN 'transaction_header_fallback' ELSE 'transaction_line' END AS record_level,
        'billed_revenue' AS amount_role,
        i.invoice_id AS transaction_id,
        i.invoice_id AS source_transaction_id,
        COALESCE(line.line_id, CONCAT('INVOICE_LINE_', COALESCE(CAST(line_position AS STRING), 'HEADER'))) AS source_line_id,
        CAST(i.transaction_date AS DATE) AS transaction_date,
        TRUNC(CAST(i.transaction_date AS DATE), 'MM') AS accounting_month,
        CAST(COALESCE(line.sales_item_line_detail.service_date, i.transaction_date) AS DATE) AS service_date,
        i.billing_period AS source_billing_period,
        CAST(i.due_date AS DATE) AS due_date,
        CASE
            WHEN i.client_id IS NULL OR LOWER(TRIM(i.client_id)) IN ('', 'n/a', 'null') THEN NULL
            ELSE TRIM(i.client_id)
        END AS client_id,
        i.customer_name AS customer_name,
        a.primary_city,
        CAST(NULL AS STRING) AS vendor_name,
        CAST(NULL AS STRING) AS vendor_id,
        CAST(NULL AS STRING) AS accounting_account_id,
        CAST(NULL AS STRING) AS accounting_account_name,
        CAST(NULL AS STRING) AS transaction_account_id,
        CAST(NULL AS STRING) AS transaction_account_name,
        COALESCE(
            NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''),
            NULLIF(TRIM(line.description), ''),
            line.detail_type
        ) AS source_category_label,
        CASE
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'next.?day' THEN 'next_day'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'same.?day' THEN 'same_day'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'return' THEN 'returns'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'distribution.?run' THEN 'distribution_run'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'fulfillment.*pick|pick.*pack' THEN 'fulfillment_pick'
            WHEN LOWER(TRIM(COALESCE(NULLIF(TRIM(line.sales_item_line_detail.item_ref.name), ''), NULLIF(TRIM(line.description), ''), line.detail_type))) RLIKE 'warehouse.*stor|storage' THEN 'storage'
            ELSE 'unclassified_revenue'
        END AS service_type,
        CAST(NULL AS STRING) AS expense_category,
        CAST(line.detail_type AS STRING) AS source_detail_type,
        CAST(NULL AS STRING) AS linked_transaction_id,
        CAST(NULL AS STRING) AS linked_transaction_type,
        CAST(NULL AS STRING) AS linked_invoice_id,
        CAST(NULL AS STRING) AS application_link_status,
        CAST(NULL AS DECIMAL(18, 2)) AS unapplied_amount,
        CAST(CASE WHEN line_position IS NULL THEN i.total_amount ELSE line.amount END AS DECIMAL(18, 2)) AS amount,
        i.currency_code,
        'quickbooks_invoice_line' AS source_record_type,
        i.source_sync_token,
        CASE WHEN line_position IS NULL THEN TRUE ELSE FALSE END AS uses_header_amount_fallback
    FROM invoice_lines_expanded i
    LEFT JOIN accounts a
        ON TRIM(i.client_id) = a.client_id
),

payment_headers AS (
    SELECT
        SHA2(CONCAT_WS('||', 'payment', p.payment_id, 'cash_receipt_header'), 256) AS transaction_line_key,
        'payment' AS transaction_type,
        'transaction_header' AS record_level,
        'cash_received' AS amount_role,
        p.payment_id AS transaction_id,
        p.payment_id AS source_transaction_id,
        CAST(NULL AS STRING) AS source_line_id,
        CAST(p.transaction_date AS DATE) AS transaction_date,
        TRUNC(CAST(p.transaction_date AS DATE), 'MM') AS accounting_month,
        CAST(NULL AS DATE) AS service_date,
        CAST(NULL AS STRING) AS source_billing_period,
        CAST(NULL AS DATE) AS due_date,
        CASE
            WHEN p.client_id IS NULL OR LOWER(TRIM(p.client_id)) IN ('', 'n/a', 'null') THEN NULL
            ELSE TRIM(p.client_id)
        END AS client_id,
        p.customer_name,
        a.primary_city,
        CAST(NULL AS STRING) AS vendor_name,
        CAST(NULL AS STRING) AS vendor_id,
        CAST(NULL AS STRING) AS accounting_account_id,
        CAST(NULL AS STRING) AS accounting_account_name,
        p.source_deposit_account_id AS transaction_account_id,
        p.deposit_account_name AS transaction_account_name,
        'cash_received' AS source_category_label,
        CAST(NULL AS STRING) AS service_type,
        CAST(NULL AS STRING) AS expense_category,
        CAST(NULL AS STRING) AS source_detail_type,
        CAST(NULL AS STRING) AS linked_transaction_id,
        CAST(NULL AS STRING) AS linked_transaction_type,
        CAST(NULL AS STRING) AS linked_invoice_id,
        'PAYMENT_HEADER_NOT_INVOICE_ALLOCATION' AS application_link_status,
        CAST(p.unapplied_amount AS DECIMAL(18, 2)) AS unapplied_amount,
        CAST(p.total_amount AS DECIMAL(18, 2)) AS amount,
        p.currency_code,
        'quickbooks_payment_header' AS source_record_type,
        p.source_sync_token,
        FALSE AS uses_header_amount_fallback
    FROM {{ ref('quickbooks_payments_t') }} p
    LEFT JOIN accounts a
        ON TRIM(p.client_id) = a.client_id
),

payment_application_rows AS (
    SELECT
        SHA2(CONCAT_WS('||', 'payment', p.payment_id, 'application_line', CAST(line_position AS STRING)), 256) AS transaction_line_key,
        'payment' AS transaction_type,
        'application_line' AS record_level,
        CASE
            WHEN SIZE(line.linked_transactions) = 1
             AND LOWER(TRIM(line.linked_transactions[0].transaction_type)) = 'invoice'
             AND linked_invoice.invoice_id IS NOT NULL
                THEN 'payment_application'
            ELSE 'unresolved_payment_line'
        END AS amount_role,
        p.payment_id AS transaction_id,
        p.payment_id AS source_transaction_id,
        CONCAT('PAYMENT_LINE_', CAST(line_position AS STRING)) AS source_line_id,
        CAST(p.transaction_date AS DATE) AS transaction_date,
        TRUNC(CAST(p.transaction_date AS DATE), 'MM') AS accounting_month,
        CAST(NULL AS DATE) AS service_date,
        CAST(NULL AS STRING) AS source_billing_period,
        CAST(NULL AS DATE) AS due_date,
        CASE
            WHEN p.client_id IS NULL OR LOWER(TRIM(p.client_id)) IN ('', 'n/a', 'null') THEN NULL
            ELSE TRIM(p.client_id)
        END AS client_id,
        p.customer_name,
        a.primary_city,
        CAST(NULL AS STRING) AS vendor_name,
        CAST(NULL AS STRING) AS vendor_id,
        CAST(NULL AS STRING) AS accounting_account_id,
        CAST(NULL AS STRING) AS accounting_account_name,
        p.source_deposit_account_id AS transaction_account_id,
        p.deposit_account_name AS transaction_account_name,
        'payment_application' AS source_category_label,
        CAST(NULL AS STRING) AS service_type,
        CAST(NULL AS STRING) AS expense_category,
        CAST(NULL AS STRING) AS source_detail_type,
        CASE WHEN SIZE(line.linked_transactions) = 1 THEN line.linked_transactions[0].transaction_id END AS linked_transaction_id,
        CASE WHEN SIZE(line.linked_transactions) = 1 THEN line.linked_transactions[0].transaction_type END AS linked_transaction_type,
        CASE
            WHEN SIZE(line.linked_transactions) = 1
             AND LOWER(TRIM(line.linked_transactions[0].transaction_type)) = 'invoice'
             AND linked_invoice.invoice_id IS NOT NULL
                THEN line.linked_transactions[0].transaction_id
            ELSE NULL
        END AS linked_invoice_id,
        CASE
            WHEN COALESCE(SIZE(line.linked_transactions), 0) = 0 THEN 'UNLINKED_PAYMENT_APPLICATION'
            WHEN SIZE(line.linked_transactions) > 1 THEN 'MULTIPLE_LINKS_AMOUNT_NOT_ALLOCATED'
            WHEN LOWER(TRIM(line.linked_transactions[0].transaction_type)) <> 'invoice' THEN 'LINKED_NON_INVOICE_TRANSACTION'
            WHEN linked_invoice.invoice_id IS NULL THEN 'LINKED_INVOICE_NOT_FOUND'
            ELSE 'ONE_INVOICE_LINK'
        END AS application_link_status,
        CAST(NULL AS DECIMAL(18, 2)) AS unapplied_amount,
        CAST(line.amount AS DECIMAL(18, 2)) AS amount,
        p.currency_code,
        'quickbooks_payment_application' AS source_record_type,
        p.source_sync_token,
        FALSE AS uses_header_amount_fallback
    FROM payment_lines_expanded p
    LEFT JOIN accounts a
        ON TRIM(p.client_id) = a.client_id
    LEFT JOIN invoice_ids linked_invoice
        ON SIZE(line.linked_transactions) = 1
       AND LOWER(TRIM(line.linked_transactions[0].transaction_type)) = 'invoice'
       AND line.linked_transactions[0].transaction_id = linked_invoice.invoice_id
    WHERE line_position IS NOT NULL
      AND (line.amount IS NOT NULL OR COALESCE(SIZE(line.linked_transactions), 0) > 0)
),

expense_line_rows AS (
    SELECT
        SHA2(CONCAT_WS('||', 'expense', e.expense_id, 'expense_line', COALESCE(CAST(line_position AS STRING), 'HEADER_FALLBACK')), 256) AS transaction_line_key,
        'expense' AS transaction_type,
        CASE WHEN line_position IS NULL THEN 'transaction_header_fallback' ELSE 'transaction_line' END AS record_level,
        'operating_expense' AS amount_role,
        e.expense_id AS transaction_id,
        e.source_expense_id AS source_transaction_id,
        COALESCE(line.line_id, CONCAT('EXPENSE_LINE_', COALESCE(CAST(line_position AS STRING), 'HEADER'))) AS source_line_id,
        CAST(e.transaction_date AS DATE) AS transaction_date,
        TRUNC(CAST(e.transaction_date AS DATE), 'MM') AS accounting_month,
        CAST(NULL AS DATE) AS service_date,
        CAST(NULL AS STRING) AS source_billing_period,
        CAST(NULL AS DATE) AS due_date,
        CAST(NULL AS STRING) AS client_id,
        CAST(NULL AS STRING) AS customer_name,
        CAST(NULL AS STRING) AS primary_city,
        e.entity_name AS vendor_name,
        e.source_entity_id AS vendor_id,
        line.account_based_expense_line_detail.account_ref.value AS accounting_account_id,
        line.account_based_expense_line_detail.account_ref.name AS accounting_account_name,
        e.source_account_id AS transaction_account_id,
        e.account_name AS transaction_account_name,
        COALESCE(
            NULLIF(TRIM(line.account_based_expense_line_detail.account_ref.name), ''),
            NULLIF(TRIM(line.detail_type), ''),
            'unclassified_expense'
        ) AS source_category_label,
        CAST(NULL AS STRING) AS service_type,
        COALESCE(
            NULLIF(TRIM(line.account_based_expense_line_detail.account_ref.name), ''),
            NULLIF(TRIM(line.detail_type), ''),
            'unclassified_expense'
        ) AS expense_category,
        CAST(line.detail_type AS STRING) AS source_detail_type,
        CAST(NULL AS STRING) AS linked_transaction_id,
        CAST(NULL AS STRING) AS linked_transaction_type,
        CAST(NULL AS STRING) AS linked_invoice_id,
        CAST(NULL AS STRING) AS application_link_status,
        CAST(NULL AS DECIMAL(18, 2)) AS unapplied_amount,
        CAST(
            CASE
                WHEN line_position IS NULL THEN e.total_amount
                ELSE line.amount
            END AS DECIMAL(18, 2)
        ) AS amount,
        e.currency_code,
        'quickbooks_expense_line' AS source_record_type,
        e.source_sync_token,
        CASE WHEN line_position IS NULL THEN TRUE ELSE FALSE END AS uses_header_amount_fallback
    FROM expense_lines_expanded e
)

SELECT * FROM invoice_line_rows
UNION ALL
SELECT * FROM payment_headers
UNION ALL
SELECT * FROM payment_application_rows
UNION ALL
SELECT * FROM expense_line_rows
