{{ config(materialized='view', schema='gold_finance') }}

/*
Purpose:
  Monthly reporting view for recorded QuickBooks activity.

Grain:
  One row per accounting month, customer (where available), location (where
  reliably mapped), financial area, and reporting category.

Sign convention:
  - recorded_billed_revenue_amount preserves QuickBooks invoice-line signs.
  - operating_expense_amount preserves QuickBooks expense-line signs.
  - pnl_signed_amount adds invoice lines and subtracts expense-line amounts.
  - payment receipts and payment applications have their own metrics and always
    contribute zero to the P&L signed amount.

Revenue categories are based on QuickBooks line labels. Expense categories use the
source line's accounting AccountRef name; no vendor-name heuristics are applied.
*/

WITH source_lines AS (
    SELECT *
    FROM {{ ref('financial_transaction_lines') }}
    WHERE accounting_month IS NOT NULL
      AND (
          amount IS NOT NULL
          OR amount_role IN ('cash_received', 'payment_application', 'unresolved_payment_line')
      )
),

classified AS (
    SELECT
        accounting_month,
        client_id,
        customer_name,
        primary_city,
        transaction_type,
        transaction_id,
        amount_role,
        amount,
        unapplied_amount,
        CASE
            WHEN amount_role = 'billed_revenue' THEN 'REVENUE'
            WHEN amount_role = 'operating_expense' THEN 'OPERATING_EXPENSE'
            WHEN amount_role = 'cash_received' THEN 'COLLECTIONS'
            WHEN amount_role = 'payment_application' THEN 'COLLECTION_APPLICATIONS'
            WHEN amount_role = 'unresolved_payment_line' THEN 'PAYMENT_LINES_REQUIRING_REVIEW'
            ELSE 'OTHER'
        END AS financial_area,
        CASE
            WHEN amount_role = 'billed_revenue' THEN COALESCE(service_type, 'unclassified_revenue')
            WHEN amount_role = 'operating_expense' THEN COALESCE(expense_category, 'unclassified_expense')
            WHEN amount_role = 'cash_received' THEN 'cash_received'
            WHEN amount_role IN ('payment_application', 'unresolved_payment_line') THEN COALESCE(application_link_status, 'unclassified_payment_application')
            ELSE COALESCE(source_category_label, 'unclassified')
        END AS reporting_category
    FROM source_lines
),

monthly_aggregated AS (
    SELECT
        accounting_month,
        client_id,
        MAX(customer_name) AS customer_name,
        primary_city,
        financial_area,
        reporting_category,
        COUNT(DISTINCT transaction_id) AS source_transaction_count,
        COUNT(*) AS source_record_count,
        COUNT(DISTINCT CASE WHEN transaction_type = 'invoice' THEN transaction_id END) AS invoice_count,
        COUNT(DISTINCT CASE WHEN transaction_type = 'payment' AND amount_role = 'cash_received' THEN transaction_id END) AS payment_receipt_count,
        COUNT(DISTINCT CASE WHEN transaction_type = 'expense' THEN transaction_id END) AS expense_transaction_count,
        CAST(SUM(CASE WHEN amount_role = 'billed_revenue' THEN COALESCE(amount, 0) ELSE 0 END) AS DECIMAL(18, 2)) AS recorded_billed_revenue_amount,
        CAST(SUM(CASE WHEN amount_role = 'operating_expense' THEN COALESCE(amount, 0) ELSE 0 END) AS DECIMAL(18, 2)) AS operating_expense_amount,
        CAST(SUM(CASE WHEN amount_role = 'cash_received' THEN COALESCE(amount, 0) ELSE 0 END) AS DECIMAL(18, 2)) AS cash_received_amount,
        CAST(SUM(CASE WHEN amount_role = 'cash_received' THEN COALESCE(unapplied_amount, 0) ELSE 0 END) AS DECIMAL(18, 2)) AS unapplied_cash_amount,
        CAST(SUM(CASE WHEN amount_role = 'payment_application' THEN COALESCE(amount, 0) ELSE 0 END) AS DECIMAL(18, 2)) AS payment_application_amount,
        CAST(SUM(CASE WHEN amount_role = 'unresolved_payment_line' THEN COALESCE(amount, 0) ELSE 0 END) AS DECIMAL(18, 2)) AS unresolved_payment_line_amount,
        CAST(SUM(
            CASE
                WHEN amount_role = 'billed_revenue' THEN COALESCE(amount, 0)
                WHEN amount_role = 'operating_expense' THEN -COALESCE(amount, 0)
                ELSE 0
            END
        ) AS DECIMAL(18, 2)) AS pnl_signed_amount
    FROM classified
    GROUP BY
        accounting_month,
        client_id,
        primary_city,
        financial_area,
        reporting_category
)

SELECT
    SHA2(CONCAT_WS('||',
        DATE_FORMAT(accounting_month, 'yyyy-MM'),
        COALESCE(client_id, 'NO_CUSTOMER'),
        COALESCE(primary_city, 'NO_MAPPED_CITY'),
        financial_area,
        reporting_category
    ), 256) AS financial_monthly_key,
    accounting_month,
    DATE_FORMAT(accounting_month, 'yyyy-MM') AS accounting_period,
    client_id,
    customer_name,
    primary_city,
    financial_area,
    reporting_category,
    source_transaction_count,
    source_record_count,
    invoice_count,
    payment_receipt_count,
    expense_transaction_count,
    recorded_billed_revenue_amount,
    operating_expense_amount,
    cash_received_amount,
    unapplied_cash_amount,
    payment_application_amount,
    unresolved_payment_line_amount,
    pnl_signed_amount,
    CASE
        WHEN financial_area = 'COLLECTIONS' THEN 'Cash received is reported separately from invoice-based revenue; unapplied cash comes from the QuickBooks payment header.'
        WHEN financial_area = 'COLLECTION_APPLICATIONS' THEN 'Only payment lines linked unambiguously to exactly one invoice are counted as invoice applications.'
        WHEN financial_area = 'PAYMENT_LINES_REQUIRING_REVIEW' THEN 'Payment line amount has no single invoice link; it is retained separately and is not counted as an applied collection.'
        WHEN financial_area = 'OPERATING_EXPENSE' THEN 'Expense category comes from the QuickBooks line accounting account when available; no customer or city allocation is inferred.'
        WHEN financial_area = 'REVENUE' THEN 'Actual billed revenue is derived from QuickBooks invoice lines, not operational billing estimates.'
        ELSE 'Source category retained for review.'
    END AS reporting_note
FROM monthly_aggregated
