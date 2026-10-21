{{ config(materialized='view', schema='gold_finance') }}

/*
Purpose:
  One invoice-level view of current receivables, recorded due dates, and payment
  applications that can be traced unambiguously to an invoice.

Grain:
  One row per QuickBooks invoice_id.

The recorded QuickBooks balance is authoritative for current balance reporting.
Payment-derived balances are included only as a diagnostic because credit notes,
unapplied cash, and other accounting events may not be fully represented by the
available payment-line links. Aging is calculated relative to CURRENT_DATE(), so
this is current aging, not a historical month-end snapshot.
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

invoice_source AS (
    SELECT
        invoice_id,
        source_sync_token,
        CAST(transaction_date AS DATE) AS invoice_date,
        CAST(due_date AS DATE) AS due_date,
        CASE
            WHEN client_id IS NULL OR LOWER(TRIM(client_id)) IN ('', 'n/a', 'null') THEN NULL
            ELSE TRIM(client_id)
        END AS client_id,
        source_customer_id,
        customer_name,
        billing_period,
        payment_terms,
        CAST(total_amount AS DECIMAL(18, 2)) AS invoice_amount,
        CAST(balance_amount AS DECIMAL(18, 2)) AS recorded_current_balance,
        currency_code,
        document_number,
        linked_transactions
    FROM {{ ref('quickbooks_invoices_t') }}
),

valid_payment_applications AS (
    SELECT
        p.payment_id,
        p.client_id AS payment_client_id,
        CAST(p.transaction_date AS DATE) AS payment_date,
        p_line.amount AS applied_amount,
        p_line.linked_transactions[0].transaction_id AS invoice_id
    FROM {{ ref('quickbooks_payments_t') }} p
    LATERAL VIEW OUTER EXPLODE(p.lines) exploded_payment_lines AS p_line
    WHERE p_line.amount IS NOT NULL
      AND SIZE(p_line.linked_transactions) = 1
      AND LOWER(TRIM(p_line.linked_transactions[0].transaction_type)) = 'invoice'
      AND p_line.linked_transactions[0].transaction_id IS NOT NULL
),

applications_by_invoice AS (
    SELECT
        p.invoice_id AS invoice_id,
        SUM(CAST(applied_amount AS DECIMAL(18, 2))) AS linked_applied_payment_amount,
        COUNT(DISTINCT payment_id) AS linked_payment_count,
        MIN(payment_date) AS first_linked_payment_date,
        MAX(payment_date) AS latest_linked_payment_date,
        SUM(
            CASE
                WHEN payment_client_id IS NOT NULL
                 AND LOWER(TRIM(payment_client_id)) NOT IN ('', 'n/a', 'null')
                 AND i.client_id IS NOT NULL
                 AND TRIM(payment_client_id) <> i.client_id
                    THEN 1
                ELSE 0
            END
        ) AS linked_payment_client_mismatch_count
    FROM valid_payment_applications p
    LEFT JOIN invoice_source i
        ON p.invoice_id = i.invoice_id
    GROUP BY p.invoice_id
),

invoice_base AS (
    SELECT
        i.*,
        a.primary_city,
        COALESCE(p.linked_applied_payment_amount, CAST(0 AS DECIMAL(18, 2))) AS linked_applied_payment_amount,
        COALESCE(p.linked_payment_count, 0) AS linked_payment_count,
        p.first_linked_payment_date,
        p.latest_linked_payment_date,
        COALESCE(p.linked_payment_client_mismatch_count, 0) AS linked_payment_client_mismatch_count
    FROM invoice_source i
    LEFT JOIN accounts a
        ON TRIM(i.client_id) = a.client_id
       AND i.client_id IS NOT NULL
       AND LOWER(TRIM(i.client_id)) NOT IN ('', 'n/a', 'null')
    LEFT JOIN applications_by_invoice p
        ON i.invoice_id = p.invoice_id
),

aging AS (
    SELECT
        *,
        CASE
            WHEN recorded_current_balance IS NULL THEN CAST(NULL AS INT)
            WHEN recorded_current_balance <= 0 THEN 0
            WHEN due_date IS NULL THEN CAST(NULL AS INT)
            ELSE GREATEST(DATEDIFF(CURRENT_DATE(), due_date), 0)
        END AS days_past_due,
        CAST(invoice_amount - linked_applied_payment_amount AS DECIMAL(18, 2)) AS payment_reconstructed_balance,
        CAST(
            recorded_current_balance - (invoice_amount - linked_applied_payment_amount)
            AS DECIMAL(18, 2)
        ) AS recorded_minus_reconstructed_balance
    FROM invoice_base
)

SELECT
    invoice_id,
    source_sync_token,
    document_number,
    client_id,
    source_customer_id,
    customer_name,
    primary_city,
    billing_period,
    invoice_date,
    due_date,
    payment_terms,
    currency_code,
    invoice_amount,
    recorded_current_balance,
    linked_applied_payment_amount,
    linked_payment_count,
    first_linked_payment_date,
    latest_linked_payment_date,
    payment_reconstructed_balance,
    recorded_minus_reconstructed_balance,
    CASE
        WHEN recorded_current_balance IS NULL THEN 'UNKNOWN_BALANCE'
        WHEN recorded_current_balance < 0 THEN 'CREDIT_BALANCE'
        WHEN recorded_current_balance = 0 THEN 'PAID_OR_ZERO_BALANCE'
        WHEN due_date IS NULL THEN 'OPEN_WITHOUT_DUE_DATE'
        WHEN due_date >= CURRENT_DATE() THEN 'CURRENT'
        WHEN days_past_due <= 30 THEN 'CURRENT_OR_0_30_DAYS_PAST_DUE'
        WHEN days_past_due <= 60 THEN '31_60_DAYS_PAST_DUE'
        WHEN days_past_due <= 90 THEN '61_90_DAYS_PAST_DUE'
        ELSE 'OVER_90_DAYS_PAST_DUE'
    END AS aging_bucket,
    days_past_due,
    CASE
        WHEN recorded_current_balance IS NULL THEN 'REVIEW_MISSING_RECORDED_BALANCE'
        WHEN due_date IS NULL AND recorded_current_balance > 0 THEN 'REVIEW_MISSING_DUE_DATE'
        WHEN ABS(COALESCE(recorded_minus_reconstructed_balance, 0)) > 0.01 THEN 'PAYMENT_RECONSTRUCTION_VARIANCE'
        ELSE 'RECORDED_BALANCE_AVAILABLE'
    END AS receivable_review_status,
    linked_payment_client_mismatch_count
FROM aging
