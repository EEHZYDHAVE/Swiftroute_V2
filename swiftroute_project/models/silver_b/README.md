# Silver Business Fulfillment

## Model

- dbt model: `silver_business_fufillment`
- intended schema: `silver_b`
- target table: `silver_business_fufillment`
- materialization: table
- grain: one row per Linnworks stock transaction

## Technical inputs

- `ref('silver_linnworks_stock_transaction')`: transaction spine and signed stock movement.
- `ref('silver_linnworks_processed_orders')`: order context and order-item matching.
- `ref('silver_linnworks_stock_items')`: product and stock-item client context.
- `ref('silver_salesforce_accounts')`: authoritative client/account attributes.
- `ref('silver_salesforce_contracts')`: contract active on the transaction date.
- `ref('silver_salesforce_contract_rates')`: most recent effective `fulfillment_pick` / `per_unit` rate for the active contract, used as context only.
- `ref('silver_quickbooks_invoices')`: client-month flag for existence of a fulfillment/pick invoice line.

## Key business decisions

1. `RECEIPT` becomes `INBOUND`; `DISPATCH` and `ADJUSTMENT` are retained as business classifications. There were no `RETURN` transactions in the profiled source types, so no returns are fabricated from invoice descriptions.
2. The row grain is the stock transaction, not the order or invoice. Order, stock-item, account, contract, rate, and invoice-period joins are designed to return at most one enrichment row per transaction.
3. For dispatches, the matched order's client ID is preferred. When no order client is available, stock-item client ID is used. Differences between order and stock-item client IDs are flagged rather than hidden.
4. Invoice totals, invoice-line amounts, balances, and payment amounts are not copied to transaction rows. The only QuickBooks enrichment is a non-additive Boolean flag indicating a matching client-month has a fulfillment/pick invoice line.
5. Contract rate fields are descriptive pricing context. No estimated charge is calculated because the current evidence does not demonstrate that signed stock movement quantity equals the invoice's billable pick quantity.
6. Signed quantity changes are preserved. No historic before/after balances are inferred from current inventory snapshots.
7. String fields that are optional for reporting are rendered as `N/A`; dates, timestamps, numbers, and Booleans retain their types and can remain null when unknown.
8. Bronze audit fields are not carried forward. `processed_at_utc` is added for Silver Business processing audit.

## dbt tests

- `properties.yml` contains generic tests for grain keys, event classifications, matching statuses, reference relationships, and the audit timestamp.
- `tests/silver_business_fufillment_preserves_transaction_grain.sql` checks that all technical stock transaction IDs appear in the business model and that the business model introduces no extra transaction keys.
- The model block in `properties.yml` should be merged into the project's existing YAML. Avoid duplicating top-level `version` or `models` keys.

The YAML syntax was validated locally. The SQL has not been executed in the user's Databricks/dbt environment. Run the model, inspect the output row count against the 58,636 profiled transactions, and run the generic and singular tests before relying on the result.
