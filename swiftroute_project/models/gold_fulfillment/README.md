# Gold Fulfillment Operations

## Contents

- `fact_fulfillment_transaction.sql` — one row per Silver Business/Linnworks stock transaction.
- `dim_customer.sql` — shared client IDs with Salesforce customer attributes and an unknown member.
- `dim_date.sql` — a calendar spanning the observed transaction dates, plus an unknown-date member.
- `dim_product.sql` — Linnworks stock-item attributes, supplemented from observed transactions where the source master is incomplete.
- `dim_warehouse.sql` — distinct source inventory location labels, plus an unknown member.
- `dim_contract.sql` — contract identity and commercial terms, plus an unknown member.
- `properties.yml` — generic dbt tests for primary keys and fact-to-dimension relationships.

## Model design decisions

### Materialization and schema

Each SQL model sets `schema = 'gold_fulfillment'` and an explicit alias. Materialization is intentionally not overridden in each model: it inherits the project's `view` default from `dbt_project.yml`.

### Fact grain and measures

The fact has one row per `fulfillment_transaction_id`, inherited from one Linnworks stock transaction. `quantity_change` keeps the signed quantity; `quantity_moved` is its absolute magnitude. Neither is assumed to equal the billable quantity of a QuickBooks pick line. `source_stock_value` is retained with its source-oriented name and should not be described as revenue.

Invoice line presence for a client-month is a Boolean diagnostic only. Invoice and payment amounts are not assigned to transaction rows, and contract rate amounts are not exposed as additive measures.

### Dimensions and keys

The model uses stable business keys rather than generated row-number surrogate keys, since the Gold models are views and a row number can change between executions. Customer key is `client_id`; product key is `stock_item_id`; warehouse key is the upper-trimmed source inventory location; contract key is `contract_id`; date key is `YYYYMMDD` (with -1 for unknown date). Each textual dimension includes an `N/A` member for unresolved keys.

### Why `dim_service` is replaced

The proposed sources expose fulfillment-pick contract-rate context, but they do not prove that each stock movement represents a particular billable service. Treating that context as a `dim_service` would imply a service assignment not supported by the data. `dim_contract` is used instead because the Silver Business model already matches the contract active on each transaction date. Service/rate-context status remains on the fact as a diagnostic attribute. This can be revisited if a service/event source with a reliable transaction-level linkage is added.

### Warehouse caveat

There is no separate warehouse master in the current model inputs. `dim_warehouse` therefore represents observed Linnworks inventory location labels, and does not fabricate facility names, addresses, or regions. Confirm that the source `location` field means warehouse/facility rather than a more granular stock location before using it for facility-level reporting.

## Suggested build and test

Run from the dbt project root:

```bash
dbt build --select fact_fulfillment_transaction dim_customer dim_date dim_product dim_warehouse dim_contract
```

The tests check uniqueness/not-null on model keys and that every fact foreign key resolves to a dimension member. SQL has been authored for Databricks/Spark SQL but has not been executed against your live dbt project. Check the generated schema name if your project uses a custom `generate_schema_name` macro.
