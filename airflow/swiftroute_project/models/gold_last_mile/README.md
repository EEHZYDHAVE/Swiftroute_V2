# Gold Last-Mile

This bundle implements `gold_last_mile` with **one fact table**: `fact_delivery`. Each model is configured with its Gold schema and alias; materialization is intentionally inherited from `dbt_project.yml` (set to `view` in this project).

## Models

- `fact_delivery`: one row per `silver_business_lastmile_delivery.delivery_id`.
- `dim_customer`: Salesforce account master, keyed by the persistent `account_id`; retains shared `client_id`.
- `dim_date`: calendar date dimension covering task-created, event, and completion dates, including date key `-1` for unknown.
- `dim_driver`: Onfleet worker profiles enriched from Gusto only using the explicit `onfleet_worker_id` match where unique.
- `dim_city`: delivery destination city/state/country composite key.
- `dim_zone`: observed `zone_id` values.
- `dim_service`: observed `service_type` values, with simple reporting labels/groups.
- `dim_contract`: Salesforce contract master, with the contract valid on each event chosen by Silver Business.

## Deliberate differences from the original proposal

- No `fact_route` or route dimension: current data has no trustworthy route ID, and trip-to-delivery matching returned no candidates. Task dependencies are not equivalent to a route identity.
- No `fact_delivery_attempt`: delivery attempts do not have an independently validated attempt-level grain/source in the available Silver models. Failure outcomes remain attributes/measures on `fact_delivery`.
- No `dim_vehicle` yet: the technical sources expose current Onfleet worker vehicle profile data, but there is no reliable historical assignment from a delivery to a physical vehicle, and the tested Samsara vehicle link returned zero matches. The task's `task_vehicle_type` remains a descriptive attribute; it is not asserted as the vehicle actually used.
- Contract dimension added because contract identity/terms are available and useful for historical commercial analysis.
- Client-month invoice totals, balances, payment receipts, driver-level Samsara summary totals, and payroll amounts are excluded from `fact_delivery` because those values belong to higher grains and would be repeated across delivery rows. Use finance/payroll analyses at their own grains.
- `matched_contract_net_rate` is retained as a contextual, non-additive rate. It is not recognized delivery revenue and must not be summed until rate-unit semantics are validated.

## Keys and unknown members

Stable source/business keys are used rather than row-number-generated keys because these models are views. Every dimension provides an `N/A` member; `dim_date` provides key `-1`. Fact foreign keys use those members when the enrichment cannot be resolved. The shared `client_id` is also retained on the fact for traceability when a customer dimension match is unavailable.

## Run

Merge `properties.yml` into the project's existing model properties YAML (avoid duplicating the `version: 2` and top-level `models:` keys), add the singular test under the dbt `tests/` directory, and run:

```bash
dbt build --select path:models/gold_last_mile
```

The bundle is based on the validated Silver Business model's columns and the Silver Technical model names used by that model. SQL has been reviewed for structural consistency but has not been executed against Databricks in this conversation. After building, confirm the fact row count equals the Silver Business delivery count and that all generic and singular tests pass.
