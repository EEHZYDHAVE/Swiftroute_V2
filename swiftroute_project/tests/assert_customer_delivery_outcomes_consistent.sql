-- Successful and failed counts are subsets of completed non-pickup deliveries.
-- Their sum must not exceed the completed-delivery denominator.
SELECT
    client_id,
    period_month,
    last_mile_completed_delivery_count,
    last_mile_successful_delivery_count,
    last_mile_failed_delivery_count
FROM {{ ref('fact_customer_economics') }}
WHERE COALESCE(last_mile_successful_delivery_count, 0) < 0
   OR COALESCE(last_mile_failed_delivery_count, 0) < 0
   OR COALESCE(last_mile_completed_delivery_count, 0) < 0
   OR COALESCE(last_mile_successful_delivery_count, 0)
      + COALESCE(last_mile_failed_delivery_count, 0)
      > COALESCE(last_mile_completed_delivery_count, 0)
