{{ config(
    materialized = 'table'
) }}

WITH fact_checkout_success__source AS (
  SELECT
    checkout_success_id,
    event_time,
    event_date,
    customer_id,
    ip_address,
    store_id,
    local_time,
    show_recommendation,
    collection,
    order_id,
    product_id
  FROM {{ ref('stg_fact_checkout_success') }}
)
, fact_checkout_success__null_handdle AS (
SELECT
  checkout_success_id,
  event_time,
  event_date,
  customer_id,
  ip_address,
  store_id,
  local_time,
  show_recommendation,
  collection,
  order_id,
  product_id
  FROM fact_checkout_success__source
)
, fact_checkout_success__undefined_value AS (
  SELECT
    checkout_success_id,
    event_time,
    event_date,
    customer_id,
    ip_address,
    store_id,
    local_time,
    show_recommendation,
    collection,
    order_id,
    product_id
  FROM fact_checkout_success__null_handdle

  UNION ALL

  SELECT
    -1 AS checkout_success_id,
    TIMESTAMP '1970-01-01 00:00:00 UTC' AS event_time,
    DATE '1970-01-01' AS event_date,
    -1 AS customer_id,
    'XNA' AS ip_address,
    'XNA' AS store_id,
    'XNA' AS local_time,
    FALSE AS show_recommendation,
    'XNA' AS collection,
    -1 AS order_id,
    -1 AS product_id

)

SELECT *
FROM fact_checkout_success__undefined_value

