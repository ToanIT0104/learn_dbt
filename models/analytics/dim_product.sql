{{ config(
    materialized = 'table'
) }}

WITH dim_product__source AS (
  SELECT DISTINCT
    product_detail_id,
    product_id,
    product_name,
    current_url,
    cart_amount,
    cart_price,
    cart_currency,
    option_id,
    option_label,
    value_id,
    value_label
  FROM {{ ref('stg_product') }}
  WHERE product_id IS NOT NULL
),

dim_product__handle_nulls AS (
  SELECT
    product_detail_id,
    product_id,
    COALESCE(product_name, 'XNA') AS product_name,
    COALESCE(current_url, 'XNA') AS current_url,
    IFNULL(cart_amount, 0) AS cart_amount,
    IFNULL(cart_price, 0.0) AS cart_price,
    COALESCE(cart_currency, 'XNA') AS cart_currency,
    COALESCE(option_id, -1) AS option_id,
    COALESCE(option_label, 'XNA') AS option_label,
    COALESCE(value_id, -1) AS value_id,
    COALESCE(value_label, 'XNA') AS value_label
  FROM dim_product__source
)
, dim_product__undefined_value AS (
  SELECT
    product_detail_id,
    product_id,
    product_name,
    current_url,
    cart_amount,
    cart_price,
    cart_currency,
    option_id,
    option_label,
    value_id,
    value_label
  FROM dim_product__handle_nulls

  UNION ALL

  SELECT
    -1 AS product_detail_id,
    -1 AS product_id,
    'XNA' AS product_name,
    'XNA' AS current_url,
    0 AS cart_amount,
    0.0 AS cart_price,
    'XNA' AS cart_currency,
    -1 AS option_id,
    'XNA' AS option_label,
    -1 AS value_id,
    'XNA' AS value_label
)

SELECT * FROM dim_product__undefined_value
