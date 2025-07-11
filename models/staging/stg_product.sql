{{ config(
    materialized = 'view'
) }}

WITH summary_with_cart_products AS (
  SELECT
    FARM_FINGERPRINT(CONCAT(
          SAFE_CAST(s.order_id AS STRING), '-',
          SAFE_CAST(cp.product_id AS STRING)
      )) AS product_detail_id,
    s.referrer_url,
    cp.product_id,
    cp.amount AS cart_amount,
    SAFE_CAST(
      REPLACE(
        REPLACE(cp.price, '.', ''),
        ',', '.'
      ) AS FLOAT64
    ) AS cart_price,
    cp.currency AS cart_currency,
    cp.option AS cart_options
  FROM {{ source('raw_glamira', 'summary') }} s,
       UNNEST(s.cart_products) AS cp
  WHERE cp.product_id IS NOT NULL
    AND s.order_id IS NOT NULL    
),
summary_with_cart_options AS (
  SELECT
    swcp.product_detail_id,
    swcp.referrer_url,
    swcp.product_id,
    swcp.cart_amount,
    swcp.cart_price,
    swcp.cart_currency,
    o.option_id,
    o.option_label,
    o.value_id,
    o.value_label
  FROM summary_with_cart_products swcp,
       UNNEST(swcp.cart_options) AS o
)

SELECT
  s.product_detail_id,
  s.product_id,
  p.product_name,
  p.current_url,
  s.referrer_url,
  s.cart_amount,
  s.cart_price,
  s.cart_currency,
  s.option_id,
  s.option_label,
  s.value_id,
  s.value_label
FROM {{ source('raw_glamira', 'products') }} p
JOIN summary_with_cart_options s
  ON p.product_id = s.product_id
WHERE p.product_name IS NOT NULL
