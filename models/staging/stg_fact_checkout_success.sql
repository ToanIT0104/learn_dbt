{{ config(materialized='view') }}

with source AS (
  SELECT
      *
  FROM {{ source('raw_glamira', 'summary') }}
),
renamed AS (
  SELECT
      FARM_FINGERPRINT(CONCAT(
          SAFE_CAST(order_id AS STRING), '-',
          SAFE_CAST(cp.product_id AS STRING)
      )) AS checkout_success_id,

      TIMESTAMP_SECONDS(time_stamp) AS event_time,
      DATE(TIMESTAMP_SECONDS(time_stamp)) AS event_date,

      FARM_FINGERPRINT(CONCAT(
      IFNULL(SAFE_CAST(device_id AS STRING), ''), '-',
      IFNULL(SAFE_CAST(ip AS STRING), ''), '-',
      IFNULL(SAFE_CAST(user_id_db AS STRING), ''), '-',
      IFNULL(SAFE_CAST(resolution AS STRING), ''), '-',
      IFNULL(SAFE_CAST(email_address AS STRING), '')
    )) AS customer_id,
      SAFE_CAST(ip AS STRING) AS ip_address,
      SAFE_CAST(store_id AS STRING) AS store_id,
      SAFE_CAST(local_time AS STRING) AS local_time,
      SAFE_CAST(show_recommendation AS BOOL) AS show_recommendation,
      SAFE_CAST(collection AS STRING) AS collection,
      SAFE_CAST(order_id AS INT) AS order_id,
      SAFE_CAST(cp.product_id AS INT) AS product_id

  FROM {{ source('raw_glamira', 'summary') }},
        UNNEST(cart_products) AS cp
  WHERE
      SAFE_CAST(collection AS STRING) = 'checkout_success'
      AND cp.product_id IS NOT NULL
)

SELECT * FROM renamed