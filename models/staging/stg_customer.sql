{{ config(materialized='view') }}

WITH stg_customer__source AS (
  SELECT
    *
  FROM {{ source('raw_glamira', 'summary') }}
),

stg_customer__handle_invalid AS (
  SELECT
    device_id,
    ip,
    CASE
      WHEN TRIM(user_id_db) = '' THEN NULL ELSE user_id_db
    END AS user_id_db,
    CASE
      WHEN TRIM(email_address) = '' THEN NULL ELSE email_address
    END AS email_address,
    CASE
      WHEN TRIM(user_agent) = '' THEN NULL ELSE user_agent
    END AS user_agent,
    resolution
  FROM stg_customer__source
),
ranked AS (
  SELECT
    user_id_db,
    email_address,
    device_id,
    ip AS ip_address,
    user_agent,
    resolution,
    ROW_NUMBER() OVER (
      PARTITION BY user_id_db, email_address, device_id, ip, resolution
      ORDER BY user_agent DESC
    ) AS row_num
  FROM stg_customer__handle_invalid
),

stg_customer__renamed AS (
  SELECT
    user_id_db,
    email_address,
    device_id,
    ip_address,
    user_agent,
    resolution
  FROM ranked
  WHERE row_num = 1
),

stg_customer__final AS (
  SELECT
    FARM_FINGERPRINT(CONCAT(
      IFNULL(SAFE_CAST(device_id AS STRING), ''), '-',
      IFNULL(SAFE_CAST(ip_address AS STRING), ''), '-',
      IFNULL(SAFE_CAST(user_id_db AS STRING), ''), '-',
      IFNULL(SAFE_CAST(resolution AS STRING), ''), '-',
      IFNULL(SAFE_CAST(email_address AS STRING), '')
    )) AS customer_id,
    device_id,
    ip_address,
    user_id_db,
    email_address,
    user_agent,
    resolution
  FROM stg_customer__renamed
)

SELECT * FROM stg_customer__final

