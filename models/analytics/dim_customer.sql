{{ config(
    materialized = 'table'
) }}

WITH stg_customer__source AS (
    SELECT * FROM {{ ref('stg_customer') }}
),

stg_customer__handdle_null AS (
  SELECT
    customer_id,
    COALESCE(user_id_db, 'XNA') AS user_id_db,
    COALESCE(email_address, 'XNA') AS email_address,
    device_id,
    ip_address,
    COALESCE(user_agent, 'XNA') AS user_agent,
    COALESCE(resolution, 'XNA') AS resolution
  FROM stg_customer__source
),

stg_customer__undefined_value AS (
  SELECT DISTINCT
    customer_id,
    user_id_db,
    email_address,
    device_id,
    ip_address,
    user_agent,
    resolution
  FROM stg_customer__handdle_null

  UNION ALL

  SELECT
    -1 AS customer_id,
    'XNA' AS user_id_db,
    'XNA' AS email_address,
    'XNA' AS device_id,
    'XNA' AS ip_address,
    'XNA' AS user_agent,
    'XNA' AS resolution
)

SELECT * FROM stg_customer__undefined_value
