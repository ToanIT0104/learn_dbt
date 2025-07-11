{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw_glamira', 'location') }}
),
stg_dim_location__rename AS (
    SELECT
        country AS country_name,
        ip AS ip_address,
        region AS region_name,
        city AS city_name,
    FROM source
),
stg_dim_location__handle_invalid AS (
  SELECT
    FARM_FINGERPRINT(ip_address) AS location_id,
    ip_address,
    CASE
      WHEN TRIM(country_name) = '' OR country_name = '-' THEN NULL
      ELSE country_name
    END AS country_name,
    CASE
      WHEN TRIM(region_name) = '' OR region_name = '-' THEN NULL
      ELSE region_name
    END AS region_name,
    CASE
      WHEN TRIM(city_name) = '' OR city_name = '-' THEN NULL
      ELSE city_name
    END AS city_name
  FROM stg_dim_location__rename
)


SELECT * FROM stg_dim_location__handle_invalid


