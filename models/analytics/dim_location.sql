{{ config(
    materialized = 'table'
) }}

with dim_location__source AS (
    SELECT *
    FROM {{ref ("stg_location") }}  AS dim_location
)
 
,dim_location__null_handle AS (
SELECT
    location_id,
    ip_address,
    coalesce(country_name, 'XNA') AS country_name,
    coalesce(region_name, 'XNA') AS region_name,
    coalesce(city_name, 'XNA') AS city_name
FROM dim_location__source
)

,dim_location__undefined_value AS (
SELECT DISTINCT
    location_id,
    ip_address,
    country_name,
    region_name,
    city_name
FROM dim_location__null_handle

UNION ALL

SELECT
    -1 AS location_id,
    'XNA' AS ip_address,
    'XNA' AS country_name,
    'XNA' AS region_name,
    'XNA' AS city_name
)

SELECT * 
FROM dim_location__undefined_value

