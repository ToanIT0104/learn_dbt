{{ config(
    materialized='table'
) }}

WITH date_range AS (
  SELECT
    DATE_ADD(DATE '2010-01-01', INTERVAL day_offset DAY) AS date_key
  FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF('2030-12-31', '2010-01-01', DAY))) AS day_offset
)
SELECT
  date_key,
  FORMAT_DATE('%Y-%m-%d', date_key) AS full_date,
  FORMAT_DATE('%A', date_key) AS day_of_week,
  FORMAT_DATE('%a', date_key) AS day_of_week_short,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM date_key) BETWEEN 2 AND 6 THEN 'Weekday'
    ELSE 'Weekend'
  END AS is_weekday_or_weekend,
  FORMAT_DATE('%d', date_key) AS day_of_month,
  DATE_TRUNC(date_key, MONTH) AS year_month,
  EXTRACT(MONTH FROM date_key) AS month,
  EXTRACT(DAYOFYEAR FROM date_key) AS day_of_the_year,
  FORMAT_DATE('%V', date_key) AS week_of_year,
  EXTRACT(QUARTER FROM date_key) AS quarter_number,
  DATE_TRUNC(date_key, YEAR) AS year,
  EXTRACT(YEAR FROM date_key) AS year_number
FROM date_range
ORDER BY date_key
