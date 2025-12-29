{{ config(
    materialized='table'
) }}

WITH date_spine AS (
  {{ date_spine(
      datepart="day",
      start_date="cast('1992-01-01' as date)",
      end_date="cast('1998-12-31' as date)"
     )
  }}
)

SELECT
    DATE_DAY AS date_key,
    DATE_DAY,
    EXTRACT(YEAR FROM DATE_DAY) AS year,
    EXTRACT(MONTH FROM DATE_DAY) AS month,
    TO_CHAR(DATE_DAY, 'MMMM') AS month_name,
    EXTRACT(QUARTER FROM DATE_DAY) AS quarter,
    EXTRACT(DAYOFWEEK FROM DATE_DAY) AS day_of_week_num,
    TO_CHAR(DATE_DAY, 'DAY') AS day_of_week_name,
    CASE
        WHEN DAYNAME(DATE_DAY) IN ('Sat', 'Sun') THEN 1
        ELSE 0
    END AS is_weekend
FROM date_spine
