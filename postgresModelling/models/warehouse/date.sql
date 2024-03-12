WITH date_series AS (
  SELECT generate_series(
    '2014-01-01'::date,
    '2050-01-01'::date,
    '1 day'::interval
  ) AS full_date
)
SELECT
  to_char(full_date, 'YYYY-MM-DD') AS id,
  full_date,
  EXTRACT(YEAR FROM full_date) AS year,
  EXTRACT(WEEK FROM full_date) AS year_week,
  EXTRACT(DOY FROM full_date) AS year_day,
  EXTRACT(YEAR FROM full_date) AS fiscal_year,
  -- PostgreSQL does not have a direct quarter format so using a case statement
  CASE 
    WHEN EXTRACT(MONTH FROM full_date) BETWEEN 1 AND 3 THEN 1
    WHEN EXTRACT(MONTH FROM full_date) BETWEEN 4 AND 6 THEN 2
    WHEN EXTRACT(MONTH FROM full_date) BETWEEN 7 AND 9 THEN 3
    WHEN EXTRACT(MONTH FROM full_date) BETWEEN 10 AND 12 THEN 4
  END AS fiscal_qtr,
  EXTRACT(MONTH FROM full_date) AS month,
  to_char(full_date, 'Month') AS month_name,
  -- PostgreSQL treats Sunday as 0, adjusting to match format_date '%w' output
  EXTRACT(DOW FROM full_date) AS week_day,
  to_char(full_date, 'Day') AS day_name,
  CASE
    WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN 0
    ELSE 1
  END AS day_is_weekday
FROM date_series
