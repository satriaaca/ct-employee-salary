{{
  config(
    materialized="view"
  )
}}
SELECT
    branch_id,
    EXTRACT(YEAR FROM e.join_date) AS year,
    EXTRACT(MONTH FROM e.join_date) AS month,
    SUM(salary) AS total_salary
  FROM {{ ref('stg_employees')}}  e
  WHERE e.resign_date IS NULL OR e.resign_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY branch_id, EXTRACT(YEAR FROM e.join_date), EXTRACT(MONTH FROM e.join_date)