{{
  config(
    materialized="view"
  )
}}
  SELECT
    e.employee_id,
    e.branch_id,
    EXTRACT(YEAR FROM t.date) AS year,
    EXTRACT(MONTH FROM t.date) AS month,
    SUM(
      CASE WHEN t.check_out IS NOT NULL THEN
        EXTRACT(EPOCH FROM t.check_out - t.check_in)/3600
      ELSE
        0
      END
    ) AS total_worked_hours
  FROM {{ ref('stg_timesheets') }} t
  JOIN {{ ref('stg_employees') }} e ON e.employee_id = t.employee_id
  WHERE e.resign_date IS NULL OR e.resign_date >= t.date
  GROUP BY e.employee_id, e.branch_id, EXTRACT(YEAR FROM t.date), EXTRACT(MONTH FROM t.date)