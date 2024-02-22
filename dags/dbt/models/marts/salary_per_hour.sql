SELECT
  b.year,
  b.month,
  b.branch_id,
  ROUND(CAST(SUM(b.total_salary)/SUM((ew.total_worked_hours)) AS NUMERIC),2) AS salary_per_hour
FROM {{ ref('branch_salary') }} b
JOIN {{ ref('employee_worktime')}} ew ON b.branch_id = ew.branch_id
AND b.year = ew.year
AND b.month = ew.month
GROUP BY b.year, b.month, b.branch_id