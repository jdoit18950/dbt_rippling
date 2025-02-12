WITH worker_base AS (
  SELECT
    *
  FROM {{ ref('base_rippling_workers') }} AS base_rippling_workers
  WHERE
    is_current_employee = TRUE
), compensation_data AS (
  SELECT
    *
  FROM {{ ref('base_rippling_compensations') }} AS base_rippling_compensations
), compensation_changes AS (
  SELECT
    worker_id,
    compensation_id,
    calculated_annual_salary,
    created_at AS effective_date,
    payment_type,
    LAG(calculated_annual_salary) OVER (PARTITION BY worker_id ORDER BY created_at NULLS FIRST) AS previous_salary,
    CASE
      WHEN NOT previous_salary IS NULL AND previous_salary <> 0
      THEN ROUND(
        (
          (
            calculated_annual_salary - previous_salary
          ) / previous_salary
        ) * 100,
        2
      )
      ELSE 0
    END AS salary_change_percentage,
    DATEDIFF(
      CREATED_AT,
      LAG(created_at) OVER (PARTITION BY worker_id ORDER BY created_at NULLS FIRST),
      'month'
    ) AS months_since_last_change
  FROM compensation_data
), annualized_compensation AS (
  SELECT
    cc.*,
    w.department_id,
    w.job_title,
    NTILE(4) OVER (PARTITION BY w.department_id ORDER BY cc.calculated_annual_salary NULLS FIRST) AS department_comp_quartile,
    CASE WHEN ABS(salary_change_percentage) > 10 THEN TRUE ELSE FALSE END AS is_significant_change,
    CASE WHEN months_since_last_change >= 12 THEN TRUE ELSE FALSE END AS is_due_for_review
  FROM worker_base AS w
  INNER JOIN compensation_changes AS cc
    ON w.worker_id = cc.worker_id
), final AS (
  SELECT
    worker_id,
    department_id,
    job_title,
    compensation_id,
    effective_date,
    payment_type,
    calculated_annual_salary,
    previous_salary,
    salary_change_percentage,
    months_since_last_change,
    department_comp_quartile,
    is_significant_change,
    is_due_for_review,
    DATE_TRUNC('MONTH', CAST(effective_date AS DATE)) AS effective_month,
    DATE_TRUNC('QUARTER', CAST(effective_date AS DATE)) AS effective_quarter,
    DATE_TRUNC('YEAR', CAST(effective_date AS DATE)) AS effective_year,
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    '3f1dc5e6-2941-4085-ba77-9b5b7fa85303' AS dbt_batch_id
  FROM annualized_compensation
)
SELECT
  *
FROM final