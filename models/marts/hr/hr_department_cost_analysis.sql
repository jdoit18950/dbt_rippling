WITH worker_comp AS (
    SELECT *
    FROM {{ ref('int_rippling__worker_compensation') }}
),

department_data AS (
    SELECT *
    FROM {{ ref('base_rippling_departments') }}
),

time_entries AS (
    SELECT *
    FROM {{ ref('hr_time_attendance_analysis') }}
),

dept_costs AS (
    SELECT
        d.department_id,
        d.department_name,
        d.department_path,
        DATE_TRUNC('MONTH', wc.effective_date) as month_date,
        -- Headcount costs
        COUNT(DISTINCT wc.worker_id) as employee_count,
        SUM(wc.calculated_annual_salary) / 12 as monthly_salary_cost,
        -- Overtime costs
        SUM(t.total_overtime_hours_mtd * (wc.calculated_annual_salary / 2080 * 1.5)) as overtime_cost,
        -- Benefits estimation (assuming 30% of salary)
        (SUM(wc.calculated_annual_salary) / 12) * 0.30 as estimated_benefits_cost,
        -- Total cost
        (SUM(wc.calculated_annual_salary) / 12) + 
        (SUM(wc.calculated_annual_salary) / 12) * 0.30 +
        SUM(t.total_overtime_hours_mtd * (wc.calculated_annual_salary / 2080 * 1.5)) as total_monthly_cost
    FROM worker_comp wc
    LEFT JOIN department_data d ON wc.department_id = d.department_id
    LEFT JOIN time_entries t ON wc.worker_id = t.worker_id
        AND DATE_TRUNC('MONTH', wc.effective_date) = DATE_TRUNC('MONTH', t.entry_date)
    GROUP BY 1, 2, 3, 4
)

SELECT 
    *,
    LAG(total_monthly_cost) OVER (PARTITION BY department_id ORDER BY month_date) as previous_month_cost,
    ROUND(100.0 * (total_monthly_cost - LAG(total_monthly_cost) 
        OVER (PARTITION BY department_id ORDER BY month_date)) / 
        NULLIF(LAG(total_monthly_cost) OVER (PARTITION BY department_id ORDER BY month_date), 0), 2) 
        as month_over_month_cost_change,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM dept_costs 