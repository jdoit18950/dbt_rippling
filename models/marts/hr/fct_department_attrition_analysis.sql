WITH monthly_dept_metrics AS (
    SELECT
        department_id,
        DATE_TRUNC('month', analysis_month) as month_date,
        SUM(CASE WHEN termination_date IS NOT NULL THEN 1 ELSE 0 END) as terminations,
        COUNT(DISTINCT worker_id) as total_headcount,
        AVG(calculated_annual_salary) as avg_salary
    FROM {{ ref('fct_employee_attrition') }}
    GROUP BY 1, 2
),

dept_trends AS (
    SELECT
        d.*,
        dh.department_name,
        dh.department_path,
        dh.hierarchy_level,
        -- Calculate YoY changes
        LAG(terminations, 12) OVER (PARTITION BY department_id ORDER BY month_date) as prev_year_terminations,
        LAG(total_headcount, 12) OVER (PARTITION BY department_id ORDER BY month_date) as prev_year_headcount,
        -- Calculate growth metrics
        (total_headcount - LAG(total_headcount, 12) OVER (PARTITION BY department_id ORDER BY month_date))::FLOAT / 
            NULLIF(LAG(total_headcount, 12) OVER (PARTITION BY department_id ORDER BY month_date), 0) * 100 as yoy_growth_rate
    FROM monthly_dept_metrics d
    LEFT JOIN {{ ref('int_rippling__department_hierarchy') }} dh 
        ON d.department_id = dh.department_id
)

SELECT
    *,
    -- Calculate relative metrics
    ROUND(terminations::FLOAT / NULLIF(total_headcount, 0) * 100, 2) as attrition_rate,
    ROUND((terminations - prev_year_terminations)::FLOAT / NULLIF(prev_year_terminations, 0) * 100, 2) as yoy_attrition_change,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM dept_trends 