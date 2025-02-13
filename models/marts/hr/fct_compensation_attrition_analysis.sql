WITH comp_bands AS (
    SELECT
        worker_id,
        department_id,
        calculated_annual_salary,
        NTILE(4) OVER (PARTITION BY department_id ORDER BY calculated_annual_salary) as salary_quartile
    FROM {{ ref('int_rippling__worker_compensation') }}
    WHERE is_current_record = true
),

attrition_by_comp AS (
    SELECT
        c.department_id,
        c.salary_quartile,
        COUNT(DISTINCT a.worker_id) as total_employees,
        COUNT(DISTINCT CASE WHEN a.termination_date IS NOT NULL THEN a.worker_id END) as terminations,
        AVG(c.calculated_annual_salary) as avg_salary_band
    FROM comp_bands c
    LEFT JOIN {{ ref('fct_employee_attrition') }} a 
        ON c.worker_id = a.worker_id
    GROUP BY 1, 2
)

SELECT
    ac.*,
    d.department_name,
    d.department_path,
    -- Calculate metrics
    ROUND(terminations::FLOAT / NULLIF(total_employees, 0) * 100, 2) as attrition_rate_by_band,
    avg_salary_band / d.avg_compensation_per_employee * 100 as salary_to_dept_avg_ratio,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM attrition_by_comp ac
LEFT JOIN {{ ref('int_rippling__department_hierarchy') }} d 
    ON ac.department_id = d.department_id 