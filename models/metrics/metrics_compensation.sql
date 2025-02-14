WITH compensation_base AS (
    SELECT *
    FROM {{ ref('fact_compensation') }}
),

employee_dim AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

department_stats AS (
    SELECT 
        e.department_name,
        DATE_TRUNC('month', c.created_at) AS analysis_month,
        AVG(c.annual_salary) AS avg_annual_salary,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY c.annual_salary) AS median_annual_salary,
        MIN(c.annual_salary) AS min_annual_salary,
        MAX(c.annual_salary) AS max_annual_salary,
        AVG(c.target_bonus) AS avg_target_bonus,
        COUNT(DISTINCT c.worker_id) AS employee_count,
        SUM(c.annual_salary) AS total_compensation_cost
    FROM compensation_base c
    JOIN employee_dim e ON c.worker_id = e.worker_id
    WHERE c.payment_type = 'SALARY'
    GROUP BY 1, 2
),

salary_bands AS (
    SELECT 
        e.department_name,
        DATE_TRUNC('month', c.created_at) AS analysis_month,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY c.annual_salary) AS salary_25th_percentile,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY c.annual_salary) AS salary_75th_percentile
    FROM compensation_base c
    JOIN employee_dim e ON c.worker_id = e.worker_id
    WHERE c.payment_type = 'SALARY'
    GROUP BY 1, 2
)

SELECT 
    d.*,
    s.salary_25th_percentile,
    s.salary_75th_percentile,
    s.salary_75th_percentile - s.salary_25th_percentile AS salary_range,
    d.max_annual_salary - d.min_annual_salary AS total_salary_spread,
    d.total_compensation_cost / NULLIF(d.employee_count, 0) AS cost_per_employee,
    
    -- Calculated ratios
    d.avg_target_bonus / NULLIF(d.avg_annual_salary, 0) * 100 AS avg_bonus_to_salary_ratio,
    
    -- Salary spread metrics
    (d.max_annual_salary - d.min_annual_salary) / NULLIF(d.median_annual_salary, 0) * 100 AS salary_spread_percentage,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at
FROM department_stats d
LEFT JOIN salary_bands s 
    ON d.department_name = s.department_name 
    AND d.analysis_month = s.analysis_month