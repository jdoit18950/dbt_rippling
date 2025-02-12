WITH worker_comp AS (
    SELECT *
    FROM {{ ref('int_rippling__worker_compensation') }}
),

department_data AS (
    SELECT *
    FROM {{ ref('base_rippling_departments') }}
),

comp_metrics AS (
    SELECT
        wc.*,
        d.department_name,
        d.department_path,
        -- Department level metrics
        AVG(calculated_annual_salary) OVER (PARTITION BY department_id) as dept_avg_salary,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY calculated_annual_salary) 
            OVER (PARTITION BY department_id) as dept_median_salary,
        -- Company wide metrics
        AVG(calculated_annual_salary) OVER () as company_avg_salary,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY calculated_annual_salary) 
            OVER () as company_median_salary,
        -- Comparative metrics
        calculated_annual_salary / NULLIF(AVG(calculated_annual_salary) OVER (PARTITION BY department_id), 0) 
            as salary_to_dept_avg_ratio,
        calculated_annual_salary / NULLIF(AVG(calculated_annual_salary) OVER (), 0) 
            as salary_to_company_avg_ratio
    FROM worker_comp wc
    LEFT JOIN department_data d ON wc.department_id = d.department_id
)

SELECT 
    *,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM comp_metrics 