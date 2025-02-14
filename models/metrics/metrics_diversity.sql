WITH employee_base AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

compensation_data AS (
    SELECT 
        worker_id,
        created_at,
        -- Ensure we're accessing the correct compensation fields
        annual_salary,
        department_id
    FROM {{ ref('fact_compensation') }}
    WHERE annual_salary IS NOT NULL
),

-- Department level metrics
department_diversity AS (
    SELECT 
        department_name,
        DATE_TRUNC('month', TRY_TO_TIMESTAMP(hire_date)) AS month,
        COUNT(DISTINCT worker_id) AS total_employees,
        
        -- Ethnicity metrics
        COUNT(DISTINCT CASE WHEN ethnicity IS NOT NULL THEN worker_id END) AS employees_with_ethnicity_data,
        COUNT(DISTINCT ethnicity) AS distinct_ethnicities,
        
        -- Location diversity
        COUNT(DISTINCT work_location_id) AS distinct_locations,
        
        -- Employment type diversity
        COUNT(DISTINCT employment_type) AS distinct_employment_types,
        SUM(CASE WHEN employment_type ILIKE '%full%time%' THEN 1 ELSE 0 END) AS full_time_count,
        SUM(CASE WHEN employment_type ILIKE '%part%time%' THEN 1 ELSE 0 END) AS part_time_count
    FROM employee_base
    WHERE employee_status = 'ACTIVE'
    GROUP BY 1, 2
),

-- Pay equity analysis (fixed grouping)
pay_equity AS (
    SELECT 
        e.department_name,
        DATE_TRUNC('month', c.created_at) AS month,
        e.employment_type,
        AVG(c.annual_salary) AS avg_salary,
        MEDIAN(c.annual_salary) AS median_salary,
        MIN(c.annual_salary) AS min_salary,
        MAX(c.annual_salary) AS max_salary,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY c.annual_salary) AS salary_25th_percentile,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY c.annual_salary) AS salary_75th_percentile
    FROM employee_base e
    JOIN compensation_data c ON e.worker_id = c.worker_id
    WHERE e.employee_status = 'ACTIVE'
    GROUP BY 1, 2, 3
),

-- Department average calculations
dept_averages AS (
    SELECT 
        department_name,
        month,
        AVG(avg_salary) AS dept_avg_salary
    FROM pay_equity
    GROUP BY 1, 2
),

-- Leadership representation
leadership_diversity AS (
    SELECT 
        department_name,
        DATE_TRUNC('month', TRY_TO_TIMESTAMP(hire_date)) AS month,
        COUNT(DISTINCT worker_id) AS total_leaders,
        COUNT(DISTINCT ethnicity) AS distinct_ethnicities_in_leadership,
        COUNT(DISTINCT work_location_id) AS distinct_leader_locations
    FROM employee_base
    WHERE employee_status = 'ACTIVE'
    AND manager_id IS NOT NULL  -- Assuming this indicates leadership
    GROUP BY 1, 2
)

SELECT 
    dd.month,
    dd.department_name,
    
    -- Workforce composition
    dd.total_employees,
    dd.employees_with_ethnicity_data,
    dd.distinct_ethnicities,
    dd.distinct_locations,
    dd.distinct_employment_types,
    
    -- Employment type distribution
    dd.full_time_count::FLOAT / NULLIF(dd.total_employees, 0) * 100 AS full_time_percentage,
    dd.part_time_count::FLOAT / NULLIF(dd.total_employees, 0) * 100 AS part_time_percentage,
    
    -- Leadership diversity
    ld.total_leaders,
    ld.distinct_ethnicities_in_leadership,
    ld.distinct_leader_locations,
    ld.total_leaders::FLOAT / NULLIF(dd.total_employees, 0) * 100 AS leadership_ratio,
    
    -- Pay equity metrics
    pe.avg_salary,
    pe.median_salary,
    pe.salary_75th_percentile - pe.salary_25th_percentile AS salary_spread,
    (pe.max_salary - pe.min_salary) / NULLIF(pe.median_salary, 0) * 100 AS salary_range_percentage,
    
    -- Compare to department average
    pe.avg_salary / NULLIF(da.dept_avg_salary, 0) * 100 AS salary_to_dept_avg_ratio,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at

FROM department_diversity dd
LEFT JOIN leadership_diversity ld 
    ON dd.department_name = ld.department_name 
    AND dd.month = ld.month
LEFT JOIN pay_equity pe 
    ON dd.department_name = pe.department_name 
    AND dd.month = pe.month
LEFT JOIN dept_averages da 
    ON dd.department_name = da.department_name 
    AND dd.month = da.month