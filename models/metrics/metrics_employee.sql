WITH employee_base AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

monthly_headcount AS (
    SELECT 
        DATE_TRUNC('month', TRY_TO_TIMESTAMP(hire_date)) AS month,
        department_name,
        employment_type,
        COUNT(DISTINCT worker_id) AS total_employees,
        SUM(CASE WHEN employee_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_employees,
        SUM(CASE WHEN termination_reason IS NOT NULL THEN 1 ELSE 0 END) AS terminated_employees
    FROM employee_base
    GROUP BY 1, 2, 3
),

turnover_calc AS (
    SELECT
        month,
        department_name,
        terminated_employees::FLOAT / NULLIF(total_employees, 0) * 100 AS turnover_rate,
        total_employees,
        active_employees,
        terminated_employees
    FROM monthly_headcount
),

tenure_calc AS (
    SELECT
        department_name,
        employment_type,
        AVG(DATEDIFF('month', TRY_TO_TIMESTAMP(hire_date), COALESCE(TRY_TO_TIMESTAMP(last_updated), CURRENT_TIMESTAMP()))) AS avg_tenure_months,
        MEDIAN(DATEDIFF('month', TRY_TO_TIMESTAMP(hire_date), COALESCE(TRY_TO_TIMESTAMP(last_updated), CURRENT_TIMESTAMP()))) AS median_tenure_months
    FROM employee_base
    WHERE employee_status = 'ACTIVE'
    GROUP BY 1, 2
)

SELECT 
    t.month,
    t.department_name,
    t.total_employees,
    t.active_employees,
    t.terminated_employees,
    t.turnover_rate,
    tn.avg_tenure_months,
    tn.median_tenure_months,
    t.active_employees::FLOAT / NULLIF(t.total_employees, 0) * 100 AS retention_rate,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM turnover_calc t
LEFT JOIN tenure_calc tn 
    ON t.department_name = tn.department_name