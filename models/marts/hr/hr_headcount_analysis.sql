WITH worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

department_data AS (
    SELECT *
    FROM {{ ref('base_rippling_departments') }}
),

monthly_headcount AS (
    SELECT
        d.department_id,
        d.department_name,
        d.department_path,
        DATE_TRUNC('MONTH', w.start_date) as month_date,
        COUNT(DISTINCT w.worker_id) as total_headcount,
        SUM(CASE WHEN w.start_date >= DATE_TRUNC('MONTH', CURRENT_DATE()) THEN 1 ELSE 0 END) as new_hires,
        SUM(CASE WHEN w.end_date >= DATE_TRUNC('MONTH', CURRENT_DATE()) THEN 1 ELSE 0 END) as terminations,
        SUM(CASE WHEN w.is_current_employee THEN 1 ELSE 0 END) as active_employees,
        -- Calculate growth metrics
        LAG(COUNT(DISTINCT w.worker_id)) OVER (PARTITION BY d.department_id ORDER BY DATE_TRUNC('MONTH', w.start_date)) as previous_month_headcount,
        ROUND(100.0 * (
            COUNT(DISTINCT w.worker_id) - LAG(COUNT(DISTINCT w.worker_id)) 
            OVER (PARTITION BY d.department_id ORDER BY DATE_TRUNC('MONTH', w.start_date))
        ) / NULLIF(LAG(COUNT(DISTINCT w.worker_id)) 
            OVER (PARTITION BY d.department_id ORDER BY DATE_TRUNC('MONTH', w.start_date)), 0), 2) as month_over_month_growth
    FROM worker_data w
    LEFT JOIN department_data d ON w.department_id = d.department_id
    GROUP BY 1, 2, 3, 4
)

SELECT 
    *,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM monthly_headcount 