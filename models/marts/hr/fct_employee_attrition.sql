WITH worker_history AS (
    SELECT
        worker_id,
        department_id,
        TRY_CAST(start_date AS DATE) as start_date,
        TRY_CAST(end_date AS DATE) as end_date,
        is_current_employee,
        DATEDIFF('day', TRY_CAST(start_date AS DATE), 
            COALESCE(TRY_CAST(end_date AS DATE), CURRENT_DATE())) as tenure_days,
        DATE_TRUNC('month', COALESCE(TRY_CAST(end_date AS DATE), CURRENT_DATE())) as analysis_month
    FROM {{ ref('base_rippling_workers') }}
),

monthly_metrics AS (
    SELECT
        department_id,
        DATE_TRUNC('month', analysis_month) as month_date,
        -- Headcount metrics
        COUNT(DISTINCT worker_id) as total_employees,
        COUNT(DISTINCT CASE WHEN is_current_employee THEN worker_id END) as active_employees,
        COUNT(DISTINCT CASE WHEN end_date IS NOT NULL 
            AND DATE_TRUNC('month', end_date) = DATE_TRUNC('month', analysis_month) 
            THEN worker_id END) as terminations,
        -- New hires
        COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', start_date) = DATE_TRUNC('month', analysis_month) 
            THEN worker_id END) as new_hires,
        -- Average tenure
        AVG(tenure_days) as avg_tenure_days
    FROM worker_history
    GROUP BY 1, 2
),

final AS (
    SELECT
        m.*,
        d.department_name,
        d.department_path,
        d.hierarchy_level,
        -- Calculate rates
        ROUND(terminations::FLOAT / NULLIF(total_employees, 0) * 100, 2) as monthly_turnover_rate,
        ROUND(new_hires::FLOAT / NULLIF(total_employees, 0) * 100, 2) as monthly_hire_rate,
        ROUND((total_employees - terminations)::FLOAT / NULLIF(total_employees, 0) * 100, 2) as retention_rate,
        ROUND(avg_tenure_days / 365.0, 2) as avg_tenure_years,
        -- Rolling metrics (12-month window)
        SUM(terminations) OVER (
            PARTITION BY m.department_id 
            ORDER BY month_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as rolling_12_month_terminations,
        AVG(terminations::FLOAT / NULLIF(total_employees, 0)) OVER (
            PARTITION BY m.department_id 
            ORDER BY month_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) * 100 as rolling_12_month_turnover_rate,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM monthly_metrics m
    LEFT JOIN {{ ref('int_rippling__department_hierarchy') }} d 
        ON m.department_id = d.department_id
)

SELECT * FROM final 