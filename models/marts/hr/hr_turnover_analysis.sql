WITH worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

department_data AS (
    SELECT *
    FROM {{ ref('base_rippling_departments') }}
),

turnover_metrics AS (
    SELECT
        d.department_id,
        d.department_name,
        d.department_path,
        DATE_TRUNC('MONTH', w.end_date) as month_date,
        COUNT(DISTINCT CASE WHEN w.end_date IS NOT NULL THEN w.worker_id END) as terminations,
        COUNT(DISTINCT w.worker_id) as total_employees,
        -- Calculate turnover rate
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN w.end_date IS NOT NULL THEN w.worker_id END) / 
            NULLIF(COUNT(DISTINCT w.worker_id), 0), 2) as turnover_rate,
        -- Voluntary vs Involuntary turnover
        COUNT(DISTINCT CASE WHEN w.termination_type = 'VOLUNTARY' THEN w.worker_id END) as voluntary_terminations,
        COUNT(DISTINCT CASE WHEN w.termination_type = 'INVOLUNTARY' THEN w.worker_id END) as involuntary_terminations,
        -- Average tenure of terminated employees
        AVG(DATEDIFF('month', w.start_date, w.end_date)) as avg_tenure_months
    FROM worker_data w
    LEFT JOIN department_data d ON w.department_id = d.department_id
    WHERE w.end_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT 
    *,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM turnover_metrics 