WITH time_entries_base AS (
    SELECT *
    FROM {{ ref('fact_time_entries') }}
),

employee_dim AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

attendance_stats AS (
    SELECT 
        e.department_name,
        e.employment_type,
        DATE_TRUNC('month', t.work_date) AS analysis_month,
        COUNT(DISTINCT t.worker_id) AS total_employees,
        COUNT(DISTINCT t.time_entry_id) AS total_entries,
        
        -- Time metrics
        SUM(t.total_hours_worked) AS total_hours_worked,
        SUM(t.overtime_hours) AS total_overtime_hours,
        SUM(t.total_paid_time_off_hours) AS total_pto_hours,
        AVG(t.utilization_rate) AS avg_utilization_rate,
        
        -- Daily averages
        AVG(t.total_hours_worked) AS avg_daily_hours,
        
        -- Peak time analysis
        MAX(t.total_hours_worked) AS peak_daily_hours,
        MIN(t.total_hours_worked) AS min_daily_hours
        
    FROM time_entries_base t
    JOIN employee_dim e ON t.worker_id = e.worker_id
    GROUP BY 1, 2, 3
),

weekly_patterns AS (
    SELECT 
        e.department_name,
        DATE_TRUNC('month', t.work_date) AS analysis_month,
        t.day_of_week,
        AVG(t.total_hours_worked) AS avg_hours_by_day,
        COUNT(DISTINCT t.time_entry_id) AS entries_by_day
    FROM time_entries_base t
    JOIN employee_dim e ON t.worker_id = e.worker_id
    GROUP BY 1, 2, 3
),

utilization_bands AS (
    SELECT
        e.department_name,
        DATE_TRUNC('month', t.work_date) AS analysis_month,
        COUNT(DISTINCT CASE WHEN t.utilization_rate >= 0.9 THEN t.worker_id END) AS high_utilization_count,
        COUNT(DISTINCT CASE WHEN t.utilization_rate < 0.7 THEN t.worker_id END) AS low_utilization_count
    FROM time_entries_base t
    JOIN employee_dim e ON t.worker_id = e.worker_id
    GROUP BY 1, 2
)

SELECT 
    a.department_name,
    a.employment_type,
    a.analysis_month,
    
    -- Headcount metrics
    a.total_employees,
    a.total_entries,
    
    -- Time metrics
    a.total_hours_worked,
    a.total_overtime_hours,
    a.total_pto_hours,
    a.avg_utilization_rate,
    
    -- Calculated metrics
    a.total_overtime_hours / NULLIF(a.total_hours_worked, 0) * 100 AS overtime_percentage,
    a.total_pto_hours / NULLIF(a.total_hours_worked, 0) * 100 AS pto_percentage,
    a.total_hours_worked / NULLIF(a.total_employees, 0) AS avg_hours_per_employee,
    
    -- Workload distribution
    a.peak_daily_hours - a.min_daily_hours AS daily_hours_variation,
    a.avg_daily_hours,
    
    -- Utilization metrics
    u.high_utilization_count,
    u.low_utilization_count,
    u.high_utilization_count::FLOAT / NULLIF(a.total_employees, 0) * 100 AS high_utilization_percentage,
    
    -- Weekly pattern indicators
    MAX(w.avg_hours_by_day) AS busiest_day_hours,
    MIN(w.avg_hours_by_day) AS slowest_day_hours,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at

FROM attendance_stats a
LEFT JOIN utilization_bands u 
    ON a.department_name = u.department_name 
    AND a.analysis_month = u.analysis_month
LEFT JOIN weekly_patterns w 
    ON a.department_name = w.department_name 
    AND a.analysis_month = w.analysis_month
GROUP BY 
    1, 2, 3, 4, 5, 6, 7, 8, 9,
    10, 11, 12, 13, 14, 15, 16, 17