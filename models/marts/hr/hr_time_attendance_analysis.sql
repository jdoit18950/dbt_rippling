WITH time_entries AS (
    SELECT *
    FROM {{ ref('base_rippling_time_entries') }}
),

worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

final AS (
    SELECT
        t.time_entry_id,
        w.worker_id,
        w.job_title,
        w.department_id,
        t.entry_date,
        t.entry_day_of_week,
        t.start_time,
        t.end_time,
        t.calculated_hours,
        t.regular_hours,
        t.overtime_hours,
        t.holiday_hours,
        t.paid_time_off_hours,
        -- Calculate metrics
        AVG(t.calculated_hours) OVER (PARTITION BY w.worker_id, DATE_TRUNC('MONTH', t.entry_date)) as avg_hours_per_day_mtd,
        SUM(t.overtime_hours) OVER (PARTITION BY w.worker_id, DATE_TRUNC('MONTH', t.entry_date)) as total_overtime_hours_mtd,
        COUNT(*) OVER (PARTITION BY w.worker_id, DATE_TRUNC('MONTH', t.entry_date)) as days_worked_mtd,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM time_entries t
    INNER JOIN worker_data w ON t.worker_id = w.worker_id
)

SELECT * FROM final 