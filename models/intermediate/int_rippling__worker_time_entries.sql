WITH time_entries_base AS (
    SELECT *
    FROM {{ ref('base_rippling_time_entries') }}
),

worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

final AS (
    SELECT
        te.time_entry_id,
        te.worker_id,
        w.department_id,
        te.entry_date,
        te.entry_day_of_week,
        te.start_time,
        te.end_time,
        te.calculated_hours,
        -- Standard work day is 8 hours
        CASE 
            WHEN te.calculated_hours <= 8 THEN te.calculated_hours
            ELSE 8
        END as regular_hours,
        -- Overtime is anything over 8 hours
        CASE 
            WHEN te.calculated_hours > 8 THEN te.calculated_hours - 8
            ELSE 0
        END as overtime_hours,
        -- Calculate running totals by month
        SUM(te.calculated_hours) OVER (
            PARTITION BY te.worker_id, DATE_TRUNC('month', te.entry_date)
            ORDER BY te.entry_date
        ) as cumulative_hours_mtd,
        -- Calculate average hours per day
        AVG(te.calculated_hours) OVER (
            PARTITION BY te.worker_id, DATE_TRUNC('month', te.entry_date)
        ) as avg_hours_per_day_mtd,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM time_entries_base te
    LEFT JOIN worker_data w 
        ON te.worker_id = w.worker_id
    WHERE te.is_current_pay_period = true
)

SELECT * FROM final 