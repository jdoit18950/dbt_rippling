WITH leave_data AS (
    SELECT *
    FROM {{ ref('base_rippling_leave_requests') }}
),

worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

leave_types AS (
    SELECT *
    FROM {{ source('rippling', 'leave_types') }}
),

final AS (
    SELECT
        l.leave_request_id,
        w.worker_id,
        w.job_title,
        w.department_id,
        lt.name as leave_type_name,
        lt.is_paid,
        l.leave_status,
        l.start_date,
        l.end_date,
        l.leave_duration_days,
        l.leave_duration_hours,
        l.is_current_leave,
        l.is_future_leave,
        -- Calculate metrics
        SUM(l.leave_duration_days) OVER (PARTITION BY w.worker_id, DATE_TRUNC('YEAR', l.start_date)) as total_leave_days_ytd,
        SUM(CASE WHEN lt.is_paid THEN l.leave_duration_days ELSE 0 END) 
            OVER (PARTITION BY w.worker_id, DATE_TRUNC('YEAR', l.start_date)) as paid_leave_days_ytd,
        COUNT(*) OVER (PARTITION BY w.worker_id, DATE_TRUNC('YEAR', l.start_date)) as leave_requests_count_ytd,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM leave_data l
    INNER JOIN worker_data w ON l.worker_id = w.worker_id
    LEFT JOIN leave_types lt ON l.leave_type_id = lt.id
)

SELECT * FROM final 