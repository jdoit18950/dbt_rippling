WITH leave_requests_base AS (
    SELECT *
    FROM {{ ref('base_rippling_leave_requests') }}
),

worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

final AS (
    SELECT
        lr.leave_request_id,
        lr.worker_id,
        w.department_id,
        lr.leave_type_name,
        lr.is_paid_leave,
        lr.leave_status,
        TRY_CAST(lr.start_date AS DATE) as start_date,
        TRY_CAST(lr.end_date AS DATE) as end_date,
        lr.leave_duration_days,
        lr.leave_duration_hours,
        -- Calculate YTD metrics
        SUM(lr.leave_duration_days) OVER (
            PARTITION BY lr.worker_id, DATE_TRUNC('year', TRY_CAST(lr.start_date AS DATE))
        ) as total_leave_days_ytd,
        SUM(CASE WHEN lr.is_paid_leave THEN lr.leave_duration_days ELSE 0 END) OVER (
            PARTITION BY lr.worker_id, DATE_TRUNC('year', TRY_CAST(lr.start_date AS DATE))
        ) as paid_leave_days_ytd,
        -- Calculate leave frequency
        COUNT(*) OVER (
            PARTITION BY lr.worker_id, DATE_TRUNC('year', TRY_CAST(lr.start_date AS DATE))
        ) as leave_requests_count_ytd,
        -- Flag current and upcoming leaves
        lr.is_current_leave,
        lr.is_future_leave,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM leave_requests_base lr
    LEFT JOIN worker_data w 
        ON lr.worker_id = w.worker_id
)

SELECT * FROM final 