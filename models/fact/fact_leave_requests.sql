WITH leave_requests AS (
    SELECT *
    FROM {{ ref('stg_human_resources__leave_requests') }}
),

leave_balances AS (
    SELECT *
    FROM {{ ref('stg_human_resources__leave_balances') }}
),

workers AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

final AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['lr.leave_request_id', 'lr.worker_id', 'lr.created_at']) }} AS leave_request_key,
        
        -- Foreign Keys
        lr.leave_request_id,
        lr.worker_id,
        w.department_id,
        lr.leave_type_id,
        
        -- Request Details
        lr.leave_status AS status,
        lr.leave_reason,
        lr.leave_type_name,
        
        -- Date and Time Information
        lr.leave_start_datetime,
        lr.leave_end_datetime,
        DATEDIFF('day', lr.leave_start_datetime, lr.leave_end_datetime) + 1 AS duration_days,
        lr.total_minutes_requested,
        
        -- Review Information
        lr.reviewer_name,
        lr.reviewer_email,
        lr.reviewed_at,
        
        -- Balance Information
        lb.balance_excluding_requests AS balance_before_request,
        lb.balance_including_requests AS balance_after_request,
        lb.unlimited_balance AS is_unlimited_balance,
        
        -- Request Status Flags
        CASE 
            WHEN lr.leave_status = 'APPROVED' THEN TRUE
            ELSE FALSE 
        END AS is_approved,
        
        CASE 
            WHEN lr.leave_status = 'PENDING' THEN TRUE
            ELSE FALSE 
        END AS is_pending,
        
        CASE 
            WHEN lr.managed_by_external_system THEN TRUE
            ELSE FALSE 
        END AS is_external_managed,
        
        -- Timing Metrics
        DATEDIFF('hour', lr.created_at, COALESCE(lr.reviewed_at, CURRENT_TIMESTAMP())) AS hours_to_review,
        
        -- Meta Data
        lr.created_at,
        lr.updated_at,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM leave_requests lr
    LEFT JOIN workers w 
        ON lr.worker_id = w.worker_id
    LEFT JOIN leave_balances lb 
        ON lr.worker_id = lb.worker_id 
        AND lr.leave_type_id = lb.leave_type_id
)

SELECT * FROM final