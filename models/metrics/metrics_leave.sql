WITH leave_requests_base AS (
    SELECT *
    FROM {{ ref('fact_leave_requests') }}
),

employee_dim AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

leave_stats AS (
    SELECT 
        e.department_name,
        e.employment_type,
        DATE_TRUNC('month', l.leave_start_datetime) AS analysis_month,
        l.leave_type_name,
        COUNT(DISTINCT l.leave_request_id) AS total_requests,
        COUNT(DISTINCT l.worker_id) AS unique_requesters,
        SUM(l.duration_days) AS total_leave_days,
        AVG(l.duration_days) AS avg_leave_duration,
        SUM(CASE WHEN l.is_approved THEN 1 ELSE 0 END) AS approved_requests,
        AVG(l.hours_to_review) AS avg_review_time,
        
        -- Advanced metrics
        COUNT(DISTINCT CASE WHEN l.is_approved THEN l.worker_id END) AS workers_with_approved_leave,
        COUNT(DISTINCT CASE WHEN l.is_pending THEN l.worker_id END) AS workers_with_pending_leave
    FROM leave_requests_base l
    JOIN employee_dim e ON l.worker_id = e.worker_id
    GROUP BY 1, 2, 3, 4
),

concurrent_leaves AS (
    SELECT
        e.department_name,
        DATE_TRUNC('month', l.leave_start_datetime) AS analysis_month,
        COUNT(DISTINCT l.leave_request_id) AS concurrent_leaves
    FROM leave_requests_base l
    JOIN employee_dim e ON l.worker_id = e.worker_id
    WHERE l.is_approved = TRUE
    GROUP BY 1, 2
),

seasonal_patterns AS (
    SELECT
        e.department_name,
        DATE_TRUNC('month', l.leave_start_datetime) AS analysis_month,
        l.leave_type_name,
        COUNT(DISTINCT l.leave_request_id) AS monthly_requests
    FROM leave_requests_base l
    JOIN employee_dim e ON l.worker_id = e.worker_id
    GROUP BY 1, 2, 3
)

SELECT 
    ls.department_name,
    ls.employment_type,
    ls.analysis_month,
    ls.leave_type_name,
    
    -- Request volumes
    ls.total_requests,
    ls.unique_requesters,
    ls.total_leave_days,
    ls.avg_leave_duration,
    
    -- Approval metrics
    ls.approved_requests,
    ls.approved_requests::FLOAT / NULLIF(ls.total_requests, 0) * 100 AS approval_rate,
    ls.avg_review_time,
    
    -- Workforce impact
    ls.workers_with_approved_leave::FLOAT / NULLIF(ls.unique_requesters, 0) * 100 AS approved_workers_percentage,
    ls.workers_with_pending_leave::FLOAT / NULLIF(ls.unique_requesters, 0) * 100 AS pending_workers_percentage,
    
    -- Concurrent leave impact
    cl.concurrent_leaves,
    cl.concurrent_leaves::FLOAT / NULLIF(ls.unique_requesters, 0) * 100 AS concurrent_leave_percentage,
    
    -- Seasonal patterns
    sp.monthly_requests,
    sp.monthly_requests - LAG(sp.monthly_requests) OVER (
        PARTITION BY ls.department_name, ls.leave_type_name 
        ORDER BY ls.analysis_month
    ) AS month_over_month_change,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at

FROM leave_stats ls
LEFT JOIN concurrent_leaves cl 
    ON ls.department_name = cl.department_name 
    AND ls.analysis_month = cl.analysis_month
LEFT JOIN seasonal_patterns sp 
    ON ls.department_name = sp.department_name 
    AND ls.analysis_month = sp.analysis_month 
    AND ls.leave_type_name = sp.leave_type_name