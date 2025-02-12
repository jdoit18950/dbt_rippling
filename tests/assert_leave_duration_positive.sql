SELECT 
    leave_request_id,
    leave_duration_days
FROM {{ ref('int_rippling__worker_leaves') }}
WHERE leave_duration_days <= 0 