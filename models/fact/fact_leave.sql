{{ config(materialized='table') }}

WITH leave_summary AS (
    SELECT * FROM {{ ref('int_human_resources__leave_summary') }}
)

SELECT
    worker_id,
    leave_type_id,
    SUM(hours_taken_off) AS total_leave_hours,
    COUNT(DISTINCT leave_day_taken) AS total_leave_days,
    SUM(CASE WHEN is_paid_leave THEN hours_taken_off ELSE 0 END) AS total_paid_leave_hours,
    SUM(CASE WHEN NOT is_paid_leave THEN hours_taken_off ELSE 0 END) AS total_unpaid_leave_hours

FROM leave_summary
GROUP BY worker_id, leave_type_id