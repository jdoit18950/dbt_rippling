{{ config(materialized='table') }}

WITH time_entries AS (
    SELECT
        worker_id,
        time_card_id,
        start_time,
        end_time,
        pay_period_start,
        pay_period_end,
        total_hours_worked,
        overtime_hours,
        regular_hours,
        paid_hours
    FROM {{ ref('stg_human_resources__time_entries') }}
),

workers AS (
    SELECT worker_id, display_name, department_id
    FROM {{ ref('stg_human_resources__workers') }}
),

departments AS (
    SELECT department_id, department_name
    FROM {{ ref('stg_human_resources__departments') }}

)

SELECT
    t.worker_id,
    w.display_name,
    d.department_name,
    
    -- Aggregated Attendance Data
    COUNT(t.time_card_id) AS total_shifts,
    SUM(t.total_hours_worked) AS total_hours,
    SUM(t.overtime_hours) AS total_overtime,
    SUM(t.regular_hours) AS regular_hours,
    SUM(t.paid_hours) AS paid_hours,

    -- Pay Period Details
    MIN(t.pay_period_start) AS first_pay_period,
    MAX(t.pay_period_end) AS last_pay_period

FROM time_entries t
LEFT JOIN workers w ON t.worker_id = w.worker_id
LEFT JOIN departments d ON w.department_id = d.department_id

GROUP BY 
    t.worker_id, w.display_name, d.department_name
