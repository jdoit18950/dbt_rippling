{{ config(materialized='table') }}

WITH workers AS (
    SELECT * FROM {{ ref('stg_human_resources__workers') }}
),
departments AS (
    SELECT * FROM {{ ref('stg_human_resources__departments') }}
),
employment_types AS (
    SELECT * FROM {{ ref('stg_human_resources__employment_types') }}
)
SELECT
    w.worker_id,
    w.display_name,
    w.work_email,
    d.department_name,
    w.status,
    w.hire_date,
    w.termination_reason
FROM workers w
LEFT JOIN departments d ON w.department_id = d.department_id
LEFT JOIN employment_types e ON w.employment_type_id = e.employment_type_id