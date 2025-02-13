{{ config(materialized='table') }}

WITH workers AS (
    SELECT 
        worker_id, 
        display_name, 
        work_email,
        department_id,
        employment_type_id,
        status AS employment_status,
        start_date AS hire_date,
        termination_details:reason::STRING AS termination_reason,
        termination_details:type::STRING AS termination_type
    FROM {{ ref('stg_human_resources__workers') }}
),

departments AS (
    SELECT 
        department_id, 
        department_name 
    FROM {{ ref('stg_human_resources__departments') }}
),

job_codes AS (
    SELECT 
        job_code_id, 
        job_code_name 
    FROM {{ ref('stg_human_resources__job_codes') }}
),

employment_types AS (
    SELECT 
        employment_type_id, 
        employment_type_name 
    FROM {{ ref('stg_human_resources__employment_types') }}
)

SELECT
    w.worker_id,
    w.display_name,
    w.work_email,
    d.department_name,
    j.job_code_name,
    e.employment_type_name,
    w.employment_status,
    w.hire_date,
    w.termination_reason,
    w.termination_type

FROM workers w
LEFT JOIN departments d ON w.department_id = d.department_id
LEFT JOIN job_codes j ON w.job_code_id = j.job_code_id
LEFT JOIN employment_types e ON w.employment_type_id = e.employment_type_id;
