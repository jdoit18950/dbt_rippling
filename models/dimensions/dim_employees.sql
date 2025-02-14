WITH workers AS (
    SELECT *
    FROM {{ ref('stg_human_resources__workers') }}
),

departments AS (
    SELECT *
    FROM {{ ref('stg_human_resources__departments') }}
),

employment_types AS (
    SELECT *
    FROM {{ ref('stg_human_resources__employment_types') }}
),

compensations AS (
    SELECT *
    FROM {{ ref('stg_human_resources__compensations') }}
),

final AS (
    SELECT
        -- Primary Key
        w.worker_id,
        
        -- Natural Keys
        w.user_id,
        w.display_name,
        w.work_email,
        
        -- Foreign Keys
        w.department_id,
        w.employment_type_id,
        w.manager_id,
        w.work_location_id,
        
        -- Employee Details
        w.status AS employee_status,
        w.date_of_birth,
        w.gender,
        w.ethnicity,
        et.type AS employment_type,
        et.name AS employment_type_name,
        d.department_name,
        
        -- Dates
        w.hire_date,
        w.last_updated,
        
        -- Compensation Details
        w.annual_salary,
        w.hourly_wage,
        w.payment_type,
        w.target_bonus,
        
        -- Manager Path (for hierarchy)
        w.manager_id AS direct_manager_id,
        
        -- Termination Info
        w.termination_reason,
        w.termination_type,
        
        -- Type 2 SCD Fields
        COALESCE(w.last_updated, CURRENT_TIMESTAMP()) AS valid_to,
        CASE 
            WHEN w.status = 'ACTIVE' THEN TRUE 
            ELSE FALSE 
        END AS is_current,
        
        -- Audit Fields
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM workers w
    LEFT JOIN departments d 
        ON w.department_id = d.department_id
    LEFT JOIN employment_types et 
        ON w.employment_type_id = et.employment_type_id
    LEFT JOIN compensations c 
        ON w.worker_id = c.worker_id
)

SELECT * FROM final