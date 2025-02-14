WITH time_entries AS (
    SELECT *
    FROM {{ ref('stg_human_resources__time_entries') }}
),

workers AS (
    SELECT *
    FROM {{ ref('stg_human_resources__workers') }}
),

departments AS (
    SELECT *
    FROM {{ ref('stg_human_resources__departments') }}
),

final AS (
    SELECT 
        -- Primary Keys
        te.time_entry_id,
        te.worker_id,
        te.time_card_id,
        
        -- Foreign Keys
        w.department_id,
        w.employment_type_id,
        
        -- Dates
        DATE_TRUNC('day', te.start_time) AS entry_date,
        te.pay_period_start,
        te.pay_period_end,
        
        -- Worker Details
        w.display_name AS employee_name,
        d.department_name,
        
        -- Time Metrics
        te.total_hours_worked,
        te.overtime_hours,
        te.regular_hours,
        te.paid_hours,
        te.total_paid_time_off_hours,
        te.total_unpaid_time_off_hours,
        
        -- Calculated Fields
        CASE 
            WHEN te.overtime_hours > 0 THEN TRUE 
            ELSE FALSE 
        END AS has_overtime,
        
        total_hours_worked / NULLIF(paid_hours, 0) AS utilization_rate,
        
        -- Metadata
        te.created_at,
        te.updated_at,
        CURRENT_TIMESTAMP() AS _loaded_at
        
    FROM time_entries te
    LEFT JOIN workers w 
        ON te.worker_id = w.worker_id
    LEFT JOIN departments d 
        ON w.department_id = d.department_id
)

SELECT * FROM final