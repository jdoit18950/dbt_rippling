WITH time_entries AS (
    SELECT *
    FROM {{ ref('stg_human_resources__time_entries') }}
),

workers AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

final AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['te.time_entry_id', 'te.worker_id', 'te.start_time']) }} AS time_entry_key,
        
        -- Foreign Keys
        te.time_entry_id,
        te.worker_id,
        te.time_card_id,
        w.department_id,
        w.employment_type_id,
        
        -- Time Entry Details
        te.entry_status,
        te.start_time,
        te.end_time,
        
        -- Time Card Summary
        te.total_hours_worked,
        te.overtime_hours,
        te.regular_hours,
        te.paid_hours,
        te.total_paid_time_off_hours,
        te.total_unpaid_time_off_hours,
        
        -- Pay Period Information
        te.pay_period_start,
        te.pay_period_end,
        
        -- Calculated Fields
        DATEDIFF('minute', te.start_time, te.end_time) AS duration_minutes,
        CASE 
            WHEN te.overtime_hours > 0 THEN TRUE
            ELSE FALSE 
        END AS has_overtime,
        
        -- Utilization Metrics
        CASE 
            WHEN te.paid_hours > 0 THEN te.total_hours_worked / te.paid_hours 
            ELSE NULL 
        END AS utilization_rate,
        
        -- Time Categories
        DATE_TRUNC('day', te.start_time) AS work_date,
        DATE_TRUNC('week', te.start_time) AS work_week,
        DATE_TRUNC('month', te.start_time) AS work_month,
        EXTRACT(DOW FROM te.start_time) AS day_of_week,
        
        -- Meta Data
        te.created_at,
        te.updated_at,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM time_entries te
    LEFT JOIN workers w 
        ON te.worker_id = w.worker_id
)

SELECT * FROM final