WITH compensations AS (
    SELECT *
    FROM {{ ref('stg_human_resources__compensations') }}
),

workers AS (
    SELECT *
    FROM {{ ref('dim_employees') }}
),

final AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['c.compensation_id', 'c.worker_id', 'c.created_at']) }} AS compensation_key,
        
        -- Foreign Keys
        c.worker_id,
        w.department_id,
        w.employment_type_id,
        
        -- Compensation Amounts
        COALESCE(c.annual_salary::FLOAT, 0) AS annual_salary,
        COALESCE(c.hourly_wage::FLOAT, 0) AS hourly_wage,
        COALESCE(c.monthly_salary::FLOAT, 0) AS monthly_salary,
        COALESCE(c.weekly_salary::FLOAT, 0) AS weekly_salary,
        COALESCE(c.target_bonus::FLOAT, 0) AS target_bonus,
        COALESCE(c.signing_bonus::FLOAT, 0) AS signing_bonus,
        COALESCE(c.relocation_reimbursement::FLOAT, 0) AS relocation_amount,
        COALESCE(c.on_target_commission::FLOAT, 0) AS target_commission,
        
        -- Compensation Details
        c.payment_type,
        c.bonus_schedule,
        COALESCE(c.target_bonus_pct::FLOAT, 0) AS target_bonus_percentage,
        
        -- Effective Dates
        c.created_at AS effective_start_date,
        COALESCE(c.updated_at, CURRENT_TIMESTAMP()) AS effective_end_date,
        
        -- Calculated Fields
        CASE 
            WHEN c.payment_type = 'HOURLY' THEN c.hourly_wage::FLOAT * 2080 -- Assuming 2080 working hours per year
            ELSE c.annual_salary::FLOAT
        END AS annualized_salary,
        
        -- Meta Data
        c.created_at,
        c.updated_at,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM compensations c
    LEFT JOIN workers w 
        ON c.worker_id = w.worker_id
)

SELECT * FROM final