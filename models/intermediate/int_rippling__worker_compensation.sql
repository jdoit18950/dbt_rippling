WITH worker_comp_base AS (
    SELECT *
    FROM {{ ref('base_rippling_compensations') }}
),

worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

final AS (
    SELECT
        wcb.worker_id,
        w.department_id,
        wcb.effective_date,
        wcb.end_date,
        wcb.compensation_type,
        wcb.compensation_amount,
        wcb.compensation_frequency,
        -- Standardize annual salary calculation based on frequency
        CASE 
            WHEN compensation_frequency = 'HOURLY' THEN compensation_amount * 2080  -- 40 hours * 52 weeks
            WHEN compensation_frequency = 'WEEKLY' THEN compensation_amount * 52
            WHEN compensation_frequency = 'BI_WEEKLY' THEN compensation_amount * 26
            WHEN compensation_frequency = 'SEMI_MONTHLY' THEN compensation_amount * 24
            WHEN compensation_frequency = 'MONTHLY' THEN compensation_amount * 12
            WHEN compensation_frequency = 'ANNUALLY' THEN compensation_amount
            ELSE NULL
        END as calculated_annual_salary,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM worker_comp_base wcb
    LEFT JOIN worker_data w 
        ON wcb.worker_id = w.worker_id
    WHERE wcb.compensation_type = 'BASE_SALARY'  -- Focus on base salary compensation
        AND wcb.is_active = true  -- Only active compensation records
)

SELECT * FROM final 