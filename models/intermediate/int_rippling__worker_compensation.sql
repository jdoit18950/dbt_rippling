WITH worker_comp_base AS (
    SELECT *
    FROM {{ ref('base_rippling_compensations') }}
),

worker_data AS (
    SELECT *
    FROM {{ ref('base_rippling_workers') }}
),

compensation_with_history AS (
    SELECT
        wcb.worker_id,
        w.department_id,
        wcb.created_at,
        wcb.updated_at,
        -- Use the pre-calculated annual salary from base model
        wcb.calculated_annual_salary,
        -- Determine compensation frequency based on which amount is not null
        CASE 
            WHEN wcb.hourly_wage_amount IS NOT NULL THEN 'HOURLY'
            WHEN wcb.weekly_compensation_amount IS NOT NULL THEN 'WEEKLY'
            WHEN wcb.monthly_compensation_amount IS NOT NULL THEN 'MONTHLY'
            WHEN wcb.annual_compensation_amount IS NOT NULL THEN 'ANNUALLY'
            ELSE NULL
        END as compensation_frequency,
        -- Track if this is the current record
        wcb.is_current_record,
        -- Add row number for historical tracking
        ROW_NUMBER() OVER (
            PARTITION BY wcb.worker_id 
            ORDER BY wcb.created_at DESC
        ) as compensation_version,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM worker_comp_base wcb
    LEFT JOIN worker_data w 
        ON wcb.worker_id = w.worker_id
    WHERE w.is_current_employee = true  -- Only include active employees
),

final AS (
    SELECT 
        *,
        -- Calculate compensation change metrics
        LAG(calculated_annual_salary) OVER (
            PARTITION BY worker_id 
            ORDER BY created_at
        ) as previous_annual_salary,
        ROUND(100.0 * (
            calculated_annual_salary - LAG(calculated_annual_salary) OVER (
                PARTITION BY worker_id 
                ORDER BY created_at
            )
        ) / NULLIF(LAG(calculated_annual_salary) OVER (
            PARTITION BY worker_id 
            ORDER BY created_at
        ), 0), 2) as salary_change_percentage,
        -- Calculate department-level metrics
        AVG(calculated_annual_salary) OVER (
            PARTITION BY department_id
        ) as dept_avg_salary,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY calculated_annual_salary) 
            OVER (PARTITION BY department_id) as dept_median_salary,
        -- Calculate company-wide metrics
        AVG(calculated_annual_salary) OVER () as company_avg_salary,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY calculated_annual_salary) 
            OVER () as company_median_salary
    FROM compensation_with_history
)

SELECT * FROM final 