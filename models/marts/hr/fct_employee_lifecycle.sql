WITH employee_tenure_bands AS (
    SELECT
        worker_id,
        department_id,
        start_date,
        end_date,
        DATEDIFF('month', start_date, CURRENT_DATE()) as months_of_service,
        CASE 
            WHEN DATEDIFF('month', start_date, CURRENT_DATE()) < 3 THEN 'New Hire (< 3 months)'
            WHEN DATEDIFF('month', start_date, CURRENT_DATE()) < 12 THEN '3-12 months'
            WHEN DATEDIFF('month', start_date, CURRENT_DATE()) < 24 THEN '1-2 years'
            WHEN DATEDIFF('month', start_date, CURRENT_DATE()) < 60 THEN '2-5 years'
            ELSE '5+ years'
        END as tenure_band,
        is_current_employee
    FROM {{ ref('base_rippling_workers') }}
),

compensation_data AS (
    SELECT 
        worker_id,
        calculated_annual_salary,
        compensation_frequency
    FROM {{ ref('int_rippling__worker_compensation') }}
    WHERE is_current_record = true
)

SELECT
    et.*,
    d.department_name,
    d.department_path,
    c.calculated_annual_salary,
    -- Risk metrics
    CASE 
        WHEN et.months_of_service < 12 AND c.calculated_annual_salary < d.avg_compensation_per_employee THEN 'High'
        WHEN et.months_of_service < 24 THEN 'Medium'
        ELSE 'Low'
    END as attrition_risk,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM employee_tenure_bands et
LEFT JOIN {{ ref('int_rippling__department_hierarchy') }} d 
    ON et.department_id = d.department_id
LEFT JOIN compensation_data c 
    ON et.worker_id = c.worker_id 