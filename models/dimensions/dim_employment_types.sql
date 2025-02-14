WITH employment_types AS (
    SELECT *
    FROM {{ ref('stg_human_resources__employment_types') }}
),

final AS (
    SELECT
        -- Primary Key
        employment_type_id,
        
        -- Employment Type Details
        name AS employment_type_name,
        type AS employment_category,
        label AS employment_label,
        
        -- Additional Attributes
        amount_worked,
        compensation_time_period,
        
        -- Classification
        CASE 
            WHEN type ILIKE '%full%time%' THEN 'Full-Time'
            WHEN type ILIKE '%part%time%' THEN 'Part-Time'
            WHEN type ILIKE '%contractor%' THEN 'Contractor'
            WHEN type ILIKE '%intern%' THEN 'Intern'
            ELSE 'Other'
        END AS employment_class,
        
        -- Type 2 SCD Fields
        created_at AS valid_from,
        COALESCE(updated_at, CURRENT_TIMESTAMP()) AS valid_to,
        TRUE AS is_current,
        
        -- Audit Fields
        created_at,
        updated_at,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM employment_types
)

SELECT * FROM final