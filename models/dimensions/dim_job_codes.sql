WITH job_codes AS (
    SELECT *
    FROM {{ ref('stg_human_resources__job_codes') }}
),

final AS (
    SELECT
        -- Primary Key
        job_code_id,
        
        -- Job Code Details
        job_code_name,
        job_group_id,
        
        -- Job Dimension Details
        job_dimension_id,
        job_dimension_name,
        job_dimension_external_id,
        job_roster_type,
        
        -- Additional Attributes
        job_custom_location,
        job_dimension_group_id,
        
        -- Classification Fields
        CASE 
            WHEN job_roster_type IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS is_roster_position,
        
        -- Type 2 SCD Fields
        created_at AS valid_from,
        COALESCE(updated_at, CURRENT_TIMESTAMP()) AS valid_to,
        TRUE AS is_current,
        
        -- Audit Fields
        created_at,
        updated_at,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM job_codes
)

SELECT * FROM final