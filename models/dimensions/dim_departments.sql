WITH departments AS (
    SELECT *
    FROM {{ ref('stg_human_resources__departments') }}
),

department_hierarchy AS (
    SELECT
        d.department_id,
        d.department_name,
        d.parent_department_id,
        p.department_name AS parent_department_name,
        CASE
            WHEN d.parent_department_id IS NULL THEN 1
            ELSE 2 -- Can be extended for deeper hierarchies
        END AS department_level
    FROM departments d
    LEFT JOIN departments p 
        ON d.parent_department_id = p.department_id
),

final AS (
    SELECT
        -- Primary Key
        d.department_id,
        
        -- Department Details
        d.department_name,
        d.department_reference_code,
        
        -- Hierarchy Fields
        d.parent_department_id,
        dh.parent_department_name,
        dh.department_level,
        
        -- Type 2 SCD Fields
        d.created_at AS valid_from,
        COALESCE(d.updated_at, CURRENT_TIMESTAMP()) AS valid_to,
        TRUE AS is_current, -- Can be modified based on business rules
        
        -- Audit Fields
        d.created_at,
        d.updated_at,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM departments d
    LEFT JOIN department_hierarchy dh 
        ON d.department_id = dh.department_id
)

SELECT * FROM final