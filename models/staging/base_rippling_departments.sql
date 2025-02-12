with source as (

    select * from {{ source('rippling', 'departments') }}

),

renamed as (

    select
        -- Primary and foreign keys
        id as department_id,
        parent_id as parent_department_id,
        
        -- Department details
        name as department_name,
        reference_code as department_code,
        
        -- Parent department details (if expanded in API)
        parent:name::string as parent_department_name,
        parent:reference_code::string as parent_department_code,
        
        -- Timestamps
        created_at,
        updated_at,
        
        -- Add loaded timestamp
        current_timestamp() as dbt_loaded_at

    from source
),

final as (

    select
        *,
        case 
            when parent_department_id is null then true 
            else false 
        end as is_top_level_department,
        
        -- Create department path for hierarchy
        case
            when parent_department_name is not null 
            then parent_department_name || ' > ' || department_name
            else department_name
        end as department_path
        
    from renamed

)

select * from final
    