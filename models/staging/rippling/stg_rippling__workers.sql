with source as (

    select * from {{ source('rippling', 'workers') }}

),

renamed as (

    select
        id as worker_id,
        user_id,
        manager_id,
        legal_entity_id,
        status as worker_status,
        start_date,
        end_date,
        work_email,
        personal_email,
        title as job_title,
        department_id,
        employment_type_id,
        compensation_id,
        
        -- Parse JSON fields
        location:type::string as location_type,
        location:work_location_id::string as work_location_id,
        
        -- Handle termination details
        termination_details:type::string as termination_type,
        termination_details:reason::string as termination_reason,
        
        -- Timestamps and audit fields
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
            when worker_status = 'ACTIVE' and end_date is null then true
            else false 
        end as is_current_employee
        
    from renamed

)

select * from final