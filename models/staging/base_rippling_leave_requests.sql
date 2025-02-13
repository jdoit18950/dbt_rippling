with source as (

    select * from {{ source('rippling', 'leave_requests') }}

),

renamed as (

    select
        -- Primary and foreign keys
        id as leave_request_id,
        worker_id,
        requester_id,
        reviewer_id,
        leave_policy_id,
        leave_type_id,
        
        -- Leave request details
        status as leave_status,
        start_date,
        start_time,
        end_date,
        end_time,
        comments,
        reason_for_leave,
        number_of_minutes_requested,
        
        -- Leave type details if expanded
        leave_type:name::string as leave_type_name,
        leave_type:description::string as leave_type_description,
        leave_type:is_paid::boolean as is_paid_leave,
        
        -- Review details
        reviewed_at,
        
        -- Parse days_take_off array
        days_take_off,  -- This will be flattened in a separate model if needed
        
        -- External system flag
        is_managed_by_external_system,
        
        -- Add loaded timestamp
        current_timestamp() as dbt_loaded_at

    from source
),

final as (

    select
        *,
        -- Calculate duration in days
        datediff('day', start_date, end_date) + 1 as leave_duration_days,
        
        -- Calculate duration in hours
        datediff('hour', start_time, end_time) as leave_duration_hours,
        
        -- Flag for current requests
        case 
            when current_date between start_date and end_date 
            then true 
            else false 
        end as is_current_leave,
        
        -- Flag for future requests
        case 
            when start_date > current_date 
            then true 
            else false 
        end as is_future_leave
        
    from renamed

)

select * from final
    