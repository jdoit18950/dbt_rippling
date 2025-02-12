with source as (

    select * from {{ source('rippling', 'time_entries') }}

),

renamed as (

    select
        -- Primary and foreign keys
        id as time_entry_id,
        worker_id,
        time_card_id,
        
        -- Time entry details
        start_time,
        end_time,
        status,
        
        -- Time card details if expanded
        time_card:summary:total_hours::number as total_hours,
        time_card:summary:regular_hours::number as regular_hours,
        time_card:summary:overtime_hours::number as overtime_hours,
        time_card:summary:total_holiday_hours::number as holiday_hours,
        time_card:summary:total_paid_time_off_hours::number as paid_time_off_hours,
        time_card:summary:total_unpaid_time_off_hours::number as unpaid_time_off_hours,
        
        -- Pay period details
        pay_period:start_date::date as pay_period_start_date,
        pay_period:end_date::date as pay_period_end_date,
        pay_period:pay_schedule_id::string as pay_schedule_id,
        
        -- Time entry summary
        time_entry_summary:duration::number as duration_hours,
        time_entry_summary:regular_hours::number as entry_regular_hours,
        time_entry_summary:over_time_hours::number as entry_overtime_hours,
        time_entry_summary:double_over_time_hours::number as entry_double_overtime_hours,
        
        -- Extract break information
        breaks,  -- This will be flattened in a separate model if needed
        
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
        -- Calculate total duration in hours from start/end time
        datediff('hour', start_time, end_time) as calculated_hours,
        
        -- Parse date components
        date(start_time) as entry_date,
        dayname(entry_date) as entry_day_of_week,
        
        -- Flag for current pay period
        case 
            when current_date between pay_period_start_date and pay_period_end_date 
            then true 
            else false 
        end as is_current_pay_period
        
    from renamed

)

select * from final