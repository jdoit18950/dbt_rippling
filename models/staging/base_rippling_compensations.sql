with source as (

    select * from {{ source('rippling', 'compensations') }}

),

renamed as (

    select
        -- Primary and foreign keys
        id as compensation_id,
        worker_id,
        
        -- Compensation details
        payment_type,
        bonus_schedule,
        target_annual_bonus_percent,
        
        -- Annual compensation
        annual_compensation:value::number as annual_compensation_amount,
        annual_compensation:currency_type::string as annual_compensation_currency,
        
        -- Monthly compensation
        monthly_compensation:value::number as monthly_compensation_amount,
        monthly_compensation:currency_type::string as monthly_compensation_currency,
        
        -- Weekly compensation
        weekly_compensation:value::number as weekly_compensation_amount,
        weekly_compensation:currency_type::string as weekly_compensation_currency,
        
        -- Hourly compensation
        hourly_wage:value::number as hourly_wage_amount,
        hourly_wage:currency_type::string as hourly_wage_currency,
        
        -- Bonus details
        signing_bonus:value::number as signing_bonus_amount,
        signing_bonus:currency_type::string as signing_bonus_currency,
        
        target_annual_bonus:value::number as target_annual_bonus_amount,
        target_annual_bonus:currency_type::string as target_annual_bonus_currency,
        
        -- Commission details
        on_target_commission:value::number as on_target_commission_amount,
        on_target_commission:currency_type::string as on_target_commission_currency,
        
        -- Other compensation
        relocation_reimbursement:value::number as relocation_reimbursement_amount,
        relocation_reimbursement:currency_type::string as relocation_reimbursement_currency,
        
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
            when annual_compensation_amount is not null then annual_compensation_amount
            when monthly_compensation_amount is not null then monthly_compensation_amount * 12
            when weekly_compensation_amount is not null then weekly_compensation_amount * 52
            when hourly_wage_amount is not null then hourly_wage_amount * 2080 -- Standard work year hours
            else null
        end as calculated_annual_salary,
        
        row_number() over (
            partition by worker_id 
            order by created_at desc
        ) = 1 as is_current_record
        
    from renamed

)

select * from final
    