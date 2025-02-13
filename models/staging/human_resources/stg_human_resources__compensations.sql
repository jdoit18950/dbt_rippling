with 

source as (

    select * from {{ source('human_resources', 'compensations') }}

),

renamed as (
select
        -- Identifiers
        id AS compensation_id,
        worker_id,

        -- Parsing JSON Fields with Safe Type Conversion
        annual_compensation:value AS annual_salary,
        hourly_wage:value AS hourly_wage,
        monthly_compensation:value AS monthly_salary,
        weekly_compensation:value AS weekly_salary,
        target_annual_bonus:value AS target_bonus,
        signing_bonus:value AS signing_bonus,
        relocation_reimbursement:value AS relocation_reimbursement,
        on_target_commission:value AS on_target_commission,
        annual_salary_equivalent:value AS annual_salary_equiv,
        target_annual_bonus_percent:value AS target_bonus_pct,

        -- Other Compensation Details
        payment_type::STRING AS payment_type,
        bonus_schedule::STRING AS bonus_schedule,

        -- Timestamps
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at

    from source

)

select * from renamed
