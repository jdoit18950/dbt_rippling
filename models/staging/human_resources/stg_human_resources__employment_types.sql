with 

source as (

    select * from {{ source('human_resources', 'employment_types') }}

),

renamed as (

    select
        id as employment_type_id,
        name,
        type,
        label,
        created_at,
        updated_at,
        amount_worked,
        compensation_time_period

    from source

)

select * from renamed
