with 

source as (

    select * from {{ source('human_resources', 'departments') }}

),

renamed as (

    select
        -- Primary Identifiers
        id AS department_id,
        name::STRING AS department_name,
        
        -- Parent Department Info (since `parent` is NULL, use `parent_id`)
        parent_id::STRING AS parent_department_id,

        -- Reference Code
        reference_code::STRING AS department_reference_code,

        -- Timestamps
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at
    from source

)

select * from renamed
