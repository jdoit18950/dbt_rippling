with 

source as (

    select * from {{ source('human_resources', 'teams') }}

),

renamed as (

    select
        -- Primary Identifiers
        id AS team_id,
        name::STRING AS team_name,

        -- Timestamps
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at

    from source

)

select * from renamed
