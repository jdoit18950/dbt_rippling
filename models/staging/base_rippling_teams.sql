with source as (

    select * from {{ source('rippling', 'teams') }}

),

renamed as (

    select
        -- Primary and foreign keys
        id as team_id,
        
        -- Team details
        name as team_name,
        
        -- Timestamps
        created_at,
        updated_at,
        
        -- Add loaded timestamp
        current_timestamp() as dbt_loaded_at

    from source

)

select * from renamed
    