with 

source as (

    select * from {{ source('human_resources', 'job_codes') }}

),

renamed as (

    select
        -- Primary Identifiers
        id AS job_code_id,
        name::STRING AS job_code_name,
        group_id::STRING AS job_group_id,

        -- Expanded Job Dimension Fields (Proper JSON Extraction)
        job_dimension:id::STRING AS job_dimension_id,
        job_dimension:name::STRING AS job_dimension_name,
        job_dimension:external_id::STRING AS job_dimension_external_id,
        job_dimension:roster_type::STRING AS job_roster_type,
        job_dimension:includes_custom_location::BOOLEAN AS job_custom_location,
        job_dimension:group_id::STRING AS job_dimension_group_id,

        -- Timestamps
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at

    from source

)

select * from renamed
