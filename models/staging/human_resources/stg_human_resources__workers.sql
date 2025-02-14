with 

source as (

    select * from {{ source('human_resources', 'workers') }}

),

renamed as (

    SELECT
        -- Primary Identifiers
        id AS worker_id,
        user:id::STRING AS user_id,
        user:display_name::STRING AS display_name,
        gender,
        ethnicity,
        user:work_email::STRING AS work_email,
        department:id::STRING AS department_id,
        employment_type:id::STRING AS employment_type_id,
        teams:id AS team_ids,
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at,

        -- Employment Details
        employment_type:label::STRING AS employment_type_label,
        employment_type:type::STRING AS employment_category,
        department:name::STRING AS department_name,


        -- Compensation Details
        compensation:annual_compensation.value::FLOAT AS annual_salary,
        compensation:hourly_wage.value::FLOAT AS hourly_wage,
        compensation:payment_type::STRING AS payment_type,
        compensation:target_annual_bonus.value::FLOAT AS target_bonus,

        -- Worker Status
        status::STRING AS status,
        created_at AS hire_date,
        updated_at AS last_updated,
        termination_details:reason::STRING AS termination_reason,
        termination_details:type::STRING AS termination_type,

        -- Additional Fields
        location:work_location_id::STRING AS work_location_id,
        manager_id::STRING AS manager_id,
        date_of_birth::DATE AS date_of_birth

    from source

)

select * from renamed
