with 

source as (

    select * from {{ source('human_resources', 'leave_requests') }}

),

renamed as (

select
        -- Primary Identifiers
        id AS leave_request_id,
        worker_id,

        -- Leave Details
        leave_type:id::STRING AS leave_type_id,
        leave_type:name::STRING AS leave_type_name,
        status::STRING AS leave_status,
        start_date::TIMESTAMP AS leave_start_datetime,
        end_date::TIMESTAMP AS leave_end_datetime,
        reason_for_leave::STRING AS leave_reason,

        -- Requester Information (Employee Who Requested Leave)
        requester:display_name::STRING AS requester_name,
        requester:work_email::STRING AS requester_email,

        -- Reviewer Information (Approver)
        reviewer:display_name::STRING AS reviewer_name,
        reviewer:work_email::STRING AS reviewer_email,
        reviewed_at::TIMESTAMP AS reviewed_at,

        -- Additional Leave Info
        number_of_minutes_requested::INT AS total_minutes_requested,
        is_managed_by_external_system::BOOLEAN AS managed_by_external_system,

    from source

)

select * from renamed
