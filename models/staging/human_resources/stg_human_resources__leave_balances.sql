with 

source as (

    select * from {{ source('human_resources', 'leave_balances') }}

),

renamed as (

    select
        -- Primary Identifiers
        id AS leave_balance_id,
        worker_id,

        -- Leave Type Details
        leave_type:id::STRING AS leave_type_id,
        leave_type:name::STRING AS leave_type_name,
        leave_type:is_paid::BOOLEAN AS is_paid_leave,

        -- Balance Details
        TRY_TO_DOUBLE(balance_excluding_future_requests) AS balance_excluding_requests,
        TRY_TO_DOUBLE(balance_including_future_requests) AS balance_including_requests,
        is_balance_unlimited::BOOLEAN AS unlimited_balance,

        -- Timestamps
        created_at::TIMESTAMP AS created_at,
        updated_at::TIMESTAMP AS updated_at

    from source

)

select * from renamed
