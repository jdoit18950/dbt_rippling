with

    source as (select * from {{ source("human_resources", "time_entries") }}),

    renamed as (

        select
            -- Primary Identifiers
            id as time_entry_id,
            worker_id,
            start_time::timestamp as start_time,
            end_time::timestamp as end_time,
            status::string as entry_status,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,

            -- Expanded Time Card Fields
            time_card:id::string as time_card_id,
            time_card:pay_period.start_date::date as pay_period_start,
            time_card:pay_period.end_date::date as pay_period_end,

            -- Summary Fields from Time Card (Proper JSON Extraction)
            to_double(time_card:summary.total_hours) as total_hours_worked,
            to_double(time_card:summary.overtime_hours) as overtime_hours,
            to_double(time_card:summary.regular_hours) as regular_hours,
            to_double(time_card:summary.paid_hours) as paid_hours,
            to_double(
                time_card:summary.total_paid_time_off_hours
            ) as total_paid_time_off_hours,
            to_double(
                time_card:summary.total_unpaid_time_off_hours
            ) as total_unpaid_time_off_hours,
            created_at,
            updated_at

        from source

    )

select *
from renamed
