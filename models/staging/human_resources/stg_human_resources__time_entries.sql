with 

source as (

    select * from {{ source('human_resources', 'time_entries') }}

),

renamed as (

    select
        -- Primary Identifiers
        id AS time_entry_id,
        worker_id,
        start_time::TIMESTAMP AS start_time,
        end_time::TIMESTAMP AS end_time,
        status::STRING AS entry_status,

        -- Expanded Time Card Fields
        time_card:id::STRING AS time_card_id,
        time_card:pay_period.start_date::DATE AS pay_period_start,
        time_card:pay_period.end_date::DATE AS pay_period_end,

        -- Summary Fields from Time Card (Proper JSON Extraction)
        TO_DOUBLE(time_card:summary.total_hours) AS total_hours_worked,
        TO_DOUBLE(time_card:summary.overtime_hours) AS overtime_hours,
        TO_DOUBLE(time_card:summary.regular_hours) AS regular_hours,
        TO_DOUBLE(time_card:summary.paid_hours) AS paid_hours,
        TO_DOUBLE(time_card:summary.total_paid_time_off_hours) AS total_paid_time_off_hours,
        TO_DOUBLE(time_card:summary.total_unpaid_time_off_hours) AS total_unpaid_time_off_hours

    from source

)

select * from renamed
