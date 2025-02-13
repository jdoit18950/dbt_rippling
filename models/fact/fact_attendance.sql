{{ config(materialized='table') }}

WITH time_summary AS (
    SELECT * FROM {{ ref('int_human_resources__time_card_summary') }}
)

SELECT
    worker_id,
    total_shifts,
    total_hours,
    total_overtime,
    first_pay_period,
    last_pay_period

FROM time_summary