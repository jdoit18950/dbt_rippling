{{ config(
    materialized = 'incremental',
    unique_key = ['worker_id', 'effective_date'],
    tags = ['intermediate', 'daily', 'compensation']
) }}

with worker_base as (
    select *
    from {{ ref('base_rippling_workers') }}
    where is_current_employee = true
),

compensation_data as (
    select *
    from {{ ref('base_rippling_compensations') }}
),

compensation_changes as (
    select 
        worker_id,
        compensation_id,
        calculated_annual_salary,
        created_at as effective_date,
        payment_type,
        
        -- Calculate compensation change percentage
        lag(calculated_annual_salary) over (
            partition by worker_id 
            order by created_at
        ) as previous_salary,
        
        case
            when previous_salary is not null and previous_salary != 0
            then round(((calculated_annual_salary - previous_salary) / previous_salary) * 100, 2)
            else 0
        end as salary_change_percentage,
        
        -- Track compensation review periods
        datediff('month', lag(created_at) over (
            partition by worker_id 
            order by created_at
        ), created_at) as months_since_last_change
        
    from compensation_data
),

annualized_compensation as (
    select
        cc.*,
        w.department_id,
        w.job_title,
        w.level_id,
        
        
        -- Calculate compensation bands
        ntile(4) over (
            partition by w.department_id 
            order by cc.calculated_annual_salary
        ) as department_comp_quartile,
        
        -- Flag for significant changes
        case
            when abs(salary_change_percentage) > 10 then true
            else false
        end as is_significant_change,
        
        -- Compensation timing flags
        case
            when months_since_last_change >= 12 then true
            else false
        end as is_due_for_review
        
    from compensation_changes cc
    left join worker_base w 
        on cc.worker_id = w.worker_id
    left join employment_types et 
        on w.employment_type_id = et.id
),

final as (
    select 
        worker_id,
        department_id,
        job_title,
        level_id,
        compensation_id,
        effective_date,
        payment_type,
        compensation_time_period,
        calculated_annual_salary,
        previous_salary,
        salary_change_percentage,
        months_since_last_change,
        department_comp_quartile,
        is_significant_change,
        is_due_for_review,
        
        -- Add period tracking for analysis
        date_trunc('month', effective_date) as effective_month,
        date_trunc('quarter', effective_date) as effective_quarter,
        date_trunc('year', effective_date) as effective_year,
        
        -- Add audit fields
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_batch_id
        
    from annualized_compensation
)

select * from final

{% if is_incremental() %}
    where effective_date > (select max(effective_date) from {{ this }})
{% endif %}