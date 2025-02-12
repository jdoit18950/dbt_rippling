WITH headcount_metrics AS (
    SELECT
        department_id,
        department_name,
        department_path,
        month_date as metric_date,
        total_headcount,
        new_hires,
        terminations,
        active_employees,
        month_over_month_growth
    FROM {{ ref('hr_headcount_analysis') }}
),

compensation_metrics AS (
    SELECT
        department_id,
        department_name,
        effective_date as metric_date,
        calculated_annual_salary,
        dept_avg_salary,
        company_avg_salary,
        salary_to_dept_avg_ratio
    FROM {{ ref('hr_compensation_analysis') }}
),

turnover_metrics AS (
    SELECT
        department_id,
        department_name,
        month_date as metric_date,
        turnover_rate,
        voluntary_terminations,
        involuntary_terminations,
        avg_tenure_months
    FROM {{ ref('hr_turnover_analysis') }}
),

cost_metrics AS (
    SELECT
        department_id,
        department_name,
        month_date as metric_date,
        monthly_salary_cost,
        overtime_cost,
        estimated_benefits_cost,
        total_monthly_cost,
        month_over_month_cost_change
    FROM {{ ref('hr_department_cost_analysis') }}
),

combined_metrics AS (
    SELECT
        COALESCE(h.department_id, c.department_id, t.department_id, co.department_id) as department_id,
        COALESCE(h.department_name, c.department_name, t.department_name, co.department_name) as department_name,
        COALESCE(h.metric_date, c.metric_date, t.metric_date, co.metric_date) as metric_date,
        -- Headcount metrics
        h.total_headcount,
        h.new_hires,
        h.terminations,
        h.active_employees,
        h.month_over_month_growth as headcount_growth,
        -- Compensation metrics
        c.calculated_annual_salary,
        c.dept_avg_salary,
        c.company_avg_salary,
        c.salary_to_dept_avg_ratio,
        -- Turnover metrics
        t.turnover_rate,
        t.voluntary_terminations,
        t.involuntary_terminations,
        t.avg_tenure_months,
        -- Cost metrics
        co.monthly_salary_cost,
        co.overtime_cost,
        co.estimated_benefits_cost,
        co.total_monthly_cost,
        co.month_over_month_cost_change
    FROM headcount_metrics h
    FULL OUTER JOIN compensation_metrics c 
        ON h.department_id = c.department_id 
        AND h.metric_date = c.metric_date
    FULL OUTER JOIN turnover_metrics t 
        ON h.department_id = t.department_id 
        AND h.metric_date = t.metric_date
    FULL OUTER JOIN cost_metrics co 
        ON h.department_id = co.department_id 
        AND h.metric_date = co.metric_date
)

SELECT 
    *,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM combined_metrics 