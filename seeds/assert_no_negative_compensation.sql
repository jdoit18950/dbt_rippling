SELECT *
FROM {{ ref('fact_compensation') }}
WHERE annual_salary < 0
   OR hourly_wage < 0
   OR monthly_salary < 0
   OR weekly_salary < 0;
