WITH RECURSIVE department_hierarchy AS (
    -- Base case: top-level departments
    SELECT
        department_id,
        parent_department_id,
        department_name,
        department_code,
        1 as hierarchy_level,
        department_name as department_path,
        ARRAY_CONSTRUCT(department_id) as department_id_path
    FROM {{ ref('base_rippling_departments') }}
    WHERE parent_department_id IS NULL

    UNION ALL

    -- Recursive case: child departments
    SELECT
        d.department_id,
        d.parent_department_id,
        d.department_name,
        d.department_code,
        h.hierarchy_level + 1,
        h.department_path || ' > ' || d.department_name,
        ARRAY_APPEND(h.department_id_path, d.department_id)
    FROM {{ ref('base_rippling_departments') }} d
    INNER JOIN department_hierarchy h 
        ON d.parent_department_id = h.department_id
),

department_metrics AS (
    SELECT
        d.*,
        w.worker_count,
        wc.total_compensation
    FROM department_hierarchy d
    LEFT JOIN (
        SELECT 
            department_id,
            COUNT(DISTINCT worker_id) as worker_count
        FROM {{ ref('base_rippling_workers') }}
        WHERE is_current_employee = true
        GROUP BY department_id
    ) w ON d.department_id = w.department_id
    LEFT JOIN (
        SELECT 
            department_id,
            SUM(calculated_annual_salary) as total_compensation
        FROM {{ ref('int_rippling__worker_compensation') }}
        GROUP BY department_id
    ) wc ON d.department_id = wc.department_id
),

final AS (
    SELECT
        department_id,
        parent_department_id,
        department_name,
        department_code,
        hierarchy_level,
        department_path,
        department_id_path,
        worker_count,
        total_compensation,
        -- Calculate department size metrics
        SUM(worker_count) OVER (
            PARTITION BY CASE WHEN hierarchy_level = 1 THEN department_id END
        ) as total_org_headcount,
        worker_count::FLOAT / NULLIF(total_org_headcount, 0) as dept_to_org_headcount_ratio,
        -- Calculate compensation metrics
        total_compensation / NULLIF(worker_count, 0) as avg_compensation_per_employee,
        CURRENT_TIMESTAMP() as dbt_loaded_at
    FROM department_metrics
)

SELECT * FROM final 