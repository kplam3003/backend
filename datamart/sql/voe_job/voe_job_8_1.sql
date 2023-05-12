-- VOE JOB 8.1
distinct_job_id AS (
    SELECT
        company_id,
        job_id,
        MIN(DATE(posted_date)) as posted_date
    FROM voe_job_summary_table
    GROUP BY company_id, job_id

),
voe_job_8_1 AS (
    SELECT 
        case_study_id, 
        case_study_name,
        nlp_pack,
        nlp_type, 
        dimension_config_id, 
        dimension_config_name,
        source_name,
        source_id,
        company_name,
        a.company_id,
        d.posted_date,
        CASE 
            WHEN role_seniority is NULL or role_seniority = '' THEN 'undefined'
            ELSE role_seniority 
        END AS role_seniority,    
        COUNT(distinct d.job_id) as job_quantity,
        max(fte) as fte
    FROM voe_job_summary_table a
    LEFT JOIN distinct_job_id d ON a.company_id = d.company_id AND a.job_id = d.job_id
    GROUP BY
        case_study_id, 
        case_study_name,
        nlp_pack,
        nlp_type, 
        dimension_config_id, 
        dimension_config_name,
        source_name,
        source_id,
        company_name,
        company_id,
        posted_date,
        role_seniority
),