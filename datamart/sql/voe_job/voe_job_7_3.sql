-- VOE JOB 7.3
distinct_job_id AS (
    SELECT
        company_id,
        job_id,
        MIN(DATE(posted_date)) as posted_date
    FROM voe_job_summary_table
    GROUP BY company_id, job_id

),
voe_job_7_3 AS (
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
            WHEN job_function is NULL or job_function = '' THEN 'undefined'
            ELSE job_function 
        END AS job_function,
        COUNT(DISTINCT d.job_id) as job_quantity,
        MAX(fte) as fte
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
        job_function
),