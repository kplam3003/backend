-- CHART VOE 6.2
voe_6_2 AS (
    SELECT
        MAX(case_study_id) AS case_study_id,
        MAX(case_study_name) AS case_study_name,
        MAX(dimension_config_name) AS dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        MAX(source_name) AS source_name,
        MAX(company_name) AS company_name,
        company_id, 
        source_id,
        dimension_type,
        modified_dimension AS dimension,
        review_date AS daily_date,
        COUNT(DISTINCT review_id) AS records,
        COUNT(DISTINCT review_id) AS collected_review_count
    FROM voe_summary_table
    WHERE
        dimension_config_name IS NOT NULL
        AND dimension IS NOT NULL
        AND is_used = true
    GROUP BY 
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id, 
        source_id,
        dimension_type,
        modified_dimension,
        daily_date
),