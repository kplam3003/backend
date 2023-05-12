-- CHART VOC 1.7
voc_1_7 AS (
    SELECT 
        MAX(s.case_study_id) AS case_study_id, 
        MAX(case_study_name) AS case_study_name, 
        MAX(dimension_config_name) dimension_config_name, 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        MAX(company_name) company_name, 
        company_id, 
        review_date AS daily_date, 
        COUNT(DISTINCT parent_review_id) AS records, 
        COUNT(DISTINCT parent_review_id) AS collected_review_count, 
        COUNT(DISTINCT 
            CASE 
                WHEN dimension IS NOT NULL THEN parent_review_id 
                ELSE NULL 
            END
        ) AS processed_review_count 
    FROM 
        voc_summary_table s
    WHERE 
        dimension_config_name IS NOT NULL 
    GROUP BY 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        company_id, 
        -- source_id,
        daily_date
),