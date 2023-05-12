-- CHART VOE 1.5
voe_1_5 AS (
    SELECT 
        MAX(s.case_study_id) AS case_study_id, 
        MAX(case_study_name) AS case_study_name, 
        MAX(dimension_config_name) dimension_config_name, 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        -- max(source_name)source_name,
        MAX(company_name) company_name, 
        company_id, 
        -- source_id,
        review_date AS daily_date, 
        CASE 
            WHEN LANGUAGE IS NULL 
                AND (
                    language_code IS NULL 
                    OR language_code = ''
                ) THEN 'blank' 
            WHEN LANGUAGE IS NULL 
                AND language_code IS NOT NULL THEN language_code 
            ELSE LANGUAGE 
        END AS language_name,
        CASE 
            WHEN language_code IS NULL 
                OR language_code = '' THEN 'blank' 
            ELSE language_code 
        END AS language_code, 
        count(DISTINCT parent_review_id) AS records, 
        count(DISTINCT parent_review_id) AS collected_review_count 
    FROM 
        voe_summary_table s
    WHERE 
        dimension_config_name IS NOT NULL 
    GROUP BY 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        company_id, 
        -- source_id,
        language_name, 
        language_code, 
        daily_date
),