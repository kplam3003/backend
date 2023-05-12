-- CHART VOE 6.3
voe_6_3 AS (
    SELECT 
        case_study_id, 
        max(case_study_name) case_study_name, 
        max(dimension_config_name) dimension_config_name, 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        max(source_name) source_name, 
        max(company_name) company_name, 
        company_id, 
        source_id, 
        dimension_type, 
        modified_dimension AS dimension, 
        polarity AS polarity_type, 
        review_date AS daily_date, 
        count(DISTINCT review_id) AS records, 
        count(DISTINCT review_id ) AS collected_review_count 
    FROM 
        voe_summary_table 
    WHERE 
        dimension_config_name IS NOT NULL 
        AND is_used = TRUE 
		AND polarity in ('N','N+','NEU','P','P+') 
    GROUP BY 
        case_study_id, 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        company_id, 
        source_id, 
        dimension_type, 
        modified_dimension, 
        polarity_type, 
        daily_date
),
