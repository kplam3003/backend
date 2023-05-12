-- CHART VOC 6.4
voc_6_4 AS (
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
        review_date AS daily_date, 
        sum(
            CASE 
                WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN modified_polarity
                ELSE null 
            END
        ) AS sum_ss, 
        count(
            CASE
				WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN review_id
                ELSE null
            END
        ) AS sum_review_count 
    FROM 
        voc_summary_table a 
    WHERE 
        dimension_config_name IS NOT NULL 
        AND dimension IS NOT NULL 
        AND is_used = TRUE 
    GROUP BY 
        case_study_id, 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        company_id, 
        source_id, 
        dimension_type, 
        modified_dimension, 
        daily_date
),
