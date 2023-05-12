-- VOE 9.1
voe_9_1 AS (
    SELECT
        s.case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        max(source_name) source_name,
        max(company_name) company_name,
        company_id, 
        source_id,
        review_date as daily_date,
        count(distinct parent_review_id) as records,
        count(distinct parent_review_id) as collected_review_count
    FROM voe_summary_table s
    WHERE dimension_config_name is not null
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id, 
        source_id,
        daily_date
),
