-- CHART VOE 10.1
voe_10_1 AS (
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
        modified_dimension as dimension,
        review_date as daily_date,

        SUM(CASE WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN modified_polarity ELSE null END) as sum_ss,
        COUNT(CASE WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN review_id  ELSE null END) as sum_review_count

    FROM voe_summary_table 
    WHERE
        dimension_config_name is not null
        AND dimension is not null
        AND is_used = true

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
