voc_12_2 AS (
SELECT
        case_study_id,
        MAX(case_study_name) case_study_name,
        MAX(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        MAX(company_name) company_name,
        review_date,
        polarity,
        COUNT ( DISTINCT review_id) as review_count
    FROM
        voc_summary_table
    WHERE
        dimension_config_name is not null 
        AND dimension is not NULL
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        review_date,
        polarity
),