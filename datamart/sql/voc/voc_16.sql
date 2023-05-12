voc_16 AS (
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
    CASE WHEN polarity in ('N','N+') then 'Negative'
    WHEN polarity in ('P','P+') then 'Positive'
    ELSE null END polarity_type,
    review_date as daily_date,
    count(distinct review_id) as records,
    count(distinct CASE WHEN polarity in ('N','N+','P','P+') THEN review_id ELSE null END) as collected_review_count

FROM voc_summary_table 
WHERE
dimension_config_name is not null
AND is_used = true
AND dimension_type ='KPC'

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