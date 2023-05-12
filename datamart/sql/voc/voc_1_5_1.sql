--- VOC_1_5_1_cusrevcountry_p_v
voc_1_5_1 AS(
SELECT
    max(a.case_study_id) AS case_study_id,
    max(case_study_name) AS case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(company_name) company_name,
    company_id,
    review_date as daily_date,
    CASE
        WHEN review_country is null OR review_country = 'Unknown' THEN 'blank'
        ELSE review_country
    END AS country_name,
    CASE
        WHEN country_code is null OR country_code = 'Unknown' THEN 'blank'
        ELSE country_code
    END AS country_code,
    count(distinct parent_review_id) as records,
    count(distinct parent_review_id) as collected_review_count
FROM
    voc_summary_table a
WHERE
    dimension_config_name is not null
GROUP BY
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    daily_date,
    country_name,
    country_code
),