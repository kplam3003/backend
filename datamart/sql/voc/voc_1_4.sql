-- CHART VOC 1.4
date_case_study as (
    SELECT
        case_study_id,
        max(case_study_name) case_study_name,
        dimension_config_id,
        max(dimension_config_name) dimension_config_name,
        nlp_type,
        nlp_pack,
        max(review_date) as max_date,
        min(review_date) as min_date,
        GENERATE_DATE_ARRAY(min(review_date), max(review_date)) as day
    FROM
        voc_summary_table
    WHERE
        dimension_config_name is not null 
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack
),
date_company as (
    SELECT
        case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        max(company_name) company_name,
        CASE 
            WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
            WHEN language is null AND language_code is not null THEN language_code
            ELSE language
        END as language_name,
        CASE WHEN language_code is null or language_code = '' THEN 'blank'
            ELSE language_code
        END as language_code,
        (
            SELECT
                max(max_date)
            FROM
                date_case_study
            WHERE
                case_study_id = a.case_study_id
                AND dimension_config_id = a.dimension_config_id
                AND nlp_pack = a.nlp_pack
                AND nlp_type = a.nlp_type
        ) as max_date,
        (
            SELECT
                min(min_date)
            FROM
                date_case_study
            WHERE
                case_study_id = a.case_study_id
                AND dimension_config_id = a.dimension_config_id
                AND nlp_pack = a.nlp_pack
                AND nlp_type = a.nlp_type
        ) as min_date
    FROM
        voc_summary_table a
    WHERE
        dimension_config_name is not null 
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        language_name,
        language_code
),
date_info as (
    SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        company_name,
        language_name,
        language_code,
        min_date,
        max_date,
        GENERATE_DATE_ARRAY(min_date, max_date) as day
    FROM
        date_company
),
date_range as (
    SELECT
        *
    EXCEPT (day), day
    FROM date_info,
        UNNEST(day) AS day
),
data as(
    SELECT
        a.case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        max(company_name) company_name,
        company_id,
        CASE 
            WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
            WHEN language is null AND language_code is not null THEN language_code
            ELSE language
        END as language_name,
        CASE WHEN language_code is null or language_code = '' THEN 'blank'
            ELSE language_code
        END as language_code,
        review_date,
        count(distinct parent_review_id) AS records,
        count(distinct parent_review_id) as collected_review_count
    FROM
        voc_summary_table a
    WHERE
        dimension_config_name is not null 
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        language_name,
        language_code,
        review_date
),
final as (
    SELECT
        d.case_study_id,
        d.case_study_name,
        d.company_name,
        d.company_id,
        d.dimension_config_name,
        d.dimension_config_id,
        d.nlp_type,
        d.nlp_pack,
        d.language_name,
        d.language_code,
        d.day as daily_date,
        dt.records,
        dt.collected_review_count
    FROM
        date_range as d
        LEFT JOIN data dt 
            ON d.case_study_id = dt.case_study_id
            AND d.dimension_config_id = dt.dimension_config_id
            AND d.nlp_pack = dt.nlp_pack
            AND d.nlp_type = dt.nlp_type
            AND d.company_id = dt.company_id
            AND d.language_code = dt.language_code
            AND d.day = dt.review_date
),
voc_1_4 AS (
    SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        -- source_name,
        company_name,
        company_id,
        -- source_id,
        language_name,
        language_code,
        daily_date,
        records,
        collected_review_count AS records_daily,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 6 PRECEDING
                    AND CURRENT ROW
            ) < 7 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 6 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA7,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 13 PRECEDING
                    AND CURRENT ROW
            ) < 14 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 13 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA14,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 29 PRECEDING
                    AND CURRENT ROW
            ) < 30 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 29 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA30,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 59 PRECEDING
                    AND CURRENT ROW
            ) < 60 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 59 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA60,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 89 PRECEDING
                    AND CURRENT ROW
            ) < 90 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 89 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA90,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 119 PRECEDING
                    AND CURRENT ROW
            ) < 120 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 119 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA120,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 149 PRECEDING
                    AND CURRENT ROW
            ) < 150 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 149 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA150,
        CASE
            WHEN COUNT(daily_date) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 179 PRECEDING
                    AND CURRENT ROW
            ) < 180 THEN NULL
            ELSE SUM(collected_review_count) OVER (
                PARTITION BY case_study_id,
                dimension_config_id,
                nlp_type,
                nlp_pack,
                company_id,
                language_name,
                language_code
                ORDER BY
                    daily_date ROWS BETWEEN 179 PRECEDING
                    AND CURRENT ROW
            )
        END AS CR_MA180
    FROM
        final
),
