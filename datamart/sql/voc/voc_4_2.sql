-- CHART VOC 4.2
date_case_study AS (
    SELECT 
        case_study_id,
        max(case_study_name) case_study_name,
        dimension_config_id,
        max(dimension_config_name) dimension_config_name,
        nlp_type,
        nlp_pack,
        max(review_date) AS max_date,
        min(review_date) AS min_date,
        GENERATE_DATE_ARRAY(min(review_date), max(review_date)) AS DAY
    FROM voc_summary_table
    WHERE dimension_config_name IS NOT NULL
    GROUP BY 
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack
),
date_company AS (
    SELECT 
        case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        max(company_name) company_name,
        (SELECT max(max_date)
        FROM date_case_study
        WHERE case_study_id = a.case_study_id
            AND dimension_config_id = a.dimension_config_id
            AND nlp_pack = a.nlp_pack
            AND nlp_type = a.nlp_type ) AS max_date,
        (SELECT min(min_date)
        FROM date_case_study
        WHERE case_study_id = a.case_study_id
            AND dimension_config_id = a.dimension_config_id
            AND nlp_pack = a.nlp_pack
            AND nlp_type = a.nlp_type ) AS min_date
    FROM voc_summary_table a
    WHERE dimension_config_name IS NOT NULL
    GROUP BY 
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id
),
date_info AS (
    SELECT 
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        company_name,
        min_date,
        max_date,
        GENERATE_DATE_ARRAY(min_date, max_date) AS DAY
    FROM date_company
),
date_range AS (
    SELECT 
        * EXCEPT(DAY),
        DAY
    FROM 
        date_info,
        UNNEST(DAY) AS DAY
),
data_cte AS (
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
        COUNT(DISTINCT review_id) AS records,
        COUNT(
            CASE
                WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN review_id
                ELSE null
            END) AS collected_review_count,
		SUM(
			CASE
				WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN modified_polarity
                ELSE null
			END) AS sum_modified_polarity
    FROM voc_summary_table a
    WHERE dimension_config_name IS NOT NULL
    GROUP BY 
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        review_date
), 
final_cte AS (
    SELECT 
        d.case_study_id,
        d.case_study_name,
        d.dimension_config_name,
        d.dimension_config_id,
        d.nlp_type,
        d.nlp_pack,
        d.company_id,
        d.company_name,
        d.day AS daily_date,
        dt.records,
        dt.collected_review_count,
        dt.sum_modified_polarity
    FROM date_range AS d
    LEFT JOIN data_cte dt ON d.case_study_id=dt.case_study_id
        AND d.dimension_config_id = dt.dimension_config_id
        AND d.nlp_pack = dt.nlp_pack
        AND d.nlp_type = dt.nlp_type
        AND d.company_id = dt.company_id
        AND d.day = dt.review_date
), 
voc_4_2 AS (
    SELECT 
        case_study_id, 
        case_study_name, 
        dimension_config_name, 
        dimension_config_id, 
        nlp_type, 
        nlp_pack, 
        company_name, 
        company_id, 
        daily_date, 
        records, 
        collected_review_count AS records_daily, 
        sum_modified_polarity AS ss_daily, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 6 PRECEDING 
                AND CURRENT ROW
        ) < 7 THEN NULL ELSE SUM(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 6 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA7, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 6 PRECEDING 
                AND CURRENT ROW
        ) < 7 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 6 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA7, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 6 PRECEDING 
                AND CURRENT ROW
        ) < 7 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 6 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA7, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 13 PRECEDING 
                AND CURRENT ROW
        ) < 14 THEN NULL ELSE sum(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 13 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA14, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 13 PRECEDING 
                AND CURRENT ROW
        ) < 14 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 13 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA14, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 13 PRECEDING 
                AND CURRENT ROW
        ) < 14 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 13 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA14, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 29 PRECEDING 
                AND CURRENT ROW
        ) < 30 THEN NULL ELSE sum(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 29 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA30, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 29 PRECEDING 
                AND CURRENT ROW
        ) < 30 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 29 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA30, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 29 PRECEDING 
                AND CURRENT ROW
        ) < 30 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 29 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA30, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 59 PRECEDING 
                AND CURRENT ROW
        ) < 60 THEN NULL ELSE sum(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 59 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA60, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 59 PRECEDING 
                AND CURRENT ROW
        ) < 60 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 59 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA60, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 59 PRECEDING 
                AND CURRENT ROW
        ) < 60 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 59 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA60, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 89 PRECEDING 
                AND CURRENT ROW
        ) < 90 THEN NULL ELSE sum(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 89 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA90, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 89 PRECEDING 
                AND CURRENT ROW
        ) < 90 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 89 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA90, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 89 PRECEDING 
                AND CURRENT ROW
        ) < 90 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 89 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA90, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 119 PRECEDING 
                AND CURRENT ROW
        ) < 120 THEN NULL ELSE sum(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 119 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA120, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 119 PRECEDING 
                AND CURRENT ROW
        ) < 120 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 119 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA120, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 119 PRECEDING 
                AND CURRENT ROW
        ) < 120 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 119 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA120, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 149 PRECEDING 
                AND CURRENT ROW
        ) < 150 THEN NULL ELSE sum(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 149 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA150, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 149 PRECEDING 
                AND CURRENT ROW
        ) < 150 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 149 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA150, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 149 PRECEDING 
                AND CURRENT ROW
        ) < 150 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 149 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA150, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 179 PRECEDING 
                AND CURRENT ROW
        ) < 180 THEN NULL ELSE sum(sum_modified_polarity) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 179 PRECEDING 
                AND CURRENT ROW
        ) END AS SS_MA180, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 179 PRECEDING 
                AND CURRENT ROW
        ) < 180 THEN NULL ELSE sum(collected_review_count) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 179 PRECEDING 
                AND CURRENT ROW
        ) END AS RECORDS_MA180, 
        CASE WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 179 PRECEDING 
                AND CURRENT ROW
        ) < 180 THEN NULL ELSE AVG(records) OVER (
            PARTITION BY case_study_id, 
            dimension_config_id, 
            nlp_type, 
            nlp_pack, 
            company_id 
            ORDER BY 
                daily_date ROWS BETWEEN 179 PRECEDING 
                AND CURRENT ROW
        ) END AS CR_MA180 
    FROM 
        final_cte
),
