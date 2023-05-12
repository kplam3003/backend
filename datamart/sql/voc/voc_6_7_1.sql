-- CHART VOC 6.7.1
run_id_t AS (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.case_study_run_id`
),
voc_6_7_1 AS (
    SELECT * except(run_id) 
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET.VOC_6_7_1_commonkeywords_table`
    WHERE
        run_id in (
            SELECT run_id
            FROM run_id_t
            WHERE rank = 1
        )
),