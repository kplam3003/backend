-- CHART VOC 6.5
run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc
        ) as rank
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.case_study_run_id`
    WHERE case_study_id = @case_study_id
),
voc_6_5 AS (
    SELECT * EXCEPT(run_id) 
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET.VOC_6_5_competitor_table`
    WHERE
        case_study_id = @case_study_id
        AND run_id in (
            SELECT
                run_id
            FROM
                run_id_t
            WHERE
                rank = 1
        )
),