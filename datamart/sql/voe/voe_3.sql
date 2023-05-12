-- CHART VOE 3
run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM
        `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.voe_case_study_run_id`
    WHERE case_study_id = @case_study_id
),
voe_3 AS (
    SELECT 
        * EXCEPT(run_id) 
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET.VOE_3_ratingtime_table`
    WHERE
        run_id in (
            SELECT
                run_id
            FROM
                run_id_t
            WHERE
                rank = 1
        )
        AND case_study_id = @case_study_id
),