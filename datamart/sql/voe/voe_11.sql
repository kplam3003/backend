-- VOE 11
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
),
voe_11 AS (
    SELECT * except(run_id) 
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET.VOE_11_termcount_table`
    WHERE
        run_id in (
            SELECT
                run_id
            FROM
                run_id_t
            WHERE
                rank = 1
        )
),