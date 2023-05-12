-- CHART VOE 5
run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc
        ) as rank
    FROM
        `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.voe_case_study_run_id`
    WHERE
        case_study_id = @case_study_id
),
voe_5 AS (
    SELECT * except(run_id) 
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET.VOE_5_heatmapdim_table`
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