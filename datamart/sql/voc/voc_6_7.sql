-- CHART VOC 6.7
cs_run_id AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY case_study_id
            ORDER BY
                created_at DESC
        ) AS rank
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.case_study_run_id`
    WHERE case_study_id = @case_study_id
),
voc_6_7 AS (
    SELECT * except(run_id) 
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET.VOC_6_7_commonterms_table`
    WHERE case_study_id = @case_study_id
        AND run_id in (
            SELECT run_id
            FROM cs_run_id
            WHERE rank = 1
        )
),