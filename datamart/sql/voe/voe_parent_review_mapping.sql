BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.voe_batch_status`
    WHERE
        batch_id in (
            SELECT
                batch_id
            FROM
                `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.voe_casestudy_batchid`
            WHERE
                case_study_id = @case_study_id
        )
        AND status = 'Active'
),
parent_review AS (
    SELECT DISTINCT 
        case_study_id,
        review_id,
        parent_review_id,
        technical_type
    FROM   `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_STAGING.voe_parent_review_mapping`
    WHERE
        batch_id IN (
            SELECT
                batch_id FROM BATCH_LIST
        )
        AND case_study_id = @case_study_id
),