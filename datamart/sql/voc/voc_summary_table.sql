WITH voc_summary_table as (
    SELECT *
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_DATAMART_CS.summary_table_*`
    WHERE _TABLE_SUFFIX = CAST(@case_study_id AS STRING)
),