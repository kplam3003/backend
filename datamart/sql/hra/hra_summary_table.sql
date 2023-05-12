WITH company_summary_table as (
    SELECT *
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_DATAMART_CS.hra_summary_table_company_*`
    WHERE _TABLE_SUFFIX = CAST(@case_study_id AS STRING)
),
employee_summary_table as (
    SELECT *
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_DATAMART_CS.hra_summary_table_employees_*`
    WHERE _TABLE_SUFFIX = CAST(@case_study_id AS STRING)
),
experience_summary_table as (
    SELECT *
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_DATAMART_CS.hra_summary_table_experience_*`
    WHERE _TABLE_SUFFIX = CAST(@case_study_id AS STRING)
),
education_summary_table as (
    SELECT *
    FROM `GCP_DATA_PLATFORM_PROJECT_ID.BQ_DATASET_DATAMART_CS.hra_summary_table_education_degree_*`
    WHERE _TABLE_SUFFIX = CAST(@case_study_id AS STRING)
),