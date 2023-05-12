import datetime

from google.cloud import bigquery
from . import (
    CASE_STUDY_ID_FILTER_KEY,
    START_DATE_FILTER_KEY,
    END_DATE_FILTER_KEY,
    COMPANY_NAME_FILTER_KEY,
    SOURCE_NAME_FILTER_KEY,
)
from . import client
from . import DEFAULT_QUERY_TIMEOUT
import config
import logger
import json

from helper import construct_final_query

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

PAGE = 1
PAGE_SIZE = 50
HRA_CHART_DATA_MART_TABLE_MAPPING = config.CHART_DATA_MART_TABLE_MAPPING["HRA"]
HRA_CHART_DATA_MART_SCRIPT_MAPPING = config.CHART_DATA_MART_SCRIPT_MAPPING["HRA"]


def fetch_hra_1(payload):
    """
    Fetch Reviews statistics data
    """
    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY,
            "INTEGER",
            int(payload["params"][CASE_STUDY_ID_FILTER_KEY]),
        ),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]
        ),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY]
        ),
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY,
                "STRING",
                payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(","),
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload["params"]:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                SOURCE_NAME_FILTER_KEY,
                "STRING",
                payload["params"][SOURCE_NAME_FILTER_KEY].lower().split(","),
            )
        )

    filter_sql = f"""
        SELECT 
            company_name,
            COUNT(DISTINCT coresignal_employee_id) AS employee_collected,
            COUNT(DISTINCT 
                CASE 
                    WHEN experience_date_from IS NOT NULL AND title IS NOT NULL THEN experience_id
                    ELSE NULL
                END
            ) AS experience_processed,
            COUNT(DISTINCT education_id) AS education_processed
        FROM {HRA_CHART_DATA_MART_SCRIPT_MAPPING["HRA_1"]}
        WHERE case_study_id = @case_study_id
            AND (
                experience_date_from >= @start_date
                OR experience_date_from IS NULL
            )
            AND (
                experience_date_to <= @end_date
                OR experience_date_to IS NULL
            )
            {company_filter_str}
            {source_filter_str}
        GROUP BY company_name
        ORDER BY employee_collected DESC
    """

    final_sql = construct_final_query(
        nlp_type="HRA",
        chart_code=HRA_CHART_DATA_MART_SCRIPT_MAPPING["HRA_1"],
        filter_query=filter_sql,
        logger=logger,
    )

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    results = []
    total_records = 0
    for row in query_job:
        total_records += row.employee_collected
        results.append(
            {
                "company_name": row.company_name,
                "employee_collected": row.employee_collected,
                "experience_processed": row.experience_processed,
                "education_processed": row.education_processed,
            }
        )

    _ = [d.update({"records": total_records}) for d in results]
    return results
