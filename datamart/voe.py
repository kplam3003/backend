from helper import construct_final_query
from google.cloud import bigquery
from . import client, MA_DAILY, SS_DAILY, RECORDS_DAILY, MA_PREFIX, SS_MA, RECORDS_MA, RAW_AVERAGE_FILTER, ER_MA, \
    RATING_DAILY, RATING_MA, ER_DAILY, END_DATE_FILTER_KEY, SOURCE_NAME_FILTER_KEY, CASE_STUDY_ID_FILTER_KEY, \
    START_DATE_FILTER_KEY, COMPANY_NAME_FILTER_KEY, DIMENSION_FILTER_KEY, DIMENSION_TYPE_FILTER_KEY, IS_DEFAULT_KEY
from . import DEFAULT_QUERY_TIMEOUT
from . import MA_TYPES
import config
import logger
import json
import datetime


logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

PAGE = 1
PAGE_SIZE = 50
VOE_CHART_DATA_MART_TABLE_MAPPING = config.CHART_DATA_MART_TABLE_MAPPING["VOE"]
VOE_CHART_DATA_MART_SCRIPT_MAPPING = config.CHART_DATA_MART_SCRIPT_MAPPING["VOE"]
VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING = config.CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB"]


def fetch_voe_7_1(payload):
    """
    Fetch Total Job listings by function - identifying hiring concentration by company data
    """
    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
        ),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]
        ),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY]
        ),
    ]

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

    job_function_filter_str = ""
    if "job_functions" in payload["params"]:
        job_function_filter_str = "AND job_function IN UNNEST(@job_functions)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "job_functions",
                "STRING",
                json.loads(payload["params"]["job_functions"]),
            )
        )

    filter_sql = f"""
        WITH company AS (
            SELECT
                case_study_id,
                company_id,
                SUM(job_quantity) AS company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_1"]}
            WHERE
                case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {job_function_filter_str}
            GROUP BY case_study_id, company_id
        ),
        voe_data AS(
            SELECT
                a.case_study_id,
                a.company_id,
                company_name,
                job_function,
                SUM(job_quantity) AS job_quantity,
                MAX(fte) AS fte,
                c.company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_1"]} a
            LEFT JOIN company c ON a.case_study_id = c.case_study_id AND a.company_id = c.company_id
            WHERE
                a.case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {job_function_filter_str}
            GROUP BY
                case_study_id,
                company_id,
                company_name,
                job_function,
                company_total
        )
        SELECT
            *,
            (
            WITH voe_job AS (
                SELECT
                    CASE
                        WHEN job_function is NULL or job_function = '' THEN 'undefined'
                        ELSE job_function
                    END AS job_function,
                    job_id
                FROM
                    {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id = @case_study_id
                AND DATE(posted_date) >= @start_date
                AND DATE(posted_date) <= @end_date
            {company_filter_str}
            {source_filter_str}
            )
            SELECT COUNT (DISTINCT job_id)
            FROM voe_job
            WHERE 1 = 1
            {job_function_filter_str}
            ) AS records
        FROM
            voe_data
    """
    final_sql = construct_final_query(nlp_type='VOE_JOB',
                                      chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    result = []
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "job_function": row.job_function,
                "job_quantity": row.job_quantity,
                "company_total": row.company_total,
                "fte": row.fte,
                "records": row.records,
            }
        )
    return result


def fetch_voe_7_2(payload):
    """
    Fetch Hiring percentage by function data
    """
    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
        ),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]
        ),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY]
        ),
    ]

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

    job_function_filter_str = ""
    if "job_functions" in payload["params"]:
        job_function_filter_str = "AND job_function IN UNNEST(@job_functions)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "job_functions",
                "STRING",
                json.loads(payload["params"]["job_functions"]),
            )
        )

    filter_sql = f"""
        WITH company AS (
            SELECT
                case_study_id,
                company_id,
                SUM(job_quantity) AS company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_2"]}
            WHERE
                case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {job_function_filter_str}
            GROUP BY
                case_study_id,company_id
        ),
        voe_data AS(
            SELECT
                a.case_study_id,
                a.company_id,
                company_name,
                job_function,
                SUM(job_quantity) AS job_quantity,
                MAX(fte) AS fte ,
                c.company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_2"]} a
            LEFT JOIN company c ON a.case_study_id = c.case_study_id AND a.company_id = c.company_id
            WHERE
                a.case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {job_function_filter_str}
            GROUP BY
                case_study_id,
                company_id,
                company_name,
                job_function,
                company_total
        ),
        percent_table AS (
            SELECT
            company_id,
            company_name,
            job_function,
            job_quantity,
            company_total,
            fte,
            CASE
                WHEN company_total > 0 THEN job_quantity/company_total
                ELSE NULL
            END AS job_function_pct
            FROM voe_data
        )
        SELECT
            *,
            (
                WITH voe_job AS (
                    SELECT
                        CASE
                            WHEN job_function is NULL or job_function = '' THEN 'undefined'
                            ELSE job_function
                        END AS job_function,
                        job_id
                    FROM
                        {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                    WHERE case_study_id = @case_study_id
                        AND DATE(posted_date) >= @start_date
                        AND DATE(posted_date) <= @end_date
                        {company_filter_str}
                        {source_filter_str}
            )
                SELECT COUNT (DISTINCT job_id)
                FROM voe_job
                WHERE 1 = 1
                {job_function_filter_str}
            ) AS records
        FROM
            percent_table
    """
    final_sql = construct_final_query(nlp_type="VOE_JOB",
                                      chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    result = []
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "job_function": row.job_function,
                "job_quantity": row.job_quantity,
                "job_function_pct": row.job_function_pct,
                "company_total": row.company_total,
                "fte": row.fte,
                "records": row.records,
            }
        )
    return result


def fetch_voe_7_3(payload):
    """
    Fetch % of jobs listed by function v company FTE count data
    """
    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
        ),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]
        ),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY]
        ),
    ]

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

    job_function_filter_str = ""
    if "job_functions" in payload["params"]:
        job_function_filter_str = "AND job_function IN UNNEST(@job_functions)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "job_functions",
                "STRING",
                json.loads(payload["params"]["job_functions"]),
            )
        )

    filter_sql = f"""
    WITH company AS (
        SELECT
            case_study_id,
            company_id,
            SUM(job_quantity) AS company_total
        FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_3"]}
        WHERE
            case_study_id = @case_study_id
            AND posted_date >= @start_date
            AND posted_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {job_function_filter_str}
        GROUP BY case_study_id,company_id
    ),
    voe_data AS(
        SELECT
            a.case_study_id,
            a.company_id,
            company_name,
            job_function,
            SUM(job_quantity) AS job_quantity,
            MAX(fte) AS fte ,
            CASE
                WHEN MAX(fte) IS NOT NULL THEN SUM(job_quantity)/MAX(fte) ELSE NULL
            END AS  job_function_pct,
            c.company_total
        FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_3"]} a
        LEFT JOIN company c ON a.case_study_id = c.case_study_id AND a.company_id = c.company_id
        WHERE
            a.case_study_id = @case_study_id
            AND posted_date >= @start_date
            AND posted_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {job_function_filter_str}
        GROUP BY
            case_study_id,
            company_id,
            company_name,
            job_function,
            company_total
    )
    SELECT
        *,
        (
            WITH voe_job AS (
                SELECT
                CASE
                    WHEN job_function is NULL or job_function = '' THEN 'undefined'
                    ELSE job_function
                END AS job_function,
                job_id
                FROM
                    {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id = @case_study_id
                    AND DATE(posted_date) >= @start_date
                    AND DATE(posted_date) <= @end_date
                    {company_filter_str}
                    {source_filter_str}
            )
            SELECT COUNT (DISTINCT job_id)
            FROM voe_job
            WHERE 1 = 1
            {job_function_filter_str}
        )  AS records
    FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE_JOB",
                                      chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_3"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    result = []
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "job_function": row.job_function,
                "job_quantity": row.job_quantity,
                "job_function_pct": row.job_function_pct,
                "company_total": row.company_total,
                "fte": row.fte,
                "records": row.records,
            }
        )
    return result


def fetch_voe_7_4(payload):
    """
    Fetch Hiring by Country data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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

    jobcountry_filter_str = ""
    if "job_countries" in payload["params"]:
        jobcountry_filter_str = "AND lower(job_country) IN UNNEST(@job_countries)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "job_countries",
                "STRING",
                json.loads(payload["params"]["job_countries"].lower()),
            )
        )

    filter_sql = f"""
    WITH company AS (
        SELECT
            case_study_id,
            company_id,
            SUM(job_quantity) AS company_total
        FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_4"]}
        WHERE case_study_id = @case_study_id
        AND posted_date >= @start_date
        AND posted_date <= @end_date
        {company_filter_str}
        {source_filter_str}
        {jobcountry_filter_str}
        GROUP BY case_study_id,company_id
    ),
    voe_data AS(
        SELECT
            a.case_study_id,
            a.company_id,
            company_name,
            job_country,
            SUM(job_quantity) AS job_quantity,
            MAX(fte) AS fte ,
            c.company_total
        FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_4"]} a
        LEFT JOIN company c ON a.case_study_id = c.case_study_id AND a.company_id = c.company_id
        WHERE a.case_study_id = @case_study_id
        AND posted_date >= @start_date
        AND posted_date <= @end_date
        {company_filter_str}
        {source_filter_str}
        {jobcountry_filter_str}
        GROUP BY
            case_study_id,
            company_id,
            company_name,
            job_country,
            company_total
    )
    SELECT
        *,
        (
            WITH voe_job AS (
                SELECT
                CASE
                    WHEN job_country is NULL or job_country = '' THEN 'undefined'
                    ELSE job_country
                END AS job_country,
                job_id
                FROM
                    {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id = @case_study_id
                    AND DATE(posted_date) >= @start_date
                    AND DATE(posted_date) <= @end_date
                    {company_filter_str}
                    {source_filter_str}
            )
            SELECT COUNT (DISTINCT job_id)
            FROM voe_job
            WHERE 1 = 1
            {jobcountry_filter_str}
        )  AS records
    FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE_JOB",
                                      chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_4"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "jobcountry": row.job_country,
                "job_quantity": row.job_quantity,
                "fte": row.fte,
                "company_total": row.company_total,
                "records": row.records,
            }
        )

    return result


def fetch_voe_7_5(payload):
    """
    Fetch Average number of days a functional job has been listed data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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

    jobcountry_filter_str = ""
    if "job_countries" in payload["params"]:
        jobcountry_filter_str = "AND lower(job_country) IN UNNEST(@job_countries)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "job_countries",
                "STRING",
                json.loads(payload["params"]["job_countries"].lower()),
            )
        )

    filter_sql = f"""
    WITH
        voe_data AS(
            SELECT
                case_study_id,
                job_country,
                SUM(job_quantity) AS job_quantity,
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_5"]}
            WHERE  case_study_id = @case_study_id
            AND posted_date >= @start_date
            AND posted_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {jobcountry_filter_str}
            GROUP BY
                case_study_id,
                job_country
            ORDER BY
                job_quantity DESC
            LIMIT 10
        )
        SELECT
            *,
            (
                WITH voe_job AS (
                    SELECT
                    CASE
                        WHEN job_country is NULL or job_country = '' THEN 'undefined'
                        ELSE job_country
                    END AS job_country,
                    job_id
                    FROM
                        {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                    WHERE case_study_id = @case_study_id
                        AND job_country IN (SELECT DISTINCT job_country FROM voe_data)
                        AND DATE(posted_date) >= @start_date
                        AND DATE(posted_date) <= @end_date
                        {company_filter_str}
                        {source_filter_str}
                )
                SELECT COUNT (DISTINCT job_id)
                FROM voe_job
                WHERE 1 = 1
                {jobcountry_filter_str}
            ) AS records
        FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE_JOB",
                                      chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_5"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "job_country": row.job_country,
                "job_quantity": row.job_quantity,
                "records": row.records,
            }
        )

    return result


def fetch_voe_7_6(payload):
    """
    Fetch Average number of days a functional job has been listed data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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

    job_function_filter_str = ""
    if "job_functions" in payload["params"]:
        job_function_filter_str = "AND job_function IN UNNEST(@job_functions)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "job_functions",
                "STRING",
                json.loads(payload["params"]["job_functions"]),
            )
        )

    filter_sql = f"""
        WITH info AS (
            SELECT
            company_id,
            min(newest_days) newest_days,
            max(oldest_days) oldest_days,
            CASE
                WHEN sum(job_quantity) > 0 THEN sum(sum_listing_days)/sum(job_quantity)
                ELSE NULL
            END AS average_listed_days
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_6"]}
            WHERE
                case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {job_function_filter_str}
            GROUP BY company_id),
        voe_data AS(
            SELECT
                company_id,
                company_name,
                job_function,
                SUM(sum_listing_days)/SUM(job_quantity) AS value,
                (   SELECT newest_days
                    FROM info
                    WHERE company_id=a.company_id
                ) AS newest_days,
                (   SELECT oldest_days
                    FROM info
                    WHERE company_id=a.company_id
                ) AS oldest_days,
                (   SELECT average_listed_days
                    FROM info
                    WHERE company_id=a.company_id
                ) AS average_listed_days
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_6"]} a
            WHERE
                case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {job_function_filter_str}
            GROUP BY
                company_id,
                company_name,
                job_function
                )
        SELECT
            *,
            (
                WITH voe_job AS (
                    SELECT
                    CASE
                        WHEN job_function is NULL or job_function = '' THEN 'undefined'
                        ELSE job_function
                    END AS job_function,
                    job_id
                    FROM
                        {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                    WHERE case_study_id = @case_study_id
                        AND DATE(posted_date) >= @start_date
                        AND DATE(posted_date) <= @end_date
                        {company_filter_str}
                        {source_filter_str}
                )
                SELECT COUNT (DISTINCT job_id)
                FROM voe_job
                WHERE 1 = 1
                {job_function_filter_str}
            ) AS records
        FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE_JOB",
                                      chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_7_6"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "job_function": row.job_function,
                "newest_days": row.newest_days,
                "oldest_days": row.oldest_days,
                "average_listed_days": row.average_listed_days,
                "value": row.value,
                "records": row.records,
            }
        )

    return result


def fetch_voe_8_1(payload):
    """
    Fetch Role Seniority Levels data
    """
    result = []
    filter_sql_tables = []
    TALBE_NAME_1 = "Role Seniority Levels"
    TALBE_NAME_2 = "Role Seniority Levels % v company FTE count"
    TALBE_NAME_3 = f"Role Seniority Listed as {'%'} of the company total job listing"

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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

    role_seniority_filter_str = ""
    if "role_seniorities" in payload["params"]:
        role_seniority_filter_str = (
            "AND lower(role_seniority) IN UNNEST(@role_seniorities)"
        )
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "role_seniorities",
                "STRING",
                json.loads(payload["params"]["role_seniorities"].lower()),
            )
        )

    filter_sql_table_1 = f"""
        WITH company AS (
            SELECT case_study_id,
                company_id,
                SUM(job_quantity) as company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_1"]}
                WHERE case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {role_seniority_filter_str}
            GROUP BY case_study_id,company_id),
        voe_data AS(
            SELECT
                a.case_study_id,
                a.company_id,
                company_name,
                role_seniority,
                SUM(job_quantity) as value,
                c.company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_1"]} a
            LEFT JOIN company c ON a.case_study_id = c.case_study_id AND a.company_id = c.company_id
            WHERE
                a.case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {role_seniority_filter_str}
            GROUP BY
                case_study_id,
                company_id,
                company_name,
                role_seniority,
                company_total
                )
        SELECT
            *,
            NULL AS fte,
            NULL AS job_quantity,
            (
                WITH voe_job AS (
                    SELECT
                    CASE
                        WHEN role_seniority is NULL or role_seniority = '' THEN 'undefined'
                        ELSE role_seniority
                    END AS role_seniority,
                    job_id
                    FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                    WHERE case_study_id = @case_study_id
                        AND DATE(posted_date) >= @start_date
                        AND DATE(posted_date) <= @end_date
                        {company_filter_str}
                        {source_filter_str}
                )
                SELECT COUNT (DISTINCT job_id)
                FROM voe_job
                WHERE 1 = 1
                {role_seniority_filter_str}
            )  AS records
        FROM voe_data
    """
    filter_sql_tables.append(
        {"table_name": TALBE_NAME_1, "sql": filter_sql_table_1})

    filter_sql_table_2 = f"""
        SELECT
            case_study_id,
            company_id,
            company_name,
            role_seniority,
            SUM(job_quantity) as job_quantity,
            MAX(fte) as fte,
            CASE
                WHEN max(fte) > 0 then sum(job_quantity)/max(fte)
                ELSE NULL
            END as value,
            NULL AS records,
            NULL AS company_total

        FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_1"]} a
        WHERE case_study_id = @case_study_id
            AND posted_date >= @start_date
            AND posted_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {role_seniority_filter_str}
        GROUP BY
            case_study_id,
            company_id,
            company_name,
            role_seniority
    """
    filter_sql_tables.append(
        {"table_name": TALBE_NAME_2, "sql": filter_sql_table_2})

    filter_sql_table_3 = f"""
        WITH company AS (
            SELECT
                case_study_id,company_id,
                SUM(job_quantity) as company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_1"]}
            WHERE case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {role_seniority_filter_str}
            GROUP BY case_study_id,company_id
        ),
        voe_data AS(
            SELECT
                a.case_study_id,
                a.company_id,
                company_name,
                role_seniority,
                SUM(job_quantity) as job_quantity,
                MAX(fte) as fte,
                c.company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_1"]} a
            LEFT JOIN company c ON a.case_study_id = c.case_study_id AND a.company_id = c.company_id
            WHERE a.case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {role_seniority_filter_str}
            GROUP BY
                case_study_id,
                company_id,
                company_name,
                role_seniority,
                company_total
        )
        SELECT
            *,
            CASE
                WHEN company_total > 0 then job_quantity/company_total
                ELSE null
            END as value,
            NULL AS records
        FROM voe_data
    """
    filter_sql_tables.append(
        {"table_name": TALBE_NAME_3, "sql": filter_sql_table_3})

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    for i in filter_sql_tables:
        _final_sql = construct_final_query(nlp_type="VOE_JOB",
                                           chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_1"],
                                           filter_query=i["sql"],
                                           logger=logger)
        query_job = client.query(
            _final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
        )
        for row in query_job:
            result.append(
                {
                    "table_name": i["table_name"],
                    "company_name": row.company_name,
                    "role_seniority": row.role_seniority,
                    "company_total": row.company_total,
                    "fte": row.fte,
                    "records": row.records,
                    "value": row.value,
                }
            )
    return result


def fetch_voe_8_4(payload):
    """
    Fetch Role Seniority by Job Type/Industry data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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

    jobtype_filter_str = ""
    jobtype_filter_str_a = ""
    if "job_types" in payload["params"]:
        jobtype_filter_str_a = "AND lower(a.job_type) IN UNNEST(@job_types)"
        jobtype_filter_str = "AND lower(job_type) IN UNNEST(@job_types)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "job_types",
                "STRING",
                json.loads(payload["params"]["job_types"].lower()),
            )
        )

    role_seniority_filter_str = ""
    if "role_seniorities" in payload["params"]:
        role_seniority_filter_str = (
            "AND lower(role_seniority) IN UNNEST(@role_seniorities)"
        )
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                "role_seniorities",
                "STRING",
                json.loads(payload["params"]["role_seniorities"].lower()),
            )
        )

    filter_sql = f"""
        WITH company AS (
            SELECT company_id,
                SUM(job_quantity) as company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_4"]}
            WHERE case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {jobtype_filter_str}
                {role_seniority_filter_str}
            GROUP BY company_id
        ),
        senior as (
            SELECT * ,
                CASE
                    WHEN role_seniority = 'non-manager' THEN 0
                    ELSE job_quantity
                END AS  seniority_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_4"]}
            WHERE case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {jobtype_filter_str}
                {role_seniority_filter_str}
        ),
        job_type as (
            SELECT company_id,job_type, SUM(seniority_total) as seniority_total
            FROM senior
            WHERE case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {jobtype_filter_str}
                {role_seniority_filter_str}
            GROUP BY company_id,job_type
        ),
        voe_data AS(
            SELECT
                a.case_study_id,
                a.company_id,
                a.company_name,
                a.job_type ,
                a.role_seniority,
                SUM(a.job_quantity) as job_quantity,
                MAX(a.fte) as fte,
                MAX(b.seniority_total) as seniority_total,
                c.company_total
            FROM {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_4"]} a
            LEFT JOIN job_type b ON b.company_id=a.company_id AND b.job_type=a.job_type
            LEFT JOIN company c ON a.company_id = c.company_id
            WHERE a.case_study_id = @case_study_id
                AND posted_date >= @start_date
                AND posted_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {jobtype_filter_str_a}
                {role_seniority_filter_str}
            GROUP BY
                case_study_id,
                company_id,
                company_name,
                job_type ,
                role_seniority,
                company_total
        )
        SELECT
        *,
        (
            WITH voe_job AS (
                SELECT
                CASE
                    WHEN job_type is NULL or job_type = '' THEN 'undefined'
                    ELSE job_type
                END AS job_type,
                CASE
                    WHEN role_seniority is NULL or role_seniority = '' THEN 'undefined'
                    ELSE role_seniority
                END AS role_seniority,
                job_id
                FROM
                    {VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id = @case_study_id
                    AND DATE(posted_date) >= @start_date
                    AND DATE(posted_date) <= @end_date
                    {company_filter_str}
                    {source_filter_str}
            )
            SELECT COUNT (DISTINCT job_id)
            FROM voe_job
            WHERE 1 = 1
                {jobtype_filter_str}
                {role_seniority_filter_str}
        ) AS records
        FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE_JOB",
                                      chart_code=VOE_JOB_CHART_DATA_MART_SCRIPT_MAPPING["VOE_JOB_8_4"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "job_type": row.job_type,
                "role_seniority": row.role_seniority,
                "job_quantity": row.job_quantity,
                "fte": row.fte,
                "company_total": row.company_total,
                "seniority_total": row.seniority_total,
                "records": row.records,
            }
        )
    return result


def fetch_voe_9_3(payload):
    """
    Fetch Average Sentiment Score data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
        ),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]
        ),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY]
        ),
    ]

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

    filter_sql = f"""
    WITH voe_data AS(
        SELECT
            company_name,
            SUM(sum_ss)/ SUM(sum_review_count) as ss_average
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_3"]}
        WHERE  case_study_id = @case_study_id
            AND sum_ss IS NOT NULL
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY company_name
    )
    SELECT *,
        (
            SELECT
                COUNT(DISTINCT review_id)
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE case_study_id = @case_study_id
                AND dimension_config_name IS NOT NULL
                AND review_date >= @start_date
                AND review_date <= @end_date
                {company_filter_str}
                {source_filter_str}
        ) AS records
    FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_3"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(final_sql, job_config=job_config,
                             timeout=DEFAULT_QUERY_TIMEOUT)
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "ss_average": row.ss_average,
                "records": row.records,
            }
        )
    return result


def fetch_voe_9_5(payload):
    """
    Fetch Sentiment By Company Over Time data
    """
    result = []

    ma_type = ''
    ma_records = ''

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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
    WITH voe_data AS(
            SELECT
                company_name,
                daily_date AS date,
                SUM({ma_type})/SUM({ma_records}) AS value
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5"]}
            WHERE
                {ma_records} IS NOT NULL
                AND {ma_type} IS NOT NULL
                AND case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                GROUP BY company_name, daily_date
    )
    SELECT *,
        (
            SELECT
                COUNT(DISTINCT review_id)
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE
                case_study_id = @case_study_id
                AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                AND dimension IS NOT NULL
                {company_filter_str}
                {source_filter_str}
        ) AS records
    FROM voe_data
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "date": row.date.isoformat(),
                "records": row.records,
                "value": row.value
            }
        )

    return result


def fetch_voe_9_5_1(payload):
    """
    Fetch Average sentiment score. data

    NOTE: This chart has the same data backend as 9_5,
        but with different filter query attached to the end
    """
    result = []

    ma_type = ''
    ma_type_filter = ''
    ma_records = ''

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    elif payload['params']['ma_type'].lower() == RAW_AVERAGE_FILTER:
        ma_type = SS_DAILY
        ma_type_filter = RAW_AVERAGE_FILTER
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload['params']['ma_type']]

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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
    WITH cte_review AS (
    SELECT
        company_name,
        COUNT(DISTINCT review_id) as distinct_review_count
    FROM  {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
    WHERE
        case_study_id = @case_study_id
        AND dimension_config_name IS NOT NULL
        AND is_used = True
        AND polarity IN ('N', 'N+', 'NEU', 'P', 'P+')
        AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
        AND review_date <= @end_date
        {company_filter_str}
        {source_filter_str}
    GROUP BY company_name)

    SELECT
        company_name,
        SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_type} ELSE NULL END) / SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_records} ELSE NULL END) AS value,
        (SELECT distinct_review_count FROM cte_review
        WHERE company_name = a.company_name) AS review_count
    FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5_1"]} a
    WHERE
        case_study_id = @case_study_id
        AND daily_date >= @start_date
        AND daily_date <= @end_date
        {company_filter_str}
        {source_filter_str}
    GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "value": row.value,
                "review": row.review_count
            }
        )

    return result


def fetch_voe_9_5_2(payload):
    """
    Fetch Period trend sentiment score. data

    NOTE: This chart has the same data backend as 9_5,
        but with different filter query attached to the end
    """
    result = []

    start_date = payload['params'][START_DATE_FILTER_KEY]
    end_date = payload['params']['end_date']
    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    datediff = abs((start_datetime - end_datetime).days)
    if datediff < 3:
        return []

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
        ),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", start_date
        ),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", end_date
        ),
    ]

    ma_type = ''
    ma_records = ''
    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload['params']['ma_type']]

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
    WITH day_diff AS (
    SELECT
        case_study_id,
        MIN(daily_date) AS min_date,
        MAX(daily_date) AS max_date,
        ROUND(DATE_DIFF(MAX(daily_date), MIN(daily_date), DAY)/3,0) AS days_diff
    FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5_2"]}
    WHERE case_study_id = @case_study_id
        AND daily_date >= @start_date
        AND daily_date <= @end_date
        {company_filter_str}
        {source_filter_str}
    GROUP BY case_study_id
    )
    , node AS (
    SELECT
        case_study_id,
        DATE_ADD(min_date, INTERVAL CAST(days_diff AS int64) DAY) AS node1,
        DATE_ADD(min_date, INTERVAL 2*CAST(days_diff AS int64) DAY) AS node2,
        max_date AS node3
    FROM day_diff
    )
    , ranking AS (
    SELECT
        company_name,
        CASE
            WHEN daily_date <= node1 THEN 'Period 1'
            WHEN daily_date > node1 AND daily_date <= node2 THEN 'Period 2'
            WHEN daily_date > node2 THEN 'Period 3'
            ELSE null
        END AS period,
        CASE
        WHEN DATE_DIFF(node2,node1,MONTH) <= 1 THEN
            CASE
            WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,@start_date),'-',FORMAT_DATE('%d/%m/%y' ,node1))
            WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node1+1),'-',FORMAT_DATE('%d/%m/%y' ,node2))
            WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node2+1),'-',FORMAT_DATE('%d/%m/%y' ,node3))
            ELSE null
            END
        ELSE
            CASE
            WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%b %Y' ,@start_date),'-',FORMAT_DATE('%b %Y' ,node1))
            WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node1+1),'-',FORMAT_DATE('%b %Y' ,node2))
            WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node2+1),'-',FORMAT_DATE('%b %Y' ,node3))
            ELSE null
            END
        END AS period_time,
        SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_type} ELSE NULL END) / SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_records} ELSE NULL END) AS value
    FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5_2"]} a
        LEFT JOIN node b
        ON a.case_study_id = b.case_study_id
    WHERE
        {ma_records} IS NOT NULL
        AND {ma_type} IS NOT NULL
        AND a.case_study_id = @case_study_id
        AND daily_date >= @start_date
        AND daily_date <= @end_date
        {company_filter_str}
        {source_filter_str}
    GROUP BY company_name, period, period_time
    )
    , cte_review AS (
    SELECT
        company_name,
        COUNT(DISTINCT CASE
        WHEN review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY) AND review_date <= node1 THEN  review_id
        ELSE NULL
        END) Period1,
        COUNT(DISTINCT CASE
        WHEN review_date > DATE_SUB(node1,INTERVAL {ma_number[1]} DAY) AND review_date <= node2 THEN review_id
        ELSE NULL
        END) AS Period2,
        COUNT(DISTINCT CASE
        WHEN review_date > DATE_SUB(node2,INTERVAL {ma_number[1]} DAY) AND   review_date <= node3 THEN review_id
        ELSE NULL
        END) AS Period3
    FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} a
        LEFT JOIN node b
        ON a.case_study_id = b.case_study_id
    WHERE
        a.case_study_id = @case_study_id
        AND dimension_config_name is not null
        AND polarity  IN ('N', 'N+', 'NEU', 'P', 'P+')
        AND is_used = true
        {company_filter_str}
        {source_filter_str}
    GROUP BY company_name
    )
    ,review_records AS (
    SELECT company_name, 'Period 1' as review_period, Period1 as distinct_review_count FROM cte_review
    UNION ALL
    SELECT company_name, 'Period 2' as review_period, Period2 as distinct_review_count FROM cte_review
    UNION ALL
    SELECT company_name, 'Period 3' as review_period, Period3 as distinct_review_count FROM cte_review
    )
    , total AS (
    SELECT
        company_name,
        SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_type} ELSE NULL END)/SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_records} ELSE NULL END) AS rank_value
    FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5_2"]}
    WHERE
        {ma_records} IS NOT NULL
        AND {ma_type} IS NOT NULL
        AND case_study_id = @case_study_id
        AND daily_date >= @start_date
        AND daily_date <= @end_date
        {company_filter_str}
        {source_filter_str}
    GROUP BY company_name
    )
    SELECT
        a.*,
        r.distinct_review_count as review_count,
        rank_value
    FROM ranking a
    LEFT JOIN total b
    ON a.company_name = b.company_name
    LEFT JOIN review_records r
    ON  a.company_name = r.company_name
    AND r.review_period = a.period
    ORDER BY period
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_5_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )

    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "period": row.period,
            "period_time": row.period_time,
            "value": row.value,
            "rank": row.rank_value,
            "review": row.review_count
        })

    return result


def fetch_voe_10_1(payload):
    """
    Fetch Signal Sentiment Scores data
    """
    result = []
    NUMBER_TOP_DIMENSION = 9

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload["params"]:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload["params"][SOURCE_NAME_FILTER_KEY].lower().split(",")))

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload["params"]:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload["params"]:
        dimension_type_filter_str = "AND lower(dimension_type) IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload["params"][DIMENSION_TYPE_FILTER_KEY].lower().split(",")))

    filter_sql = f"""
        WITH  cte_review AS (
        SELECT
            company_name,
            modified_dimension,
            COUNT(DISTINCT review_id) as distinct_review_count
        FROM  {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
        WHERE
            case_study_id = @case_study_id
            AND dimension_config_name IS NOT NULL
            AND dimension IS NOT NULL
            AND is_used = true
            AND polarity  IN ('N', 'N+', 'NEU', 'P', 'P+')
            AND review_date >= @start_date
            AND review_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {summary_table_dimension_filter_str}
            {dimension_type_filter_str}
        GROUP BY
            company_name,
            modified_dimension
            )
        , voe_data AS (
        SELECT dimension,
            dimension_type,
            company_name,
            CASE WHEN SUM(sum_ss) IS NULL OR SUM(sum_review_count) IS NULL THEN NULL
                ELSE ROUND(SUM(sum_ss) / SUM(sum_review_count), 2)
            END AS sentiment_score,
            (SELECT distinct_review_count FROM cte_review
            WHERE company_name = a.company_name
            AND modified_dimension = a.dimension
            ) AS review_count,
            (SELECT COUNT(DISTINCT review_id) FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE case_study_id = @case_study_id
                AND dimension_config_name IS NOT NULL
                AND review_date >= @start_date
                AND review_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {summary_table_dimension_filter_str}
                {dimension_type_filter_str}
                AND modified_dimension = a.dimension
            ) AS collected_review_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_10_1"]} a
        WHERE case_study_id =  @case_study_id
            AND sum_ss IS NOT NULL
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {dimension_filter_str}
            {dimension_type_filter_str}
        GROUP BY dimension, dimension_type, company_name
        )
        ,final AS (
        SELECT *,
            (
            SELECT COUNT(DISTINCT review_id)
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE case_study_id =  @case_study_id
                AND review_date >= @start_date
                AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                AND dimension is not null
                AND is_used = true
                {company_filter_str}
                {source_filter_str}
                {summary_table_dimension_filter_str}
                {dimension_type_filter_str}
            ) AS records,
            dense_rank() over (order by collected_review_count desc) AS rank
        FROM voe_data
        )
        SELECT * FROM final
        WHERE rank <= {NUMBER_TOP_DIMENSION}
        ORDER BY collected_review_count DESC, sentiment_score DESC
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_10_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "dimension": row.dimension,
            "dimension_type": row.dimension_type,
            "company_name": row.company_name,
            "sentiment_score": row.sentiment_score,
            "records": row.records,
            "rank": row.rank,
            "review": row.review_count
        })

    return result


def fetch_voe_11(payload):
    """
    Fetch Most common terms data
    """
    return_data = []
    page_size = None
    start_index = 0

    if 'page' in payload['params'] and 'page_size' in payload['params']:
        page = int(payload['params']['page'])
        page_size = int(payload['params']['page_size'])

        start_index = page_size * (page - 1)

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    polarity_filter_str = ""
    if 'polarities' in payload['params']:
        polarity_filter_str = "AND polarity IN UNNEST(@polarities)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "polarities", "STRING", payload['params']['polarities'].split(',')))

    filter_sql = f"""
        WITH voe_data AS (
            SELECT company_name, single_terms, polarity, SUM(collected_review_count) AS count
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_11"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {polarity_filter_str}
            GROUP BY company_name, single_terms, polarity
            ORDER BY company_name, count DESC, single_terms DESC, polarity DESC
        )
        SELECT *,
            (SELECT COUNT(DISTINCT review_id)
                FROM
                    {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE
                    case_study_id = @case_study_id AND review_date >= @start_date AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND dimension IS NOT NULL
            AND is_used = true
                    {company_filter_str}
                    {source_filter_str}
                    {polarity_filter_str}
            ) AS records
        FROM
            voe_data
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_11"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    query_job.result()
    destination = query_job.destination
    table = client.get_table(destination)
    results = client.list_rows(
        table, max_results=page_size, start_index=start_index)
    for result in results:
        return_data.append({
            "company": result.company_name,
            "terms": result.single_terms,
            "polarity": result.polarity,
            "count": result.count,
            "records": result.records
        })

    return return_data


def fetch_voe_11_1(payload):
    """
    Fetch Most common terms data
    """
    return_data = []
    page_size = None
    start_index = 0

    if 'page' in payload['params'] and 'page_size' in payload['params']:
        page = int(payload['params']['page'])
        page_size = int(payload['params']['page_size'])

        start_index = page_size * (page - 1)

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    polarity_filter_str = ""
    if 'polarities' in payload['params']:
        polarity_filter_str = "AND polarity IN UNNEST(@polarities)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "polarities", "STRING", payload['params']['polarities'].split(',')))

    filter_sql = f"""
        WITH voe_data AS (
            SELECT company_name, single_terms, polarity, SUM(collected_review_count) AS count
            FROM `{VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_11_1"]}`
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                AND single_terms != ''
                AND single_terms IS NOT NULL
                {company_filter_str}
                {source_filter_str}
                {polarity_filter_str}
            GROUP BY company_name, single_terms, polarity
            ORDER BY company_name, count DESC, single_terms DESC, polarity DESC
        )
        SELECT *,
            (SELECT COUNT(DISTINCT review_id)
                FROM
                    `{VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}`
                WHERE
                    case_study_id = @case_study_id AND review_date >= @start_date AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND dimension IS NOT NULL
            AND is_used = true
                    {company_filter_str}
                    {source_filter_str}
                    {polarity_filter_str}
            ) AS records
        FROM
            voe_data
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_11_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    query_job.result()
    destination = query_job.destination
    table = client.get_table(destination)
    results = client.list_rows(
        table, max_results=page_size, start_index=start_index)
    for result in results:
        return_data.append({
            "company": result.company_name,
            "terms": result.single_terms,
            "polarity": result.polarity,
            "count": result.count,
            "records": result.records
        })

    return return_data


def fetch_voe_9_4(payload):
    """
    Fetch Employee reviews count over time data
    """
    result = []

    ma_type = ''

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, ER_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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
        WITH voe_data AS(
            SELECT company_name,
                daily_date AS date,
                SUM({ma_type}) AS value
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                AND {ma_type} IS NOT NULL
            GROUP BY
                company_name,
                daily_date
        )
        SELECT
            *,
            (SELECT
                    COUNT(DISTINCT parent_review_id)
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
            WHERE
                s.case_study_id = @case_study_id
                AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                AND review_date <= @end_date
                AND dimension_config_name is not null
                {company_filter_str}
                {source_filter_str}
            ) AS records
        FROM voe_data
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "date": row.date.isoformat(),
                "records": row.records,
                "value": row.value
            }
        )

    return result


def fetch_voe_9_4_1(payload):
    """
    Fetch  Average sentiment score data

    NOTE: This chart has the same data backend as 9_4,
        but with different filter query attached to the end
    """
    result = []

    ma_type = ''
    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = RECORDS_DAILY
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, ER_MA)

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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
            AVG(
                CASE 
                    WHEN {ma_type} > 0 THEN {ma_type} 
                    WHEN {ma_type} IS NULL THEN 0
                    ELSE 0 
                END
            ) AS value
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4_1"]}
        WHERE
            case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "value": row.value
            }
        )

    return result


def fetch_voe_9_4_2(payload):
    """
    Fetch Period trend sentiment score data

    NOTE: This chart has the same data backend as 9_4,
        but with different filter query attached to the end
    """
    result = []

    ma_type = ''
    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = RECORDS_DAILY
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, ER_MA)

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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
         WITH day_diff AS (
            SELECT
                case_study_id,
                MIN(daily_date) AS min_date,
                MAX(daily_date) AS max_date,
                ROUND(DATE_DIFF(MAX(daily_date), MIN(daily_date), DAY)/3,0) AS days_diff
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4_2"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
            GROUP BY case_study_id
        ), node AS (
            SELECT
                case_study_id,
                DATE_ADD(min_date, INTERVAL CAST(days_diff AS int64) DAY) AS node1,
                DATE_ADD(min_date, INTERVAL 2*CAST(days_diff AS int64) DAY) AS node2,
                max_date AS node3
            FROM day_diff
        ), ranking AS (
            SELECT
                company_name,
                CASE
                    WHEN daily_date <= node1 THEN 'Period 1'
                    WHEN daily_date > node1 AND daily_date <= node2 THEN 'Period 2'
                    WHEN daily_date > node2 THEN 'Period 3'
                    ELSE null
                END AS period,
                CASE
                WHEN DATE_DIFF(node2,node1,MONTH) <= 1 THEN
                    CASE
                    WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,@start_date),'-',FORMAT_DATE('%d/%m/%y' ,node1))
                    WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node1+1),'-',FORMAT_DATE('%d/%m/%y' ,node2))
                    WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node2+1),'-',FORMAT_DATE('%d/%m/%y' ,node3))
                    ELSE null
                    END
                ELSE
                    CASE
                    WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%b %Y' ,@start_date),'-',FORMAT_DATE('%b %Y' ,node1))
                    WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node1+1),'-',FORMAT_DATE('%b %Y' ,node2))
                    WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node2+1),'-',FORMAT_DATE('%b %Y' ,node3))
                    ELSE null
                    END
                END AS period_time,
                AVG(
                    CASE 
                        WHEN {ma_type} > 0 THEN {ma_type} 
                        WHEN {ma_type} IS NULL THEN 0
                        ELSE 0 
                    END
                ) AS value
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4_2"]} a
            LEFT JOIN node b ON a.case_study_id = b.case_study_id
            WHERE
                a.case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
            GROUP BY company_name, period, period_time
        ), total AS (
            SELECT
                company_name,
                AVG(
                    CASE 
                        WHEN {ma_type} > 0 THEN {ma_type} 
                        WHEN {ma_type} IS NULL THEN 0
                        ELSE 0 
                    END
                ) AS rank_value
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4_2"]}
            WHERE
                case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
            GROUP BY company_name
        )
        SELECT ranking.*, total.rank_value
        FROM ranking
        LEFT JOIN total ON ranking.company_name = total.company_name
        ORDER BY period
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_4_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )

    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "period": row.period,
            "period_time": row.period_time,
            "value": row.value,
            "rank": row.rank_value
        })

    return result


def fetch_voe_9_1(payload):
    """
    Fetch Employee Reviews by Company data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(
            CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
                payload["params"][CASE_STUDY_ID_FILTER_KEY])
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
            SELECT company_name,
                SUM(records) AS records,
                SUM(collected_review_count) AS collected_review_count
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_1"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
            GROUP BY company_name
        """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_9_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(
        final_sql, job_config=job_config,
        timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append(
            {
                "company_name": row.company_name,
                "collected_review_count": row.collected_review_count,
                "records": row.records,
            }
        )

    return result


# Port back area

def fetch_voe_1_3_2(payload):
    """
    Fetch Reviews count by players and data sources data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    filter_sql = f"""
        SELECT
            source_name,
            company_name,
            SUM(records) AS records
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_3_2"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY
            source_name,
            company_name
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_3_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "source_name": row.source_name,
            "company_name": row.company_name,
            "records": row.records
        })
    return result


def fetch_voe_1_4(payload):
    """
    Fetch Collected customer reviews count by language data
    """
    result = []

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_number = (1, 0)  # Daily type
    else:
        ma_number = MA_TYPES[payload['params']['ma_type']]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    language_filter_str = ""
    if 'language_codes' in payload['params']:
        language_filter_str = "AND language_code IN UNNEST(@language_codes)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "language_codes", "STRING", payload['params']['language_codes'].split(',')))

    filter_sql = f"""
        WITH voe_data AS(
            SELECT language_name,
                language_code,
                daily_date AS date,
                sum(records_daily) records_daily
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_4"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {language_filter_str}
            GROUP BY language_name, language_code, daily_date
        ),
        foo AS (
            SELECT
                *,
                CASE
                    WHEN COUNT(date) OVER (
                        PARTITION BY
                        language_name,
                        language_code
                        ORDER BY
                            date ROWS BETWEEN {ma_number[1]} PRECEDING
                            AND CURRENT ROW
                    ) < {ma_number[0]} THEN NULL
                    ELSE AVG(records_daily) OVER (
                        PARTITION BY
                        language_name,
                        language_code
                        ORDER BY
                            date ROWS BETWEEN {ma_number[1]} PRECEDING
                            AND CURRENT ROW
                    )
                END AS value,
                (
                    SELECT
                        COUNT(DISTINCT parent_review_id)
                    FROM
                        {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
                    WHERE
                        s.case_study_id = @case_study_id
                        AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                        AND review_date <= @end_date
                        AND dimension_config_name IS NOT NULL
                        {company_filter_str}
                        {language_filter_str}
                ) AS records
            FROM voe_data
        )
        SELECT CONCAT('[', string_agg(to_json_string(t)),']') AS value
        FROM foo t
        WHERE t.value IS NOT NULL
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_4"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result = row.value
    return json.loads(result) if result else json.loads('[]')


def fetch_voe_1_5(payload):
    """
    Fetch Collected customer reviews count by language data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    language_filter_str = ""
    if 'language_codes' in payload['params']:
        language_filter_str = "AND language_code IN UNNEST(@language_codes)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "language_codes", "STRING", payload['params']['language_codes'].split(',')))

    filter_sql = f"""
        WITH voe_data AS (
            SELECT language_code,
                language_name,
                SUM(collected_review_count) AS collected_review_count
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_5"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {language_filter_str}
            GROUP BY language_code, language_name
        ),
        foo AS (SELECT
            *,
            (   SELECT
                    COUNT(DISTINCT parent_review_id)
                FROM
                    {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE
                    st.case_study_id = @case_study_id
                    AND review_date >= @start_date
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND st.language_code = voe_data.language_code
                    AND st.language = voe_data.language_name
                    {company_filter_str}
                    {language_filter_str}
            ) AS records
        FROM
            voe_data
        )
        SELECT CONCAT('[', string_agg(to_json_string(t)),']') AS value
        FROM foo t
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_5"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result = row.value
    return json.loads(result) if result else json.loads('[]')


def fetch_voe_1_6(payload):
    """
    Fetch Reviews count by data source data
    """
    result = []

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_number = (1, 0)  # Daily type
    else:
        ma_number = MA_TYPES[payload['params']['ma_type']]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    filter_sql = f"""
        WITH voe_data AS (
            SELECT
                source_name,
                daily_date AS date,
                sum(records_daily) AS records_daily
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_6"]}
            WHERE
                case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
            GROUP BY source_name, daily_date
        ),
        foo AS (
            SELECT
                *,
                CASE
                    WHEN COUNT(date) OVER (
                        PARTITION BY
                        source_name
                        ORDER BY
                            date ROWS BETWEEN {ma_number[1]} PRECEDING
                            AND CURRENT ROW
                    ) < {ma_number[0]} THEN NULL
                    ELSE AVG(records_daily) OVER (
                        PARTITION BY
                        source_name
                        ORDER BY
                            date ROWS BETWEEN {ma_number[1]} PRECEDING
                            AND CURRENT ROW
                    )
                END AS value,
                (
                    SELECT COUNT(DISTINCT parent_review_id)
                    FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
                    WHERE
                        s.case_study_id = @case_study_id
                        AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                        AND review_date <= @end_date
                        AND dimension_config_name IS NOT NULL
                        {company_filter_str}
                        {source_filter_str}
                ) AS records
            FROM voe_data
        )
        SELECT *
        FROM foo
        WHERE value IS NOT NULL
    """

    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_6"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "source_name": row.source_name,
            "date": row.date.isoformat(),
            "records": row.records,
            "value": row.value
        })
    return result


def fetch_voe_1_7(payload):
    """
    Fetch Reviews collected and processed successfully  data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
        SELECT
            company_name,
            SUM(records) AS records,
            SUM(collected_review_count) AS collected_review_count,
            SUM(processed_review_count) AS processed_review_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_7"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_7"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "records": row.records,
            "collected_review_count": row.collected_review_count,
            "processed_review_count": row.processed_review_count
        })
    return result


def fetch_voe_2(payload):
    """
    Fetch Reviews distribution per polarity data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
        WITH voe_data AS(
            SELECT
            company_name,
            polarity ,
            sum(collected_review_count) collected_review_count
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_2"]}
            WHERE  case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            GROUP BY company_name, polarity
        )
        SELECT
            *,
            (
                SELECT
                    COUNT(DISTINCT review_id)
                FROM
                    {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE case_study_id = @case_study_id
                    AND review_date >= @start_date
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    {company_filter_str}
            ) AS records
        FROM
            voe_data
        ORDER BY polarity DESC, company_name
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "polarity": row.polarity,
            "collected_review_count": row.collected_review_count,
            "records": row.records
        })
    return result


def fetch_voe_3(payload):
    """
    Fetch Employee review ratings data
    """
    result = []

    ma_type = ''
    ma_records = ''

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = RATING_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, RATING_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    filter_sql = f"""
        WITH voe_data AS(
            SELECT
            source_name,
            daily_date as date,
            {ma_type} as sum_rating,
            {ma_records} as sum_review_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_3"]}
        WHERE
            case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            AND {ma_records} IS NOT NULL
            AND {ma_type} IS NOT NULL
            {company_filter_str}
            {source_filter_str}
        )
        SELECT
        *,
        (
            SELECT
                COUNT(DISTINCT parent_review_id)
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
            WHERE
                st.case_study_id = @case_study_id
                AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                AND review_date <= @end_date
                AND dimension_config_name is not null
                {company_filter_str}
                {source_filter_str}
        ) as records
        FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_3"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "source_name": row.source_name,
            "date": row.date.isoformat(),
            "records": row.records,
            "sum_rating": int(row.sum_rating),
            "sum_review_count": row.sum_review_count
        })
    return result


def fetch_voe_6_1(payload):
    """
    Fetch Dimension sentiment score per company data
    """
    result = []

    ma_type = ''
    ma_records = ''

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload['params']:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    filter_sql = f"""
    WITH voe_data AS(
        SELECT
            company_name,
            daily_date AS date,
            {ma_type} AS sum_ss,
            {ma_records} AS sum_review_counts
        FROM
            {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_1"]}
        WHERE
            {ma_records} IS NOT NULL
            AND case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {dimension_filter_str}
    )
        SELECT
        *,
        (
            SELECT
                COUNT(DISTINCT review_id)
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
            WHERE
                case_study_id = @case_study_id
                AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
				AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                AND dimension is not null
                AND is_used = true
                {company_filter_str}
                {summary_table_dimension_filter_str}
        ) AS records
    FROM
        voe_data
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "date": row.date.isoformat(),
            "sum_ss": row.sum_ss,
            "sum_review_counts": row.sum_review_counts,
            "records": row.records
        })
    return result


def fetch_voe_6_2(payload):
    """
    Fetch Dimensions mentioned in reviews data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload['params']:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload['params']:
        dimension_type_filter_str = "AND dimension_type IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload['params'][DIMENSION_TYPE_FILTER_KEY].split(',')))

    filter_sql = f"""
    WITH voe_data AS (
        SELECT
            dimension, SUM(collected_review_count) AS collected_review_count
        FROM
            {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_2"]}
        WHERE
            case_study_id = @case_study_id
            AND daily_date >= @start_date AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {dimension_filter_str}
            {dimension_type_filter_str}
        GROUP BY
            dimension
        ORDER BY
            collected_review_count DESC
    )
    SELECT
        *,
        (
            SELECT
                COUNT(DISTINCT review_id)
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE
                case_study_id = @case_study_id
                AND review_date >= @start_date AND review_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {summary_table_dimension_filter_str}
                {dimension_type_filter_str}
                AND dimension_config_name IS NOT NULL
                AND dimension is not null
                AND is_used = true
        ) AS records
    FROM
        voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "dimension": row.dimension,
            "collected_review_count": row.collected_review_count,
            "records": row.records
        })
    return result


def fetch_voe_6_3(payload):
    """
    Fetch Dimensions mentioned: Positive and Negative data
    """
    POSITIVE_LABEL = "Positive"
    NEGATIVE_LABEL = "Negative"
    RECORDS = "records"
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload['params']:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload['params']:
        dimension_type_filter_str = "AND dimension_type IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload['params'][DIMENSION_TYPE_FILTER_KEY].split(',')))

    filter_sql = f"""
    WITH voe_data AS (
        SELECT dimension, polarity_type, SUM(collected_review_count) AS collected_review_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_3"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {dimension_filter_str}
            {dimension_type_filter_str}
        GROUP BY dimension, polarity_type)
    SELECT *,
        (
            SELECT COUNT(DISTINCT review_id)
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE
                case_study_id = @case_study_id AND review_date >= @start_date AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                AND polarity IN ('N','N+','NEU','P','P+')
                AND is_used = True
                {company_filter_str}
                {source_filter_str}
                {summary_table_dimension_filter_str}
                {dimension_type_filter_str}
        ) AS records
    FROM
        voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_3"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )

    for row in query_job:
        result.append({
            "dimension": row.dimension,
            "polarity": row.polarity_type,
            "value": row.collected_review_count,
            "records": row.records
        })

    return result


def fetch_voe_6_5(payload):
    """
    Fetch Competitors overlap analysis data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    filter_sql = f"""
    WITH voe_data AS (
        SELECT company_name, competitor,
            SUM(processed_review_count) AS sum_processed_review_count,
            SUM(processed_review_mention) AS sum_processed_review_mention,
            SUM(sum_mentioned) AS sum_mentioned
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_5"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY company_name, competitor
    )
    SELECT *,
    (
        SELECT COUNT(DISTINCT review_id)
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
        WHERE case_study_id = @case_study_id
            AND review_date >= @start_date
            AND review_date <= @end_date
            {company_filter_str}
            {source_filter_str}
    ) AS records
    FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_5"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "competitor": row.competitor,
            "sum_processed_review_count": row.sum_processed_review_count,
            "sum_processed_review_mention": row.sum_processed_review_mention,
            "sum_mentioned": row.sum_mentioned,
            "records": row.records
        })

    return result


def fetch_voe_6_6(payload):
    """
    Fetch Sentiment score by data source per company data
    """
    result = []

    ma_type = ''
    ma_records = ''

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    filter_sql = f"""
        WITH voe_data AS (
            SELECT source_name,
                daily_date as date,
                {ma_type} AS sum_ss,
                {ma_records} AS sum_review_counts
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_6"]}
            WHERE {ma_records} IS NOT NULL
                AND {ma_type} IS NOT NULL
                AND case_study_id = @case_study_id
                AND daily_date >= @start_date AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
        )
        SELECT
            *,
            (
                SELECT
                    COUNT(DISTINCT review_id)
                FROM
                    {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name is not null
                    AND dimension is not null
                    {company_filter_str}
                    {source_filter_str}
            ) AS records
        FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_6"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "source_name": row.source_name,
            "date": row.date.isoformat(),
            "sum_ss": row.sum_ss,
            "sum_review_counts": row.sum_review_counts,
            "records": row.records
        })
    return result


def fetch_voe_6_7(payload):
    """
    Fetch data
    """
    return_data = []
    page_size = None
    start_index = 0

    if 'page' in payload['params'] and 'page_size' in payload['params']:
        page = int(payload['params']['page'])
        page_size = int(payload['params']['page_size'])

        start_index = page_size * (page - 1)

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    polarity_filter_str = ""
    if 'polarities' in payload['params']:
        polarity_filter_str = "AND polarity IN UNNEST(@polarities)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "polarities", "STRING", payload['params']['polarities'].split(',')))

    filter_sql = f"""
    WITH voe_data AS (
        SELECT company_name, single_terms, polarity, SUM(collected_review_count) AS count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_7"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {polarity_filter_str}
        GROUP BY company_name, single_terms, polarity
        ORDER BY company_name, count DESC, single_terms DESC, polarity DESC
    )
    SELECT *,
        (SELECT COUNT(DISTINCT review_id)
            FROM
                {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE
                case_study_id = @case_study_id
                AND review_date >= @start_date
                AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                AND dimension IS NOT NULL
                {company_filter_str}
                {source_filter_str}
                {polarity_filter_str}
        ) AS records
    FROM
        voe_data
    """

    # construct full query to send to BQ
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_7"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    query_job.result()
    destination = query_job.destination
    table = client.get_table(destination)
    results = client.list_rows(
        table, max_results=page_size, start_index=start_index)
    for result in results:
        return_data.append({
            "company": result.company_name,
            "terms": result.single_terms,
            "polarity": result.polarity,
            "count": result.count,
            "records": result.records
        })

    return return_data


def fetch_voe_6_8(payload):
    """
    Fetch Average sentiment score per dimension data

    NOTE: This chart has the same data backend as 6_1,
        but with different filter query attached to the end
    """
    return_data = []

    ma_type = ''
    ma_records = ''
    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload['params']['ma_type']]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload["params"]:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    filter_sql = f"""
    WITH cte_review AS (
    SELECT
        company_name,
        COUNT(DISTINCT review_id) as distinct_review_count
    FROM  {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
    WHERE
        case_study_id = @case_study_id
        AND dimension_config_name IS NOT NULL
        AND is_used = True
        AND polarity IN ('N', 'N+', 'NEU', 'P', 'P+')
        AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
        AND review_date <= @end_date
        {company_filter_str}
        {summary_table_dimension_filter_str}
    GROUP BY company_name)
    SELECT a.company_name,
        SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_type} ELSE NULL END) / SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_records} ELSE NULL END) AS value,
        (SELECT distinct_review_count FROM cte_review
        WHERE company_name = a.company_name) AS review_count
    FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_8"]} a
    WHERE
        {ma_records} IS NOT NULL
        AND case_study_id = @case_study_id
        AND daily_date >= @start_date
        AND daily_date <= @end_date
        {company_filter_str}
        {dimension_filter_str}
    GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING['VOE_6_8'],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        return_data.append({
            "company_name": row.company_name,
            "value": row.value,
            "review": row.review_count
        })

    return return_data


def fetch_voe_6_9(payload):
    """
    Fetch Period trend sentiment score per dimension data

    NOTE: This chart has the same data backend as 6_1,
        but with different filter query attached to the end
    """
    start_date = payload['params'][START_DATE_FILTER_KEY]
    end_date = payload['params']['end_date']

    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    datediff = abs((start_datetime - end_datetime).days)
    if datediff < 3:
        return []

    return_data = []

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload['params']['ma_type']]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", start_date),
        bigquery.ScalarQueryParameter(END_DATE_FILTER_KEY, "DATE", end_date)
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload["params"]:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    filter_sql = f"""
        WITH day_diff AS (
        SELECT case_study_id,
            MIN(daily_date) AS min_date,
            MAX(daily_date) AS max_date,
            ROUND(DATE_DIFF(MAX(daily_date), MIN(daily_date), DAY)/3,0) AS days_diff
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_9"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {dimension_filter_str}
        GROUP BY case_study_id
        )
        , node AS (
        SELECT case_study_id,
            DATE_ADD(min_date, INTERVAL CAST(days_diff AS int64) DAY) AS node1,
            DATE_ADD(min_date, INTERVAL 2*CAST(days_diff AS int64) DAY) AS node2,
            max_date AS node3
        FROM day_diff)
        , ranking AS (
        SELECT company_name,
            CASE
                WHEN daily_date <= node1 THEN 'Period 1'
                WHEN daily_date > node1 AND daily_date <= node2 THEN 'Period 2'
                WHEN daily_date > node2 THEN 'Period 3'
                ELSE null
            END AS period,
            CASE
                WHEN DATE_DIFF(node2,node1,MONTH) <= 1 THEN
                    CASE
                    WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,@start_date),'-',FORMAT_DATE('%d/%m/%y' ,node1))
                    WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node1+1),'-',FORMAT_DATE('%d/%m/%y' ,node2))
                    WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node2+1),'-',FORMAT_DATE('%d/%m/%y' ,node3))
                    ELSE null
                    END
                ELSE
                    CASE
                    WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%b %Y' ,@start_date),'-',FORMAT_DATE('%b %Y' ,node1))
                    WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node1+1),'-',FORMAT_DATE('%b %Y' ,node2))
                    WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node2+1),'-',FORMAT_DATE('%b %Y' ,node3))
                    ELSE null
                    END
                END AS period_time,
            SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_type} ELSE NULL END) / SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_records} ELSE NULL END) AS value
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_9"]} a
            LEFT JOIN node b
            ON a.case_study_id = b.case_study_id
        WHERE {ma_records} IS NOT NULL
            AND a.case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {dimension_filter_str}
        GROUP BY company_name, period, period_time
        )
        , cte_review AS (
        SELECT
            company_name,
            COUNT(DISTINCT CASE
            WHEN review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY) AND review_date <= node1 THEN  review_id
            ELSE NULL
            END) Period1,
            COUNT(DISTINCT CASE
            WHEN review_date > DATE_SUB(node1,INTERVAL {ma_number[1]} DAY) AND review_date <= node2 THEN review_id
            ELSE NULL
            END) AS Period2,
            COUNT(DISTINCT CASE
            WHEN review_date > DATE_SUB(node2,INTERVAL {ma_number[1]} DAY) AND   review_date <= node3 THEN review_id
            ELSE NULL
            END) AS Period3
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} a
            LEFT JOIN node b
            ON a.case_study_id = b.case_study_id
        WHERE
            a.case_study_id = @case_study_id
            AND dimension_config_name is not null
            AND polarity  IN ('N', 'N+', 'NEU', 'P', 'P+')
            AND is_used = true
            {company_filter_str}
            {summary_table_dimension_filter_str}
        GROUP BY company_name
        )
        ,review_records AS (
        SELECT company_name, 'Period 1' as review_period, Period1 as distinct_review_count FROM cte_review
        UNION ALL
        SELECT company_name, 'Period 2' as review_period, Period2 as distinct_review_count FROM cte_review
        UNION ALL
        SELECT company_name, 'Period 3' as review_period, Period3 as distinct_review_count FROM cte_review
        )
        , total AS (
        SELECT company_name,
            SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_type} ELSE NULL END) / SUM(CASE WHEN {ma_type} IS NOT NULL THEN {ma_records} ELSE NULL END) AS rank_value
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_6_9"]}
        WHERE
            {ma_records} IS NOT NULL
            AND case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {dimension_filter_str}
        GROUP BY company_name
        )
        SELECT
            a.*,
            r.distinct_review_count as review_count,
            rank_value
        FROM ranking a
        LEFT JOIN total b
            ON a.company_name = b.company_name
        LEFT JOIN review_records r
            ON  a.company_name = r.company_name
            AND r.review_period = a.period
            ORDER BY period
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING['VOE_6_9'],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        return_data.append({
            "company_name": row.company_name,
            "value": row.value,
            "period": row.period,
            "period_time": row.period_time,
            "rank": row.rank_value,
            "review": row.review_count
        })

    return return_data


def fetch_voe_12(payload):
    """
    Fetch data
    """
    result = []
    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = RATING_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, RATING_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
        WITH voe_data AS(
            SELECT
                company_name,
                daily_date AS date,
                {ma_type}/{ma_records} AS value
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                AND {ma_records} IS NOT NULL
                AND {ma_type} IS NOT NULL
                {company_filter_str}
        )
        SELECT
        *,
        (SELECT COUNT(DISTINCT parent_review_id)
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
        WHERE s.case_study_id = @case_study_id
            AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
            AND review_date <= @end_date
            AND dimension_config_name IS NOT NULL
            {company_filter_str}
        ) AS records
        FROM voe_data
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "date": row.date.isoformat(),
            "value": row.value,
            "records": row.records
        })

    return result


def fetch_voe_12_1(payload):
    """
    Fetch Average sentiment score per dimension data

    NOTE: This chart has the same data backend as VOE_12,
        only different at filter_sql level
    """
    result = []
    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = RATING_DAILY
        ma_records = RECORDS_DAILY
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, RATING_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
        SELECT
            company_name,
            SUM({ma_type})/SUM({ma_records}) AS value
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12_1"]}
        WHERE {ma_records} IS NOT NULL
            AND {ma_type} IS NOT NULL
            AND case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12_1"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "value": row.value
        })

    return result


def fetch_voe_12_2(payload):
    """
    Fetch Period trend sentiment score per dimension data

    NOTE: This chart has the same data backend as VOE_12,
        only different at filter_sql level
    """
    result = []
    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = RATING_DAILY
        ma_records = RECORDS_DAILY
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, RATING_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
    WITH day_diff AS (
        SELECT
            case_study_id,
            MIN(daily_date) AS min_date,
            MAX(daily_date) AS max_date,
            ROUND(DATE_DIFF(MAX(daily_date), MIN(daily_date), DAY)/3,0) AS days_diff
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12_2"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY case_study_id
    ), node AS (
        SELECT
            case_study_id,
            DATE_ADD(min_date, INTERVAL CAST(days_diff AS int64) DAY) AS node1,
            DATE_ADD(min_date, INTERVAL 2*CAST(days_diff AS int64) DAY) AS node2,
            max_date AS node3
        FROM day_diff
    ),
    ranking AS (
        SELECT
            company_name,
            CASE
                WHEN daily_date <= node1 THEN 'Period 1'
                WHEN daily_date > node1 AND daily_date <= node2 THEN 'Period 2'
                WHEN daily_date > node2 THEN 'Period 3'
                ELSE null
            END AS period,
            CASE
                WHEN DATE_DIFF(node2,node1,MONTH) <= 1 THEN
                    CASE
                    WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,@start_date),'-',FORMAT_DATE('%d/%m/%y' ,node1))
                    WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node1+1),'-',FORMAT_DATE('%d/%m/%y' ,node2))
                    WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%d/%m/%y' ,node2+1),'-',FORMAT_DATE('%d/%m/%y' ,node3))
                    ELSE null
                    END
                ELSE
                    CASE
                    WHEN daily_date <= node1 THEN CONCAT(FORMAT_DATE('%b %Y' ,@start_date),'-',FORMAT_DATE('%b %Y' ,node1))
                    WHEN daily_date > node1 AND daily_date <= node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node1+1),'-',FORMAT_DATE('%b %Y' ,node2))
                    WHEN daily_date > node2 THEN CONCAT(FORMAT_DATE('%b %Y' ,node2+1),'-',FORMAT_DATE('%b %Y' ,node3))
                    ELSE null
                    END
            END AS period_time,
            SUM({ma_type})/SUM({ma_records}) AS value
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12_2"]} a
        LEFT JOIN node b ON a.case_study_id = b.case_study_id
        WHERE
            {ma_type} IS NOT NULL
            AND a.case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name, period, period_time
    ),
    total AS (
        SELECT
            company_name,
            SUM({ma_type})/SUM({ma_records}) AS rank_value
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12_2"]}
        WHERE
            {ma_type} IS NOT NULL
            AND case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
    )
    SELECT
        ranking.*,
        total.rank_value
    FROM ranking
    LEFT JOIN total ON ranking.company_name = total.company_name
    ORDER BY period
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_12_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "period": row.period,
            "period_time": row.period_time,
            "value": row.value,
            "rank": row.rank_value
        })

    return result


def fetch_voe_1_2(payload):
    """
    Fetch Employee review analytics data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    filter_sql = f"""
        SELECT company_name,
            SUM(records) AS records,
            SUM(collected_review_count) AS collected_review_count,
            SUM(processed_review_count) AS processed_review_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_2"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY company_name
    """

    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_1_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "records": row.records,
            "collected_review_count": row.collected_review_count,
            "processed_review_count": row.processed_review_count
        })
    return result


def fetch_voe_4_2(payload):
    """
    Fetch Sentiment scores and customer reviews collected over time data
    """
    result = []

    ma_type = ''
    ma_records = ''
    ma_er = ''

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = payload['params']['ma_type'] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_er = ER_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_er = payload['params']['ma_type'].replace(MA_PREFIX, ER_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
        WITH voe_data AS(
            SELECT
                company_name,
                daily_date as date,
                {ma_type} AS sum_ss,
                {ma_records} AS sum_review_counts,
                {ma_er} AS collected_review_counts
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_4_2"]}
            WHERE
                {ma_records} IS NOT NULL
                AND case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
        )
        SELECT
            *,
            (
                SELECT
                    COUNT(DISTINCT review_id)
                FROM
                    {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name is not null
                    {company_filter_str}

            ) as records
        FROM
            voe_data
    """
    final_sql = construct_final_query(nlp_type='VOE',
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_4_2"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "date": row.date.isoformat(),
            "sum_ss": row.sum_ss,
            "sum_review_counts": row.sum_review_counts,
            "collected_review_counts": row.collected_review_counts,
            "records": row.records
        })
    return result


def fetch_voe_5(payload):
    """
    Fetch Employee reviews count per dimension data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload['params']:
        dimension_type_filter_str = "AND dimension_type IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload['params'][DIMENSION_TYPE_FILTER_KEY].split(',')))

    filter_sql = f"""
    WITH max_value AS (
        SELECT
            case_study_id,
            case_study_name,
            dimension_config_name,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            source_name,
            company_name,
            company_id,
            source_id,
            daily_date,
            dimension,
            label,
            single_terms,
            polarity,
            collected_review_count,
            topic_review_counts,
            row_number() over(
                PARTITION BY
                    case_study_id,
                    case_study_name,
                    dimension_config_name,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    source_name,
                    company_name,
                    company_id,
                    source_id,
                    daily_date,
                    dimension,
                    label,
                    polarity
                    ORDER BY
                    topic_review_counts
                    DESC
            ) AS row_topic_review_counts,
            sum_ss,
            row_number() over(
                PARTITION BY
                    case_study_id,
                    case_study_name,
                    dimension_config_name,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    source_name,
                    company_name,
                    company_id,
                    source_id,
                    daily_date,
                    dimension
                    ORDER BY
                    sum_ss
                    DESC
            ) AS row_sum_ss,
            sum_review_counts,
            row_number() over(
                PARTITION BY
                    case_study_id,
                    case_study_name,
                    dimension_config_name,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    source_name,
                    company_name,
                    company_id,
                    source_id,
                    daily_date,
                    dimension
                    ORDER BY
                    sum_review_counts
                    DESC
            ) AS row_sum_review_counts,
            records,
            row_number() over(
                PARTITION BY
                    case_study_id,
                    case_study_name,
                    dimension_config_name,
                    dimension_config_id,
                    nlp_type,
                    nlp_pack,
                    source_name,
                    company_name,
                    company_id,
                    source_id,
                    daily_date
                    ORDER BY
                    records
                    DESC
            ) AS row_records,
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING['VOE_5']}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {dimension_type_filter_str}
    )
    SELECT
        company_name,
        dimension,
        label,
        single_terms,
        polarity,
        sum(collected_review_count) AS collected_review_count,
        sum(CASE WHEN row_topic_review_counts = 1 THEN topic_review_counts ELSE 0 END) AS topic_review_counts,
        sum(CASE WHEN row_sum_ss = 1 THEN sum_ss ELSE null END) AS sum_ss,
        sum(CASE WHEN row_sum_review_counts = 1 THEN sum_review_counts ELSE 0 END) AS sum_review_counts,
        sum(CASE WHEN row_records = 1 THEN records ELSE 0 END) AS records,
		(SELECT SUM(CASE WHEN row_sum_ss = 1 THEN sum_ss ELSE null END)/SUM(CASE WHEN row_sum_review_counts = 1 THEN sum_review_counts ELSE 0 END)
		FROM max_value WHERE  dimension = m.dimension) AS dimension_avg_ss
    FROM max_value m
    GROUP BY
        company_name,
        dimension,
        label,
        single_terms,
        polarity
    """
    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING['VOE_5'],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "dimension": row.dimension,
            "label": row.label,
            "single_terms": row.single_terms,
            "polarity": row.polarity,
            "records": row.records,
            "sum_review_counts": row.sum_review_counts,
            "sum_ss": row.sum_ss,
            "collected_review_count": row.collected_review_count,
            "topic_review_counts": row.topic_review_counts,
            "dimension_avg_ss": row.dimension_avg_ss
        })
    return result


def fetch_voe_14(payload):
    """
    Fetch N vs Rating data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = RATING_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, RATING_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    filter_sql = f"""
    WITH cte AS (
      SELECT  *
      FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_14"]}
      WHERE case_study_id = @case_study_id
        AND daily_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
        AND daily_date <= @end_date
        {company_filter_str}
        {source_filter_str}
    ),
    all_combined AS (
      SELECT
        "Combined" as source_name,
        company_name,
        SUM({ma_type})/SUM({ma_records}) AS rating,
        (SELECT SUM(records) FROM cte WHERE company_name=a.company_name) AS review_count
      FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_14"]} a
      WHERE case_study_id = @case_study_id
        AND {ma_type} IS NOT NULL
        AND daily_date >= @start_date
        AND daily_date <= @end_date
        {company_filter_str}
        {source_filter_str}
      GROUP BY
      source_name,
      company_name
    ),
    voe_data AS (
    SELECT
        source_name,
        company_name,
        SUM({ma_type})/SUM({ma_records}) AS rating,
        (SELECT SUM(records) FROM cte WHERE company_name=b.company_name AND source_name= b.source_name ) AS review_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_14"]} b
        WHERE case_study_id = @case_study_id
            AND {ma_type} IS NOT NULL
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY
            source_name,
            company_name
        UNION ALL
            SELECT *
            FROM all_combined
    )
    SELECT
        *,
        (SELECT COUNT(DISTINCT parent_review_id)
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
        WHERE s.case_study_id = @case_study_id
            AND rating IS NOT NULL
            AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
            AND review_date <= @end_date
            AND dimension_config_name IS NOT NULL
            {company_filter_str}
            {source_filter_str}
        ) AS records
    FROM voe_data
    ;
    """

    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_14"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )

    for row in query_job:
        result.append({
            "source_name": row.source_name,
            "company_name": row.company_name,
            "rating": row.rating,
            "review_count": row.review_count,
            "records": row.records,
        })
    return result


def fetch_voe_15(payload):
    """
    Fetch N vs Rating over time data
    """
    result = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload['params'][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload['params'][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload['params']['end_date'])
    ]

    if payload['params']['ma_type'].lower() == MA_DAILY.lower():
        ma_type = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    else:
        ma_type = payload['params']['ma_type'].replace(MA_PREFIX, SS_MA)
        ma_records = payload['params']['ma_type'].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload['params']:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload['params'][SOURCE_NAME_FILTER_KEY].lower().split(',')))

    is_default = False
    if IS_DEFAULT_KEY in payload['params']:
        is_default = bool(payload['params'][SOURCE_NAME_FILTER_KEY])

    filter_sql = ""

    if is_default:
        filter_sql = f"""
        WITH cte AS (
        SELECT 
            case_study_id,
            company_name,
            source_name,
            daily_date,
            sum(records) OVER (PARTITION BY case_study_id,
            company_name,
            source_name
            ORDER BY daily_date ROWS BETWEEN  {ma_number[1]} PRECEDING AND CURRENT ROW) as review_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_15"]}
        WHERE case_study_id =@case_study_id
            {company_filter_str}
            {source_filter_str}
        ),
        data_check AS (
        SELECT 
            case_study_id, 
            daily_date, 
            COUNT(DISTINCT company_name) as company_count
        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_15"]}
        WHERE case_study_id =@case_study_id
        AND {ma_type} IS NOT NULL
            {company_filter_str}
            {source_filter_str}
        GROUP BY 
        case_study_id,
        daily_date
        ),
        default_date AS (
        SELECT DISTINCT 
            case_study_id,
            FIRST_VALUE(daily_date) OVER(ORDER BY company_count DESC, daily_date DESC) as max_date,
            FIRST_VALUE(daily_date) OVER(ORDER BY company_count DESC, daily_date ASC) as min_date
            FROM data_check 
        ),
        voe_data AS (
        SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        nlp_type,
        nlp_pack,
        company_name,
        daily_date,
        SUM({ma_type})/SUM({ma_records}) AS value,
        (SELECT SUM(review_count) FROM cte WHERE company_name=a.company_name AND daily_date= a.daily_date) as review_number
        

        FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_15"]} a
        WHERE case_study_id =@case_study_id
            AND {ma_type} IS NOT NULL
            AND {ma_records} IS NOT NULL
            AND (daily_date = (SELECT min_date FROM default_date) 
                OR daily_date = (SELECT max_date FROM default_date) )
            {company_filter_str}
            {source_filter_str}
        GROUP BY 
        case_study_id,
        case_study_name,
        dimension_config_name,
        nlp_type,
        nlp_pack,
        company_name,
        daily_date
        )
        SELECT
            *,
            (SELECT 
                (SELECT COUNT(DISTINCT review_id) 
                FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE 
                    d.case_study_id=case_study_id
                    AND ((review_date >= DATE_SUB(d.min_date,INTERVAL {ma_number[1]}  DAY) AND review_date <= d.min_date)
                        OR ( review_date >= DATE_SUB(d.max_date,INTERVAL {ma_number[1]}  DAY) AND review_date <= d.max_date))
                    AND dimension_config_name IS NOT NULL
                    {company_filter_str}
                    {source_filter_str})

            FROM default_date d ) AS records
        FROM voe_data
        ;
        """
    else:
        filter_sql = f"""
            WITH cte AS (
            SELECT 
                case_study_id,
                company_name,
                source_name,
                daily_date,
                sum(records) OVER (PARTITION BY case_study_id,
                company_name,
                source_name
                ORDER BY daily_date ROWS BETWEEN  {ma_number[1]} PRECEDING AND CURRENT ROW) as review_count
            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_15"]}
            WHERE case_study_id =@case_study_id
                {company_filter_str}
                {source_filter_str}
            ),
            voe_data AS (
            SELECT
            case_study_id,
            case_study_name,
            dimension_config_name,
            nlp_type,
            nlp_pack,
            company_name,
            daily_date,
            (SELECT SUM(review_count) FROM cte WHERE company_name=a.company_name AND daily_date= a.daily_date) as review_number,
            SUM({ma_type})/SUM({ma_records}) AS value

            FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_15"]} a
            WHERE case_study_id =@case_study_id
                AND {ma_type} IS NOT NULL
                AND {ma_records} IS NOT NULL
                AND daily_date IN ( @start_date ,@end_date)
                {company_filter_str}
                {source_filter_str}
            GROUP BY 
            case_study_id,
            case_study_name,
            dimension_config_name,
            nlp_type,
            nlp_pack,
            company_name,
            daily_date
            )
            SELECT
                case_study_id,
                case_study_name,
                dimension_config_name,
                nlp_type,
                nlp_pack,
                company_name,
                daily_date,
                value,
                review_number,
                (SELECT COUNT(DISTINCT review_id)
                FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id = @case_study_id
                    AND ((review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY) AND review_date <= @start_date)
                        OR ( review_date >= DATE_SUB(@end_date,INTERVAL {ma_number[1]} DAY) AND review_date <= @end_date))
                    AND dimension_config_name IS NOT NULL
                    {company_filter_str}
                    {source_filter_str}
                ) AS records
            FROM voe_data
            ;
        """

    final_sql = construct_final_query(nlp_type="VOE",
                                      chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_15"],
                                      filter_query=filter_sql,
                                      logger=logger)

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )

    for row in query_job:
        result.append({
            "company_name": row.company_name,
            "value": row.value,
            "daily_date": row.daily_date.isoformat(),
            "reviews": row.review_number,
            "records": row.records,
        })

    return result


def fetch_voe_16(payload):
    """
    Fetch KPC prevalence by company / sentiment data

    Filter:
      + Data source
      + Company
      + Dimension
      + Start Date
      + End Date
    """

    POSITIVE_LABEL = "Positive"
    NEGATIVE_LABEL = "Negative"

    result = []
    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = f"AND lower(company_name) IN UNNEST(@{COMPANY_NAME_FILTER_KEY})"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower(
                ).split(",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload["params"]:
        source_filter_str = f"AND lower(source_name) IN UNNEST(@{SOURCE_NAME_FILTER_KEY})"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                SOURCE_NAME_FILTER_KEY, "STRING", payload["params"][SOURCE_NAME_FILTER_KEY].lower(
                ).split(",")
            )
        )

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload['params']:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    for polarity_type in [POSITIVE_LABEL, NEGATIVE_LABEL]:
        polarity_type_filter_str = f"AND polarity_type= '{polarity_type}'"

        filter_sql = f"""
            WITH cte AS (
                SELECT *
                FROM  {VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_16"]}
                WHERE  case_study_id =  @case_study_id
                    {polarity_type_filter_str}
                    AND daily_date >=@start_date
                    AND daily_date <=@end_date
                    {company_filter_str}
                    {source_filter_str}
                    {dimension_filter_str}
            ),
            all_company AS (
                SELECT
                    polarity_type,
                    "All" as company_name,
                    dimension,
                    SUM( collected_review_count) as review_count,
                    (SELECT SUM( collected_review_count) FROM cte
                    WHERE  dimension= a.dimension)/(SELECT SUM( collected_review_count) FROM cte  ) as  review_percent,
                    NULL as all_company_dimension_percent
                FROM cte a
                GROUP BY
                    polarity_type,
                    company_name,
                    dimension
            ),
            voe_data as (
                SELECT
                    polarity_type,
                    company_name,
                    dimension,
                    SUM( collected_review_count) as review_count,
                    SUM( collected_review_count) / (SELECT SUM( collected_review_count) FROM cte
                    WHERE  company_name=b.company_name )as review_percent,
                    (SELECT SUM( collected_review_count) FROM cte
                    WHERE dimension= b.dimension)/(SELECT SUM( collected_review_count) FROM cte ) as all_company_dimension_percent

                FROM cte b
                GROUP BY
                    polarity_type,
                    company_name,
                    dimension
                UNION ALL
                SELECT *
                FROM all_company
            )
            SELECT
            *,
            (
                SELECT COUNT(DISTINCT review_id)
                FROM {VOE_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE
                    case_study_id =@case_study_id
                    AND review_date >= @start_date
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND polarity IN ('N', 'N+', 'P', 'P+')
                    AND is_used = True
                    AND dimension_type ='Signal'
                    {company_filter_str}
                    {source_filter_str}
                    {summary_table_dimension_filter_str}
            ) AS records
            FROM voe_data
        """
        final_sql = construct_final_query(nlp_type="VOE",
                                          chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_16"],
                                          filter_query=filter_sql,
                                          logger=logger)

        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters
        )
        query_job = client.query(
            final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
        )
        for row in query_job:
            result.append({
                "records": row.records,
                "company_name": row.company_name,
                "company_percent": row.review_percent,
                "review_count": row.review_count,
                "polarity_type": row.polarity_type,
                "dimension": row.dimension,
                "dimension_percent": row.all_company_dimension_percent,
                "polarity_type": row.polarity_type
            })

    return result


def fetch_voe_13(payload):
    """
    Fetch Source Profile Statistics Chart
    """
    results = []

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", payload["params"][END_DATE_FILTER_KEY])
    ]

    company_filter_str = ""
    if COMPANY_NAME_FILTER_KEY in payload["params"]:
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    source_filter_str = ""
    if SOURCE_NAME_FILTER_KEY in payload["params"]:
        source_filter_str = "AND lower(source_name) IN UNNEST(@source_names)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            SOURCE_NAME_FILTER_KEY, "STRING", payload["params"][SOURCE_NAME_FILTER_KEY].lower().split(",")))

    filter_sql = f"""
        WITH filtered_table AS (
            SELECT
                company_name,
                source_name,
                url,
                COUNT(parent_review_id) as reviews_filtered,
                AVG(rating) AS rating_filtered
            FROM distinct_reviews
            WHERE review_date >= @start_date
                AND review_date <= @end_date
                { company_filter_str }
                { source_filter_str }
            GROUP BY company_name, source_name, url
        )
        SELECT
            att.company_name,
            att.source_name,
            att.url,
            rst.total_reviews_stats,
            rst.total_ratings_stats,
            rst.average_rating_stats,
            att.reviews_all_time,
            att.rating_all_time,
            ft.reviews_filtered,
            ft.rating_filtered,
            (SELECT SUM(reviews_filtered) FROM filtered_table ) AS records
        FROM filtered_table ft
        LEFT JOIN all_time_table att
            ON att.company_name = ft.company_name
            AND att.source_name = ft.source_name
            AND att.url = ft.url
        LEFT JOIN review_stats_table rst
            ON att.company_name = rst.company_name
            AND att.source_name = rst.source_name
            AND att.url = rst.url
    """
    final_sql = construct_final_query(
        nlp_type='VOE',
        chart_code=VOE_CHART_DATA_MART_SCRIPT_MAPPING["VOE_13"],
        filter_query=filter_sql,
        logger=logger
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(
        final_sql, job_config=job_config, timeout=DEFAULT_QUERY_TIMEOUT
    )
    query_job.result()
    for row in query_job:
        results.append({
            "company_name": row.company_name,
            "source_name": row.source_name,
            "url": row.url,
            "total_reviews_stats": row.total_reviews_stats,
            "total_ratings_stats": row.total_ratings_stats,
            "average_rating_stats": row.average_rating_stats,
            "reviews_all_time": row.reviews_all_time,
            "rating_all_time": row.rating_all_time,
            "reviews_filtered": row.reviews_filtered,
            "rating_filtered": row.rating_filtered,
            "records": row.records
        })

    return results
