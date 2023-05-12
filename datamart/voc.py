import datetime

from google.cloud import bigquery
from . import AFTER_END_DATE_FILTER_KEY, AFTER_START_DATE_FILTER_KEY, BEFORE_END_DATE_FILTER_KEY, BEFORE_START_DATE_FILTER_KEY, CURRENT_END_DATE_FILTER_KEY, CURRENT_START_DATE_FILTER_KEY, MA_TYPES, MA_DAILY, RECORDS_DAILY, MA_PREFIX, CR_MA, RATING_DAILY, RATING_MA, RECORDS_MA, SS_DAILY, SS_MA, \
    CR_DAILY, CASE_STUDY_ID_FILTER_KEY, START_DATE_FILTER_KEY, END_DATE_FILTER_KEY, COMPANY_NAME_FILTER_KEY, \
    SOURCE_NAME_FILTER_KEY, DIMENSION_FILTER_KEY, DIMENSION_TYPE_FILTER_KEY, IS_DEFAULT_KEY
from . import client
from . import DEFAULT_QUERY_TIMEOUT
import config
import logger
import json

from helper import construct_final_query

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

PAGE = 1
PAGE_SIZE = 50
VOC_CHART_DATA_MART_TABLE_MAPPING = config.CHART_DATA_MART_TABLE_MAPPING["VOC"]
VOC_CHART_DATA_MART_SCRIPT_MAPPING = config.CHART_DATA_MART_SCRIPT_MAPPING["VOC"]

# daily, null value


def fetch_voc_1_1(payload):
    """
    Fetch Customer reviews count over time data
    """
    result = []

    ma_type = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, CR_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
        WITH voc_data AS(
            SELECT company_name,
                daily_date AS date,
                {ma_type} AS value
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_1"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                AND {ma_type} IS NOT NULL
        )
        SELECT
            *,
            (SELECT
                    COUNT(DISTINCT parent_review_id)
            FROM
                {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
            WHERE
                s.case_study_id = @case_study_id
                AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                {company_filter_str}
            ) AS records
        FROM
            voc_data
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_1"],
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
            "records": row.records,
            "value": row.value
        })
    return result


def fetch_voc_1_2(payload):
    """
    Fetch Reviews statistics data
    """
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
        SELECT company_name,
            SUM(records) AS records,
            SUM(collected_review_count) AS collected_review_count,
            SUM(processed_review_count) AS processed_review_count
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_2"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY company_name
    """

    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_2"],
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


def fetch_voc_1_3_1(payload):
    """
    Fetch Customer Reviews Collected by Players data
    """
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
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
        SELECT company_name,
            SUM(records) AS records,
            SUM(collected_review_count) AS collected_review_count
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_3_1"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_3_1"],
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
            "collected_review_count": row.collected_review_count
        })
    return result


def fetch_voc_1_3_2(payload):
    """
    Fetch Reviews Count by Players and Data Sources data
    """
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
        SELECT source_name,
            company_name,
            SUM(records) AS records
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_3_2"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
        GROUP BY
            source_name,
            company_name
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_3_2"],
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


def fetch_voc_1_4(payload):
    """
    Fetch Collected Customer Reviews Count by Language data
    """
    result = []

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_number = (1, 0)  # Daily type
    else:
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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

    language_filter_str = ""
    if 'language_codes' in payload["params"]:
        language_filter_str = "AND language_code IN UNNEST(@language_codes)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "language_codes", "STRING", payload["params"]['language_codes'].split(",")))

    filter_sql = f"""
        WITH voc_data AS(
            SELECT language_name,
                language_code,
                daily_date AS date,
                sum(records_daily) records_daily
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_4"]}
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
                        {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
                    WHERE
                        s.case_study_id = @case_study_id
                        AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                        AND review_date <= @end_date
                        AND dimension_config_name IS NOT NULL
                        {company_filter_str}
                        {language_filter_str}
                ) AS records
            FROM voc_data
        )
        SELECT CONCAT('[', string_agg(to_json_string(t)),']') AS value
        FROM foo t
        WHERE t.value IS NOT NULL
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_4"],
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


def fetch_voc_1_5(payload):
    """
    Fetch Collected Customer Reviews Count by Language data
    """
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
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    language_filter_str = ""
    if 'language_codes' in payload["params"]:
        language_filter_str = "AND language_code IN UNNEST(@language_codes)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "language_codes", "STRING", payload["params"]['language_codes'].split(",")))

    filter_sql = f"""
        WITH voc_data AS (
            SELECT language_code,
                language_name,
                SUM(collected_review_count) AS collected_review_count
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_5"]}
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
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE
                    st.case_study_id = @case_study_id AND review_date >= @start_date AND review_date <= @end_date
                    AND dimension_config_name is NOT NULL
                    AND st.language_code = voc_data.language_code
                    AND st.language = voc_data.language_name
                    {company_filter_str}
                    {language_filter_str}
            ) AS records
        FROM
            voc_data
        )
        SELECT CONCAT('[', string_agg(to_json_string(t)),']') AS value
        FROM foo t
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_5"],
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


def fetch_voc_1_6(payload):
    """
    Fetch Reviews Count by Data Source data
    """
    result = []

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_number = (1, 0)  # Daily type
    else:
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
        WITH voc_data AS (
            SELECT
                source_name,
                daily_date AS date,
                sum(records_daily) AS records_daily
            FROM
                {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_6"]}
            WHERE
                case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
            GROUP BY source_name, daily_date
        ), foo AS (
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
                    FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
                    WHERE
                        s.case_study_id = @case_study_id
                        AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                        AND review_date <= @end_date
                        AND dimension_config_name IS NOT NULL
                        {company_filter_str}
                        {source_filter_str}
                ) AS records
            FROM voc_data
        )
        SELECT * FROM foo WHERE value IS NOT NULL
    """

    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_6"],
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


def fetch_voc_1_7(payload):
    """
    Fetch Reviews collected and processed successfully data
    """
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_7"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_7"],
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


def fetch_voc_1_8(payload):
    """
    Fetch Average customer review data

    NOTE: This chart has the same data backend as VOC_1_1,
        only different at filter_sql level
    """
    result = []

    ma_type = ''
    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = RECORDS_DAILY
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, CR_MA)

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

    # NOTE: TPPLEO-2526 change
    # - change to only for daily `ma_type`
    # - if value is null count as 0 instead of being removed from average
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_8"]}
        WHERE 
            case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_8"],
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


def fetch_voc_1_9(payload):
    """
    Fetch Yearly average customer reviews data

    NOTE: This chart has the same data backend as VOC_1_1,
        only different at filter_sql level
    """
    start_date = payload["params"][START_DATE_FILTER_KEY]
    end_date = payload["params"][END_DATE_FILTER_KEY]

    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    datediff = abs((start_datetime - end_datetime).days)
    if datediff < 3:
        return []

    result = []

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, CR_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
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

    # NOTE: TPPLEO-2526 change
    # - change to only for daily `ma_type`
    # - if value is null count as 0 instead of being removed from average
    filter_sql = f"""
        WITH day_diff AS (
            SELECT
                case_study_id,
                MIN(daily_date) AS min_date,
                MAX(daily_date) AS max_date,
                ROUND(DATE_DIFF(MAX(daily_date), MIN(daily_date), DAY)/3,0) AS days_diff
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_9"]}
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
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_9"]} a
            LEFT JOIN node b ON a.case_study_id = b.case_study_id
            WHERE
                a.case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
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
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_9"]}
            WHERE
                case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
            GROUP BY company_name
        )
        SELECT ranking.*, total.rank_value
        FROM ranking
        LEFT JOIN total ON ranking.company_name = total.company_name
        ORDER BY period
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_9"],
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


def fetch_voc_2(payload):
    """
    Fetch Reviews distribution per polarity data
    """
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
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    filter_sql = f"""
        WITH voc_data AS(
            SELECT company_name, polarity , sum(collected_review_count) collected_review_count
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_2"]}
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
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE case_study_id = @case_study_id
                    AND review_date >= @start_date
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    {company_filter_str}
            ) AS records
        FROM
            voc_data
        ORDER BY polarity DESC, company_name
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_2"],
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


def fetch_voc_3(payload):
    """
    Fetch Customer review ratings data
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
        WITH voc_data AS(
            SELECT
            source_name,
            daily_date as date,
            {ma_type} as sum_rating,
            {ma_records} as sum_review_count
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_3"]}
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
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE
                    st.case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name is not null
                    {company_filter_str}
                    {source_filter_str}
            ) as records
        FROM voc_data
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_3"],
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


def fetch_voc_4_1(payload):
    """
    Fetch Sentiment scores by players over time data
    """
    result = []

    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
        WITH voc_data AS(
            SELECT
                company_name,
                daily_date AS date,
                {ma_type}/{ma_records} AS value
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_1"]}
            WHERE
                {ma_records} IS NOT NULL
                AND {ma_type} IS NOT NULL
                AND case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
        )
        SELECT *,
            (
                SELECT
                    COUNT(DISTINCT review_id)
                FROM
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND dimension IS NOT NULL
                    {company_filter_str}
            ) AS records
        FROM voc_data
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_1"],
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
            "records": row.records,
            "value": row.value
        })
    return result


def fetch_voc_4_2(payload):
    """
    Fetch Sentiment scores and customer reviews collected over time data
    """
    result = []

    ma_type = ''
    ma_records = ''
    ma_cr = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_cr = CR_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_cr = payload["params"]["ma_type"].replace(MA_PREFIX, CR_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
        WITH voc_data AS(
            SELECT
                company_name,
                daily_date as date,
                {ma_type} AS sum_ss,
                {ma_records} AS sum_review_counts,
                {ma_cr} AS collected_review_counts
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_2"]}
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
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    {company_filter_str}
            ) as records
        FROM voc_data
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_2"],
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


def fetch_voc_4_3(payload):
    """
    Fetch average sentiment score data

    NOTE: this chart use the same data backend script as VOC_4_1
    """
    result = []
    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = SS_DAILY
        ma_records = RECORDS_DAILY
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_number = (1, 0)  # Daily type
    else:
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
        WITH cte_review AS (
        SELECT
            company_name,
            COUNT(DISTINCT review_id) as distinct_review_count
        FROM  {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
        WHERE
            case_study_id = @case_study_id
            AND dimension_config_name IS NOT NULL
            AND is_used = True
            AND polarity IN ('N', 'N+', 'NEU', 'P', 'P+')
            AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
            AND review_date <= @end_date
            {company_filter_str}

        GROUP BY company_name)
        SELECT
            a.company_name,
            SUM({ma_type}) / SUM({ma_records}) AS value,
            (SELECT distinct_review_count FROM cte_review
            WHERE company_name = a.company_name) AS review_count
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_3"]} a
        WHERE
            {ma_records} IS NOT NULL
            AND case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
        """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_3"],
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
            "review": row.review_count
        })

    return result


def fetch_voc_4_4(payload):
    """
    Fetch Yearly average sentiment score data

    NOTE: this chart use the same data backend script as VOC_4_1
    """
    start_date = payload["params"][START_DATE_FILTER_KEY]
    end_date = payload["params"][END_DATE_FILTER_KEY]

    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    datediff = abs((start_datetime - end_datetime).days)
    if datediff < 3:
        return []

    result = []

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
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

    filter_sql = f"""
        WITH day_diff AS (
        SELECT
            case_study_id,
            MIN(daily_date) AS min_date,
            MAX(daily_date) AS max_date,
            ROUND(DATE_DIFF(MAX(daily_date), MIN(daily_date), DAY)/3,0) AS days_diff
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_4"]}
        WHERE case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY case_study_id
        )
        , node AS (
        SELECT
            case_study_id,
            DATE_ADD(min_date, INTERVAL CAST(days_diff AS int64) DAY) AS node1,
            DATE_ADD(min_date, INTERVAL 2*CAST(days_diff AS int64) DAY) AS node2,
            max_date AS node3
        FROM day_diff)
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
            SUM({ma_type}) / SUM({ma_records}) AS value
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_4"]} a
            LEFT JOIN node b
            ON a.case_study_id = b.case_study_id
        WHERE
            {ma_records} IS NOT NULL
            AND a.case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name, period,period_time
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} a
            LEFT JOIN node b
            ON a.case_study_id = b.case_study_id
        WHERE
            a.case_study_id = @case_study_id
            AND dimension_config_name is not null
            AND polarity  IN ('N', 'N+', 'NEU', 'P', 'P+')
            AND is_used = true
            {company_filter_str}

        GROUP BY company_name
        )
        ,review_records AS (
        SELECT company_name, 'Period 1' as review_period, Period1 as distinct_review_count FROM cte_review
        UNION ALL
        SELECT company_name, 'Period 2' as review_period, Period2 as distinct_review_count FROM cte_review
        UNION ALL
        SELECT company_name, 'Period 3' as review_period, Period3 as distinct_review_count FROM cte_review)
        , total AS (
        SELECT
            company_name,
            SUM({ma_type}) / SUM({ma_records}) AS rank_value
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_4"]}
        WHERE
            {ma_records} IS NOT NULL
            AND case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY
            company_name
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
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_4_4"],
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


def fetch_voc_6_1(payload):
    """
    Fetch Dimension sentiment score per company data
    """
    result = []

    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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

    dimension_filter_str = ""
    summary_table_dimension_filter_str = ""
    if DIMENSION_FILTER_KEY in payload["params"]:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    filter_sql = f"""
    WITH voc_data AS(
        SELECT
            company_name,
            daily_date AS date,
            {ma_type} AS sum_ss,
            {ma_records} AS sum_review_counts
        FROM
            {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_1"]}
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
                {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
            WHERE
                case_study_id = @case_study_id
                AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
				AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                {company_filter_str}
                {summary_table_dimension_filter_str}
        ) AS records
    FROM
        voc_data
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_1"],
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


def fetch_voc_6_2(payload):
    """
    Fetch Dimensions mentioned in reviews data
    """
    NUMBER_TOP_DIMENSION = 6
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
    limitation_filter_str = ""
    summary_table_dimension_filter_str = ""
    if 'dimension' in payload["params"]:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))
    else:
        limitation_filter_str = f"LIMIT {NUMBER_TOP_DIMENSION}"

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload["params"]:
        dimension_type_filter_str = "AND dimension_type IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload["params"][DIMENSION_TYPE_FILTER_KEY].split(",")))

    filter_sql = f"""
    WITH voc_data AS (
        SELECT
            dimension, SUM(collected_review_count) AS collected_review_count
        FROM
            {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_2"]}
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
            {limitation_filter_str}
    )
    SELECT
        *,
        (
            SELECT
                COUNT(DISTINCT review_id)
            FROM
                {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE
                case_study_id = @case_study_id
                AND review_date >= @start_date AND review_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {summary_table_dimension_filter_str}
                {dimension_type_filter_str}
                AND dimension_config_name IS NOT NULL
        ) AS records
    FROM
        voc_data
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_2"],
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


def fetch_voc_6_3(payload):
    """
    Fetch Dimensions mentioned: Positive and Negative polarity data
    """
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
    if 'dimension' in payload["params"]:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload["params"]:
        dimension_type_filter_str = "AND dimension_type IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload["params"][DIMENSION_TYPE_FILTER_KEY].split(",")))

    filter_sql = f"""
        WITH voc_data AS (
            SELECT dimension, polarity_type, SUM(collected_review_count) AS collected_review_count
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_3"]}
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
                FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id = @case_study_id AND review_date >= @start_date AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND polarity IN ('N','N+','NEU','P','P+')
                    AND is_used = True
                    {company_filter_str}
                    {source_filter_str}
                    {summary_table_dimension_filter_str}
                    {dimension_type_filter_str}
            ) AS records
        FROM voc_data
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_3"],
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


def fetch_voc_6_4(payload):
    """
    Fetch Sentiment scores ranking per dimension data
    """
    NUMBER_TOP_DIMENSION = 9
    result = []
    top_dimension_filter_str = ""

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
    if 'dimension' in payload["params"]:
        dimension_filter_str = "AND dimension IN UNNEST(@dimension)"
        summary_table_dimension_filter_str = "AND modified_dimension IN UNNEST(@dimension)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload["params"]:
        dimension_type_filter_str = "AND dimension_type IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload["params"][DIMENSION_TYPE_FILTER_KEY].split(",")))

    filter_sql = f"""
    WITH  cte_review AS (
    SELECT
        company_name,
        modified_dimension,
        COUNT(DISTINCT review_id) as distinct_review_count
    FROM  {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
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
    ,voc_data AS (
    SELECT
        dimension,
        company_name,
        CASE
            WHEN SUM(sum_ss) IS NULL OR SUM(sum_review_count) IS NULL THEN NULL
            ELSE ROUND(SUM(sum_ss) / SUM(sum_review_count), 2)
        END AS sentiment_score,
        (SELECT distinct_review_count FROM cte_review
        WHERE company_name = a.company_name
        AND modified_dimension = a.dimension
        ) AS review_count,
        (SELECT COUNT(DISTINCT review_id)
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
        WHERE case_study_id = @case_study_id
            AND review_date >= @start_date
            AND review_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {summary_table_dimension_filter_str}
            {dimension_type_filter_str}
            AND modified_dimension = a.dimension
        ) AS collected_review_count
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_4"]} a
        WHERE case_study_id =  @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
            {source_filter_str}
            {dimension_filter_str}
            {dimension_type_filter_str}
        GROUP BY dimension, company_name
    )
    ,final AS (
        SELECT *,
            (SELECT COUNT(DISTINCT review_id)
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
            WHERE case_study_id =  @case_study_id
            AND dimension_config_name IS NOT NULL
            AND dimension IS NOT NULL
            AND is_used = true
            AND review_date >= @start_date
            AND review_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {summary_table_dimension_filter_str}
                {dimension_type_filter_str}
            ) AS records,
            dense_rank() over (order by collected_review_count desc) AS rank
        FROM voc_data
    )
    SELECT * FROM final
    WHERE rank <= {NUMBER_TOP_DIMENSION}
    ORDER BY collected_review_count DESC, sentiment_score DESC
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_4"],
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
            "company_name": row.company_name,
            "sentiment_score": row.sentiment_score,
            "records": row.records,
            "review": row.review_count
        })

    return result


def fetch_voc_6_5(payload):
    """
    Fetch Competitors overlap analysis data
    """
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
    WITH voc_data AS (
        SELECT company_name, competitor,
            SUM(processed_review_count) AS sum_processed_review_count,
            SUM(processed_review_mention) AS sum_processed_review_mention,
            SUM(sum_mentioned) AS sum_mentioned
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_5"]}
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
        WHERE case_study_id = @case_study_id
            AND review_date >= @start_date
            AND review_date <= @end_date
            {company_filter_str}
            {source_filter_str}
    ) AS records
    FROM voc_data
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_5"],
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


def fetch_voc_6_6(payload):
    """
    Fetch Sentiment score by data source per company data
    """
    result = []

    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
        WITH voc_data AS (
            SELECT source_name,
                daily_date as date,
                {ma_type} AS sum_ss,
                {ma_records} AS sum_review_counts
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_6"]}
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
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} st
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name is not null
                    AND dimension is not null
                    {company_filter_str}
                    {source_filter_str}
            ) AS records
        FROM voc_data
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_6"],
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


def fetch_voc_6_7(payload):
    """
    Fetch Most common terms data
    """
    return_data = []
    page_size = None
    start_index = 0

    if 'page' in payload["params"] and 'page_size' in payload["params"]:
        page = int(payload["params"]['page'])
        page_size = int(payload["params"]['page_size'])

        start_index = page_size * (page - 1)

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

    polarity_filter_str = ""
    if 'polarities' in payload["params"]:
        polarity_filter_str = "AND polarity IN UNNEST(@polarities)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "polarities", "STRING", payload["params"]['polarities'].split(",")))

    filter_sql = f"""
        WITH voc_data AS (
            SELECT single_terms, polarity, SUM(collected_review_count) AS count, company_name
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_7"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {polarity_filter_str}
            GROUP BY single_terms, polarity, company_name
            ORDER BY count DESC, single_terms DESC, polarity DESC
        )
        SELECT *,
            (SELECT COUNT(DISTINCT review_id)
                FROM
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= @start_date
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND dimension IS NOT NULL
                    AND is_used = true
                    {company_filter_str}
                    {source_filter_str}
                    {polarity_filter_str}
            ) AS records
        FROM voc_data
    """

    # construct full query to send to BQ
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_7"],
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


def fetch_voc_6_7_1(payload):
    """
    Fetch Most common terms data
    """
    return_data = []
    page_size = None
    start_index = 0

    if 'page' in payload["params"] and 'page_size' in payload["params"]:
        page = int(payload["params"]['page'])
        page_size = int(payload["params"]['page_size'])

        start_index = page_size * (page - 1)

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

    polarity_filter_str = ""
    if 'polarities' in payload["params"]:
        polarity_filter_str = "AND polarity IN UNNEST(@polarities)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "polarities", "STRING", payload["params"]['polarities'].split(",")))

    filter_sql = f"""
        WITH voc_data AS (
            SELECT single_terms, polarity, SUM(collected_review_count) AS count, company_name
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_7_1"]}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                AND single_terms != ''
                AND single_terms IS NOT NULL
                {company_filter_str}
                {source_filter_str}
                {polarity_filter_str}
            GROUP BY single_terms, polarity, company_name
            ORDER BY count DESC, single_terms DESC, polarity DESC
        )
        SELECT *,
            (SELECT COUNT(DISTINCT review_id)
                FROM
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= @start_date
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND dimension IS NOT NULL
                    AND is_used = true
                    {company_filter_str}
                    {source_filter_str}
                    {polarity_filter_str}
            ) AS records
        FROM voc_data
    """

    # construct full query to send to BQ
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_6_7_1"],
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


def fetch_voc_6_8(payload):
    """
    Fetch  Average sentiment score per dimension data

    NOTE: This chart has the same data backend as 6_1,
        but with different filter query attached to the end
    """
    return_data = []

    ma_type = ''
    ma_records = ''
    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
    FROM  {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
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
        SUM({ma_type}) / SUM({ma_records}) AS value,
        (SELECT distinct_review_count FROM cte_review
        WHERE company_name = a.company_name) AS review_count
    FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_6_8']} a
    WHERE
        {ma_records} IS NOT NULL
        AND case_study_id = @case_study_id
        AND daily_date >= @start_date
        AND daily_date <= @end_date
        {company_filter_str}
        {dimension_filter_str}
    GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_6_8'],
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


def fetch_voc_6_9(payload):
    """
    Fetch Period trend sentiment score per dimension data

    NOTE: This chart has the same data backend as 6_1,
        but with different filter query attached to the end
    """
    start_date = payload["params"][START_DATE_FILTER_KEY]
    end_date = payload["params"][END_DATE_FILTER_KEY]

    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    datediff = abs((start_datetime - end_datetime).days)
    if datediff < 3:
        return []

    return_data = []

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_6_9']}
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
            SUM({ma_type}) / SUM({ma_records}) AS value
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_6_9']} a
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} a
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
        SELECT company_name, 'Period 3' as review_period, Period3 as distinct_review_count FROM cte_review)
        , total AS (
        SELECT company_name,
            SUM({ma_type}) / SUM({ma_records}) AS rank_value
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_6_9']}
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
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_6_9'],
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


def fetch_voc_5(payload):
    """
    Fetch Customer reviews count per dimension data
    """
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

    dimension_type_filter_str = ""
    if DIMENSION_TYPE_FILTER_KEY in payload["params"]:
        dimension_type_filter_str = "AND dimension_type IN UNNEST(@dimension_type)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_TYPE_FILTER_KEY, "STRING", payload["params"][DIMENSION_TYPE_FILTER_KEY].split(",")))

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
                        ORDER BY topic_review_counts DESC
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
                        ORDER BY sum_ss DESC
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
                        ORDER BY sum_review_counts DESC
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
                        ORDER BY records DESC
                ) AS row_records,
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_5']}
            WHERE case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
                {source_filter_str}
                {dimension_type_filter_str}
        ),
        dimension_average_ss AS (
            SELECT 
                dimension,
                SUM(
                    CASE WHEN row_sum_ss = 1 THEN sum_ss 
                    ELSE null 
                    END
                ) / SUM(
                    CASE WHEN row_sum_review_counts = 1 THEN sum_review_counts 
                    ELSE 0 
                    END
                ) AS dimension_avg_ss 
            FROM max_value 
            GROUP BY
                dimension
        ),
        dimension_review_count AS (
            SELECT
                company_name,
                dimension,
                label,
                single_terms,
                polarity,
                SUM(collected_review_count) AS collected_review_count,
                SUM(CASE WHEN row_topic_review_counts = 1 THEN topic_review_counts ELSE 0 END) AS topic_review_counts,
                SUM(CASE WHEN row_sum_ss = 1 THEN sum_ss ELSE null END) AS sum_ss,
                SUM(CASE WHEN row_sum_review_counts = 1 THEN sum_review_counts ELSE 0 END) AS sum_review_counts,
                SUM(CASE WHEN row_records = 1 THEN records ELSE 0 END) AS records,
            FROM max_value m
            GROUP BY
                company_name,
                dimension,
                label,
                single_terms,
                polarity
        )
        SELECT 
            l.company_name,
            l.dimension,
            l.label,
            l.single_terms,
            l.polarity,
            l.collected_review_count,
            l.topic_review_counts,
            l.sum_ss,
            l.sum_review_counts,
            l.records,
            r.dimension_avg_ss
        FROM dimension_review_count l
        INNER JOIN dimension_average_ss r
        ON l.dimension = r.dimension
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING['VOC_5'],
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


def fetch_voc_12(payload):
    """
    Fetch VoC Sentiment Causal Mining data
    """
    result = []
    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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
        WITH voc_data AS (
            SELECT
                company_name,
                daily_date AS date,
                {ma_type}/{ma_records} AS value
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_12"]}
            WHERE
                {ma_records} IS NOT NULL
                AND {ma_type} IS NOT NULL
                AND case_study_id = @case_study_id
                AND daily_date >= @start_date
                AND daily_date <= @end_date
                {company_filter_str}
        )
        SELECT *,
            (
                SELECT
                    COUNT(DISTINCT review_id)
                FROM
                    {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE
                    case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND dimension IS NOT NULL
                    {company_filter_str}
            ) AS records
        FROM
            voc_data
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_12"],
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


def fetch_voc_13(payload):
    """
    Fetch Self-reported customer scores by players over time data
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
        WITH voc_data AS(
            SELECT
                company_name,
                daily_date AS date,
                {ma_type}/{ma_records} AS value
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13"]}
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
        WHERE s.case_study_id = @case_study_id
            AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
            AND review_date <= @end_date
            AND dimension_config_name IS NOT NULL
            {company_filter_str}
        ) AS records
        FROM voc_data
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13"],
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
            "records": row.records,
            "value": row.value
        })

    return result


def fetch_voc_13_1(payload):
    """
    Fetch Average sentiment score per dimension data

    NOTE: This chart has the same data backend as VOC_13,
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13_1"]}
        WHERE {ma_records} IS NOT NULL
            AND {ma_type} IS NOT NULL
            AND case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            {company_filter_str}
        GROUP BY company_name
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13_1"],
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


def fetch_voc_13_2(payload):
    """
    Fetch Period trend sentiment score per dimension data

    NOTE: This chart has the same data backend as VOC_13,
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13_2"]}
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13_2"]} a
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
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13_2"]}
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
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_13_2"],
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


def fetch_voc_14(payload):
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
          FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_14"]}
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
            SUM({ma_type})/SUM({ma_records}) as rating,
            (SELECT SUM(records) FROM cte WHERE company_name=a.company_name) as review_count
          FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_14"]} a
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
        voc_data AS (
        SELECT
            source_name,
            company_name,
            SUM({ma_type})/SUM({ma_records}) as rating,
            (SELECT SUM(records) FROM cte WHERE company_name=b.company_name AND source_name= b.source_name ) as review_count
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_14"]} b
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
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]} s
            WHERE s.case_study_id = @case_study_id
                    AND rating IS NOT NULL
                AND review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
                AND review_date <= @end_date
                AND dimension_config_name IS NOT NULL
                {company_filter_str}
                {source_filter_str}
            ) AS records
        FROM voc_data
        ;
    """

    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_14"],
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


def fetch_voc_15(payload):
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
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_15"]}
            WHERE case_study_id =@case_study_id
                {company_filter_str}
                {source_filter_str}
            ),
            data_check AS (
            SELECT 
                case_study_id, 
                daily_date, 
                COUNT(DISTINCT company_name) as company_count
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_15"]}
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
            voc_data AS (
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
            

            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_15"]} a
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
                    FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                    WHERE 
                        d.case_study_id=case_study_id
                        AND ((review_date >= DATE_SUB(d.min_date,INTERVAL {ma_number[1]}  DAY) AND review_date <= d.min_date)
                            OR ( review_date >= DATE_SUB(d.max_date,INTERVAL {ma_number[1]}  DAY) AND review_date <= d.max_date))
                        AND dimension_config_name IS NOT NULL
                        {company_filter_str}
                        {source_filter_str})

                FROM default_date d ) AS records
            FROM voc_data
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
            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_15"]}
            WHERE case_study_id =@case_study_id
                {company_filter_str}
                {source_filter_str}
            ),
            voc_data AS (
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

            FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_15"]} a
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
                FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id = @case_study_id
                    AND ((review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY) AND review_date <= @start_date)
                        OR ( review_date >= DATE_SUB(@end_date,INTERVAL {ma_number[1]} DAY) AND review_date <= @end_date))
                    AND dimension_config_name IS NOT NULL
                    {company_filter_str}
                    {source_filter_str}
                ) AS records
            FROM voc_data
            ;
        """

    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_15"],
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


def fetch_voc_16(payload):
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
    if DIMENSION_FILTER_KEY in payload["params"]:
        dimension_filter_str = f"AND dimension IN UNNEST(@{DIMENSION_FILTER_KEY})"
        summary_table_dimension_filter_str = f"AND modified_dimension IN UNNEST(@{DIMENSION_FILTER_KEY})"
        query_parameters.append(bigquery.ArrayQueryParameter(
            DIMENSION_FILTER_KEY, "STRING", json.loads(payload["params"][DIMENSION_FILTER_KEY])))

    for polarity_type in [POSITIVE_LABEL, NEGATIVE_LABEL]:
        polarity_type_filter_str = f"AND polarity_type= '{polarity_type}'"

        filter_sql = f"""
            WITH cte AS (
                SELECT *
                FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_16"]}
                WHERE case_study_id = @case_study_id
                    {polarity_type_filter_str}
                    AND daily_date >=@start_date
                    AND daily_date <=@end_date
                    {company_filter_str}
                    {source_filter_str}
                    {dimension_filter_str}
            ),
            all_company AS(
                SELECT polarity_type,
                    "All" AS company_name,
                    dimension,
                    SUM(collected_review_count) AS review_count,
                    (
                        SELECT SUM(collected_review_count)
                        FROM cte
                        WHERE dimension= a.dimension)/(SELECT SUM(collected_review_count)
                        FROM cte
                    ) AS review_percent,
                    NULL AS all_company_dimension_percent
                FROM cte a
                GROUP BY polarity_type,
                        company_name,
                        dimension),
            voc_data AS (
                SELECT polarity_type,
                    company_name,
                    dimension,
                    SUM(collected_review_count) AS review_count,
                    SUM(collected_review_count) /
                    (
                        SELECT SUM(collected_review_count)
                        FROM cte
                        WHERE company_name=b.company_name
                    ) AS review_percent,
                    (
                        SELECT SUM(collected_review_count)
                        FROM cte
                        WHERE dimension= b.dimension)/
                    (SELECT SUM(collected_review_count) FROM cte) AS all_company_dimension_percent
                FROM cte b
                GROUP BY polarity_type,
                        company_name,
                        dimension
                UNION ALL SELECT *
                FROM all_company)
            SELECT *,
            (
                SELECT COUNT(DISTINCT review_id)
                FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"]}
                WHERE case_study_id =@case_study_id
                    AND review_date >= @start_date
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    AND polarity IN ('N', 'N+', 'P', 'P+')
                    AND is_used = TRUE
                    AND dimension_type = 'KPC'
                    {company_filter_str}
                    {source_filter_str}
                    {summary_table_dimension_filter_str}
            ) AS records
            FROM voc_data
        """
        final_sql = construct_final_query(
            nlp_type="VOC",
            chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_16"],
            filter_query=filter_sql, logger=logger
        )

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
                "dimension_percent": row.all_company_dimension_percent
            })

    return result


def fetch_voc_12_1(payload):
    """
    Fetch Sentiment Causal Mining - Sentiment by dimension data
    """
    result = []
    ma_type = ''
    ma_records = ''

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_type = payload["params"]["ma_type"] = SS_DAILY
        ma_records = RECORDS_DAILY
        # ma_number = (1, 0)  # Daily type
    else:
        ma_type = payload["params"]["ma_type"].replace(MA_PREFIX, SS_MA)
        ma_records = payload["params"]["ma_type"].replace(
            MA_PREFIX, RECORDS_MA)
        # ma_number = MA_TYPES[payload["params"]["ma_type"]]

    # all end_date - 1 to make sure data on this section end_date not duplicate with the next section start_date
    end_date = payload["params"][END_DATE_FILTER_KEY]
    before_end_date = payload["params"].get(BEFORE_END_DATE_FILTER_KEY, end_date)
    after_end_date = payload["params"].get(AFTER_END_DATE_FILTER_KEY, end_date)
    section = payload["params"].get("section")

    if section:
        end_date = (
            datetime.datetime.strptime(end_date, "%Y-%m-%d")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

    if section:
        before_end_date = (
            datetime.datetime.strptime(before_end_date, "%Y-%m-%d")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

    if section:
        after_end_date = (
            datetime.datetime.strptime(after_end_date, "%Y-%m-%d")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            CURRENT_START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            CURRENT_END_DATE_FILTER_KEY, "DATE", end_date),
        bigquery.ScalarQueryParameter(
            BEFORE_START_DATE_FILTER_KEY, "DATE", payload["params"].get(BEFORE_START_DATE_FILTER_KEY, payload["params"][START_DATE_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            BEFORE_END_DATE_FILTER_KEY, "DATE", before_end_date),
        bigquery.ScalarQueryParameter(
            AFTER_START_DATE_FILTER_KEY, "DATE", payload["params"].get(AFTER_START_DATE_FILTER_KEY, payload["params"][START_DATE_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            AFTER_END_DATE_FILTER_KEY, "DATE", after_end_date),
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
        WITH cte AS (
        SELECT
            company_name,
            dimension,
            SUM(CASE WHEN daily_date >= @current_start_date AND daily_date <= @current_end_date THEN {ma_type} ELSE NULL END )/
            SUM(CASE WHEN daily_date >= @current_start_date AND daily_date <= @current_end_date THEN {ma_records} ELSE NULL END) 
            AS current_value,
            CASE WHEN @current_start_date=@before_start_date 
            THEN
                NULL
            ELSE 
                SUM(CASE WHEN daily_date >= @before_start_date AND daily_date <= @before_end_date  THEN {ma_type} ELSE NULL END )/
                SUM(CASE WHEN daily_date >= @before_start_date AND daily_date <= @before_end_date THEN {ma_records} ELSE NULL END) 
            END AS before_value,
            CASE WHEN @current_end_date= @after_end_date 
            THEN
                NULL
            ELSE
                SUM(CASE WHEN daily_date >= @after_start_date  AND daily_date <= @after_end_date THEN {ma_type} ELSE NULL END )/
                SUM(CASE WHEN daily_date >= @after_start_date AND daily_date <= @after_end_date THEN {ma_records} ELSE NULL END) 
            END AS after_value
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_12_1"]}
        WHERE
            {ma_records} IS NOT NULL
            AND {ma_type} IS NOT NULL
            AND case_study_id = @case_study_id
            {company_filter_str}
        GROUP BY company_name,dimension
        )
        SELECT * 
        FROM cte 
        WHERE current_value IS NOT NULL
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_12_1"],
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
            "name": row.dimension,
            "value": row.current_value,
            "before_value": row.before_value,
            "after_value": row.after_value
        })

    return result


def fetch_voc_12_2(payload):
    """
    Fetch Sentiment Causal Mining - Polarity
    """
    result = []

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_number = (1, 0)  # Daily type
    else:
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

    # end_date - 1 to make sure data on this section end_date not duplicate with the next section start_date
    end_date = payload["params"][END_DATE_FILTER_KEY]
    section = payload["params"].get("section")

    if section:
        end_date = (
            datetime.datetime.strptime(end_date, "%Y-%m-%d")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

    query_parameters = [
        bigquery.ScalarQueryParameter(CASE_STUDY_ID_FILTER_KEY, "INTEGER", int(
            payload["params"][CASE_STUDY_ID_FILTER_KEY])),
        bigquery.ScalarQueryParameter(
            START_DATE_FILTER_KEY, "DATE", payload["params"][START_DATE_FILTER_KEY]),
        bigquery.ScalarQueryParameter(
            END_DATE_FILTER_KEY, "DATE", end_date),
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
            polarity as name,
            SUM(review_count) as value
        FROM {VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_12_2"]}
        WHERE review_date >= DATE_SUB(@start_date,INTERVAL {ma_number[1]} DAY)
            AND review_date <= @end_date
            AND case_study_id = @case_study_id
            {company_filter_str}
        GROUP BY polarity
    """
    final_sql = construct_final_query(nlp_type="VOC",
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_12_2"],
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
            "polarity": row.name,
            "value": row.value
        })

    return result


def fetch_voc_1_4_1(payload):
    """
    Fetch Collected Customer Reviews Count by Language data
    """
    result = []

    if payload["params"]["ma_type"].lower() == MA_DAILY.lower():
        ma_number = (1, 0)  # Daily type
    else:
        ma_number = MA_TYPES[payload["params"]["ma_type"]]

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

    country_filter_str = ""
    if 'country_codes' in payload["params"]:
        country_filter_str = "AND country_code IN UNNEST(@country_codes)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "country_codes", "STRING", payload["params"]['country_codes'].split(",")))

    filter_sql = f"""
    WITH voc_data AS(
        SELECT
            country_name,
            country_code,
            daily_date AS date,
            sum(records_daily) records_daily
        FROM
            { VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_4_1"] }
        WHERE
            case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
            { company_filter_str }
            { country_filter_str }
        GROUP BY
            country_name,
            country_code,
            daily_date
    ),
    foo AS (
        SELECT
            *,
            CASE
                WHEN COUNT(date) OVER (
                    PARTITION BY country_name,
                    country_code
                    ORDER BY
                        date ROWS BETWEEN { ma_number[1] } PRECEDING
                        AND CURRENT ROW
                ) < { ma_number[0] } THEN NULL
                ELSE AVG(records_daily) OVER (
                    PARTITION BY country_name,
                    country_code
                    ORDER BY
                        date ROWS BETWEEN { ma_number[1] } PRECEDING
                        AND CURRENT ROW
                )
            END AS value,
            (
                SELECT
                    COUNT(DISTINCT parent_review_id)
                FROM
                    { VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"] } s
                WHERE
                    s.case_study_id = @case_study_id
                    AND review_date >= DATE_SUB(@start_date, INTERVAL { ma_number[1] } DAY)
                    AND review_date <= @end_date
                    AND dimension_config_name IS NOT NULL
                    { company_filter_str }
                    { country_filter_str }
            ) AS records
        FROM
            voc_data
    )
    SELECT
        CONCAT('[', string_agg(to_json_string(t)), ']') AS value
    FROM
        foo t
    WHERE
        t.value IS NOT NULL
    """
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_4_1"],
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


def fetch_voc_1_5_1(payload):
    """
    Fetch Collected Customer Reviews Count by Language data
    """
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
        company_filter_str = "AND lower(company_name) IN UNNEST(@company_names)"
        query_parameters.append(
            bigquery.ArrayQueryParameter(
                COMPANY_NAME_FILTER_KEY, "STRING", payload["params"][COMPANY_NAME_FILTER_KEY].lower().split(
                    ",")
            )
        )

    country_filter_str = ""
    if 'country_codes' in payload["params"]:
        country_filter_str = "AND country_code IN UNNEST(@country_codes)"
        query_parameters.append(bigquery.ArrayQueryParameter(
            "country_codes", "STRING", payload["params"]['country_codes'].split(",")))

    filter_sql = f"""
    WITH voc_data AS (
        SELECT 
            country_name, 
            country_code, 
            SUM(collected_review_count) as collected_review_count,
            SUM(records) as records
        FROM
            { VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_5_1"] }
        WHERE
            case_study_id = @case_study_id
            AND daily_date >= @start_date
            AND daily_date <= @end_date
        { company_filter_str }
        { country_filter_str }
        GROUP BY
            country_name,
            country_code
    ),
    voc_data_notblank AS (
        SELECT 
                *
        FROM 
            voc_data
        WHERE
            country_code NOT IN UNNEST(['blank'])
    )
    -- foo AS (
    --    SELECT
    --        *,
    --        (
    --             SELECT
    --                 COUNT(DISTINCT parent_review_id)
    --            FROM
    --                { VOC_CHART_DATA_MART_SCRIPT_MAPPING["summary_table"] } st
    --            WHERE
    --                case_study_id = @case_study_id
    --                 AND review_date >= @start_date
    --                AND review_date <= @end_date
    --                AND dimension_config_name is NOT NULL
    --                AND country_code = voc_data_notblank.country_code
    --                AND country_name = voc_data_notblank.country_name
    --                 { company_filter_str }
    --                 { country_filter_str }
    --        ) AS records
    --     FROM
    --         voc_data_notblank
    -- )
    SELECT
                CONCAT('[', string_agg(to_json_string(t)), ']') AS value
    FROM
        voc_data t
    """
    
    final_sql = construct_final_query(nlp_type='VOC',
                                      chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_1_5_1"],
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


def fetch_voc_11(payload):
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
        nlp_type='VOC',
        chart_code=VOC_CHART_DATA_MART_SCRIPT_MAPPING["VOC_11"],
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
