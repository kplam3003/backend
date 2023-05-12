from sqlalchemy.sql.expression import desc
import config
import datetime as dt
import logger

from database import CompanyDataSource, DimensionConfigInfoV110, DimensionConfigVersion, auto_session
from database import CaseStudy
from database import CaseStudyChartFilter
from database import CaseStudyCompanyDataSource
from database import CaseStudyDimensionStatisticV110
from database import NLPPack
from database import NLPType
from google.cloud import bigquery
from src.chart.prepare_chart_data import PrepareChartDataRunner

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

CS_CRAWL_STATISTIC_KEY_NAME = "crawl_statistics"
CS_CRAWL_STATISTIC_DATA_VERSION_REVIEW_KEY_NAME = "data_version_review"
CS_CRAWL_STATISTIC_DATA_VERSION_JOB_KEY_NAME = "data_version_job"
CS_CRAWL_STATISTIC_DATA_VERSION_CORESIGNAL_EMPLOYEES_KEY_NAME = "data_version_coresignal_employees"
COMPANY_DATA_SOURCE_NLP_REVIEW_KEY_NAME = "nlp_review"


@auto_session
def update_case_study_company_data_source_data_version(case_study_id, data_version, session=None):
    """
    payload = {
        "data_versions": [
            {"company_datasource_id": 123, "data_version_review": 2, "data_version_job": 3, "data_version_coresignal_employees": 4},
            {...}
        ],
        "progress": -1
    }
    """
    logger.info(
        f"BEGIN task: Update case study company data source data version: {data_version}")
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    if not case_study:
        logger.warning(
            f"Could not find active case study with ID {case_study_id}")
        return False

    case_study_company_datasources = session.query(CaseStudyCompanyDataSource)\
        .filter(CaseStudyCompanyDataSource.case_study_id == case_study_id).all()

    company_data_version_dict = {
        item["company_datasource_id"]: (
            item[CS_CRAWL_STATISTIC_DATA_VERSION_REVIEW_KEY_NAME], 
            item[CS_CRAWL_STATISTIC_DATA_VERSION_JOB_KEY_NAME],
            item[CS_CRAWL_STATISTIC_DATA_VERSION_CORESIGNAL_EMPLOYEES_KEY_NAME]
        )
        for item in data_version
    }

    logger.info(f"Data version dict: {company_data_version_dict}")

    for case_study_company_datasource in case_study_company_datasources:
        company_data_source_id = case_study_company_datasource.data_source_id
        extra_data = case_study_company_datasource.extra_data
        if CS_CRAWL_STATISTIC_KEY_NAME not in extra_data:
            extra_data[CS_CRAWL_STATISTIC_KEY_NAME] = {}
        if company_data_source_id in company_data_version_dict:
            extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_REVIEW_KEY_NAME] = company_data_version_dict[company_data_source_id][0]
            extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_JOB_KEY_NAME] = company_data_version_dict[company_data_source_id][1]
            extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_CORESIGNAL_EMPLOYEES_KEY_NAME] = company_data_version_dict[company_data_source_id][2]
        else:
            extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_REVIEW_KEY_NAME] = None
            extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_JOB_KEY_NAME] = None
            extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_CORESIGNAL_EMPLOYEES_KEY_NAME] = None

        case_study_company_datasource.set_jsonb_attr("extra_data", extra_data)

    session.commit()
    return True


@auto_session
def update_case_study_company_data_source_statistic(case_study_id, session=None):
    """
    Update statistic of Case Study Company Datasource whenever a CS is completed. Apply for:
    - New approved case study is done
    - Approved case study is done from schedule task
    """
    logger.info(f"Begin task - Update case study company data source statistic")
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    if not case_study:
        logger.warning(
            f"Could not find active case study with ID {case_study_id}")
        return False

    # Get Case Study company data source IDs
    case_study_company_datasources = session.query(CaseStudyCompanyDataSource)\
        .filter(CaseStudyCompanyDataSource.case_study_id == case_study_id).all()

    company_data_source_review_version_filter_str = " OR ".join([
        f"(company_datasource_id = {item.data_source_id} AND data_version <= {item.extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_REVIEW_KEY_NAME] if item.extra_data[CS_CRAWL_STATISTIC_KEY_NAME].get(CS_CRAWL_STATISTIC_DATA_VERSION_REVIEW_KEY_NAME) else '0'})"
        for item in case_study_company_datasources
    ])

    company_data_source_job_version_filter_str = " OR ".join([
        f"(company_datasource_id = {item.data_source_id} AND data_version <= {item.extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_JOB_KEY_NAME] if item.extra_data[CS_CRAWL_STATISTIC_KEY_NAME].get(CS_CRAWL_STATISTIC_DATA_VERSION_JOB_KEY_NAME) else '0'})"
        for item in case_study_company_datasources
    ])
    
    company_data_source_coresignal_employees_version_filter_str = " OR ".join([
        f"(company_datasource_id = {item.data_source_id} AND data_version <= {item.extra_data[CS_CRAWL_STATISTIC_KEY_NAME][CS_CRAWL_STATISTIC_DATA_VERSION_CORESIGNAL_EMPLOYEES_KEY_NAME] if item.extra_data[CS_CRAWL_STATISTIC_KEY_NAME].get(CS_CRAWL_STATISTIC_DATA_VERSION_CORESIGNAL_EMPLOYEES_KEY_NAME) else '0'})"
        for item in case_study_company_datasources
    ])

    # Get statistic from BQ
    nlp_pack = session.query(NLPPack).filter(
        NLPPack.id == case_study.nlp_pack_id).first()
    nlp_type = session.query(NLPType).filter(
        NLPType.id == nlp_pack.nlp_type_id).first()

    target_table_name_dict = {
        "voe": "voe_crawl_statistics",
        "voc": "voc_crawl_statistics",
        "hra": "hra_crawl_statistics",
    }
    target_table_name = target_table_name_dict[nlp_type.name.lower()]
    crawl_data_sql = f"""
        WITH voc AS (
            SELECT 
                company_datasource_id,
                created_at,
                num_reviews,
            FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voc_crawl_statistics`
            WHERE data_type = 'review'
                AND ({company_data_source_review_version_filter_str})
        ),
        voe AS (
            SELECT 
                company_datasource_id,
                created_at,
                num_reviews,
            FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voe_crawl_statistics`
            WHERE (
                data_type = 'review'
                AND ({company_data_source_review_version_filter_str})
            ) OR (
                data_type = 'job'
                AND ({company_data_source_job_version_filter_str})
            )
        ),
        hra AS (
            SELECT 
                company_datasource_id,
                created_at,
                num_reviews,
            FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.hra_crawl_statistics`
            WHERE data_type = 'coresignal_employees'
                AND ({company_data_source_coresignal_employees_version_filter_str})
        )
        SELECT
            company_datasource_id,
            MAX(created_at) AS created_at,
            SUM(num_reviews) AS num_reviews
        FROM {nlp_type.name.lower()}
        GROUP BY company_datasource_id
    """
    client = bigquery.Client()
    query_job = client.query(crawl_data_sql)
    rows = query_job.result()
    crawl_statistics = {}
    for row in rows:
        logger.info(f"Data from BQ: {row}")
        crawl_statistic = crawl_statistics.setdefault(
            row.company_datasource_id, {})
        crawl_statistic["date"] = row.created_at.strftime("%d/%m/%Y")
        crawl_statistic["total_reviews"] = row.num_reviews

    # Update CaseStudyCompanyDataSource.statistic data
    for case_study_company_datasource in case_study_company_datasources:
        crawl_statistic = case_study_company_datasource.extra_data
        if case_study_company_datasource.data_source_id in crawl_statistics:
            crawl_statistic[CS_CRAWL_STATISTIC_KEY_NAME]["date"] = crawl_statistics[case_study_company_datasource.data_source_id]["date"]
            crawl_statistic[CS_CRAWL_STATISTIC_KEY_NAME]["total_reviews"] = crawl_statistics[
                case_study_company_datasource.data_source_id]["total_reviews"]
        else:
            crawl_statistic[CS_CRAWL_STATISTIC_KEY_NAME]["date"] = None
            crawl_statistic[CS_CRAWL_STATISTIC_KEY_NAME]["total_reviews"] = None

        case_study_company_datasource.set_jsonb_attr(
            "extra_data", crawl_statistic)
    session.commit()
    logger.info(f"End task - Update case study company data source statistic")
    return True


@auto_session
def update_finished_case_study_status(case_study_id, session=None):
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    if not case_study:
        logger.warning(
            f"Could not find active case study with ID {case_study_id}")
        return False

    is_case_study_has_data = True

    nlp_pack = session.query(NLPPack).filter(
        NLPPack.id == case_study.nlp_pack_id).first()
    nlp_type = session.query(NLPType).filter(
        NLPType.id == nlp_pack.nlp_type_id).first()

    if nlp_type.name.lower() == "voc":
        review_count_sql = f"""
            WITH cte AS (
                SELECT review_id
                FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET_STAGING}.voc`
                WHERE case_study_id = @case_study_id
                LIMIT 1
            )
            SELECT COUNT(review_id) AS review_count
            FROM cte;
        """

        query_parameters = [
            bigquery.ScalarQueryParameter(
                "case_study_id", "INTEGER", case_study_id),
        ]

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters
        )
        query_job = client.query(review_count_sql, job_config=job_config)
        rows = query_job.result()
        review_count = 0
        for row in rows:
            review_count = row.review_count

        is_case_study_has_data = review_count > 0

    if nlp_type.name.lower() == "voe":
        review_job_count_sql = f"""
            WITH top_1_review_id AS (
                SELECT review_id
                FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET_STAGING}.voe`
                WHERE case_study_id = @case_study_id
                LIMIT 1
            ),
            top_1_job_id AS (
                SELECT job_id
                FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET_STAGING}.voe_job`
                WHERE case_study_id = @case_study_id
                LIMIT 1
            )
            SELECT COUNT(review_id) AS review_count, (
                SELECT COUNT(job_id)
                FROM top_1_job_id
            )   AS job_count
            FROM top_1_review_id
        """

        query_parameters = [
            bigquery.ScalarQueryParameter(
                "case_study_id", "INTEGER", case_study_id),
        ]

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters
        )
        query_job = client.query(review_job_count_sql, job_config=job_config)
        rows = query_job.result()
        review_count = 0
        job_count = 0
        for row in rows:
            review_count = row.review_count
            job_count = row.job_count

        is_case_study_has_data = review_count > 0 or job_count > 0

    if not is_case_study_has_data:
        case_study.status = "FAILED"
        session.commit()

    return True


@auto_session
def update_voe_chart_filter_data(case_study_id, session=None):
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    if not case_study:
        logger.warning(
            f"Could not find active case study with ID {case_study_id}")
        return False

    nlp_pack = session.query(NLPPack).filter(
        NLPPack.id == case_study.nlp_pack_id).first()
    nlp_type = session.query(NLPType).filter(
        NLPType.id == nlp_pack.nlp_type_id).first()
    if nlp_type.name.lower() != "voe":
        return False

    logger.info(
        f"[update_voe_chart_filter_data] Getting filter VoE filter data of Case Study {case_study_id}")
    bq_sql = f"""
        SELECT
            DISTINCT CASE
                WHEN job_function is NULL or job_function = '' THEN 'undefined'
                ELSE job_function
            END AS job_function,
            CASE
                WHEN job_country is NULL or job_country = '' THEN 'undefined'
                ELSE job_country
            END AS job_country,
            CASE
                WHEN role_seniority is NULL or role_seniority = '' THEN 'undefined'
                ELSE role_seniority
            END AS role_seniority,
            CASE
                WHEN job_type is NULL or job_type = '' THEN 'undefined'
                ELSE job_type
            END AS job_type,
            posted_date
        FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET}.voe_job_summary_table`
        WHERE case_study_id = @case_study_id
    """

    query_parameters = [
        bigquery.ScalarQueryParameter(
            "case_study_id", "INTEGER", case_study_id),
    ]

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(bq_sql, job_config=job_config)
    rows = query_job.result()
    job_functions, job_countries, role_seniorities, job_types = [], [], [], []
    latest_job_posting_date = None

    for row in rows:
        job_functions.append(row["job_function"])
        job_countries.append(row["job_country"])
        role_seniorities.append(row["role_seniority"])
        job_types.append(row["job_type"])
        if not latest_job_posting_date:
            latest_job_posting_date = row["posted_date"]
            continue
        if latest_job_posting_date < row["posted_date"]:
            latest_job_posting_date = row["posted_date"]

    filter_data = {}
    if job_functions:
        job_functions = list(set(job_functions))
        job_functions.sort()
        job_functions_dict = {k+1: v for k, v in enumerate(job_functions)}
        filter_data["job_function"] = job_functions_dict
    if job_countries:
        job_countries = list(set(job_countries))
        job_countries.sort()
        job_countries_dict = {k+1: v for k, v in enumerate(job_countries)}
        filter_data["job_country"] = job_countries_dict
    if role_seniorities:
        role_seniorities = list(set(role_seniorities))
        role_seniorities.sort()
        role_seniorities_dict = {k+1: v for k,
                                 v in enumerate(role_seniorities)}
        filter_data["role_seniority"] = role_seniorities_dict
    if job_types:
        job_types = list(set(job_types))
        job_types.sort()
        job_types_dict = {k+1: v for k, v in enumerate(job_types)}
        filter_data["job_type"] = job_types_dict

    if 'latest_job_posting_date' not in filter_data:
        filter_data["latest_job_posting_date"] = latest_job_posting_date

    if latest_job_posting_date:
        filter_data["latest_job_posting_date"] = str(
            filter_data["latest_job_posting_date"].isoformat())

    case_study_chart_filter = session.query(CaseStudyChartFilter)\
        .filter(CaseStudyChartFilter.case_study_id == case_study_id).first()
    if case_study_chart_filter:
        chart_filter = case_study_chart_filter.chart_filter
        for key, item in filter_data.items():
            chart_filter[key] = item

        case_study_chart_filter.set_jsonb_attr("chart_filter", chart_filter)
        case_study_chart_filter.update_time = str(
            dt.datetime.utcnow().isoformat())
    else:
        case_study_chart_filter = CaseStudyChartFilter(
            case_study_id=case_study_id,
            chart_filter=filter_data,
            is_active=True
        )
        session.add(case_study_chart_filter)
    session.commit()

    return True


@auto_session
def prepare_chart_data(case_study_id, session):
    logger.info(f"Begin - Prepare chart data - Case study ID {case_study_id}")

    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    if not case_study:
        logger.error(
            f"In progress - Prepare chart data - Case study ID {case_study_id} does not exists")

    # No preparation when there is
    if not case_study.nlp_type_latest_review_date:
        logger.error("No latest_review_date value")
        return False
    prepare_chart_data_runner = PrepareChartDataRunner(case_study=case_study)
    prepare_chart_data_runner.run()
    logger.info(f"End - Prepare chart data - Case study ID {case_study_id}")


@auto_session
def update_npl_type_latest_review_date(case_study_id, session=None):
    logger.info("Begin - Updating case study nlp_type_latest_review_date")
    NLP_OUTPUT_PREFIX = {
        "voc": "",
        "voe": "voe_",
        "hra": "hra_"
    }
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    nlp_pack = session.query(NLPPack).filter(
        NLPPack.id == case_study.nlp_pack_id).first()
    nlp_type = session.query(NLPType).filter(
        NLPType.id == nlp_pack.nlp_type_id).first()

    if nlp_type.name.lower() == "hra":
        if not case_study.nlp_type_latest_review_date:
            case_study.nlp_type_latest_review_date = dt.date.today()

    else:
        sql = f"""
            SELECT MAX(review_date) AS review_date
            FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET}.{NLP_OUTPUT_PREFIX[nlp_type.name.lower()]}nlp_output`
            WHERE case_study_id = {case_study.id}
                AND nlp_type = '{nlp_type.name}'
        """
        client = bigquery.Client()
        query_job = client.query(sql)
        rows = query_job.result()
        for row in rows:
            case_study.nlp_type_latest_review_date = row.review_date
            

    logger.info("End - Updating case study nlp_type_latest_review_date")
    logger.info("Begin - Cleaning up error messages")
    case_study.crawl_error_reason = []
    logger.info("End - Cleaning up error messages")
    session.commit()


@auto_session
def update_case_study_dimension_config_statistic_V110(case_study_id, session=None):
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    if not case_study:
        logger.warning(
            "Could not fiund active case study with ID {}".format(case_study_id))
        return False

    dimension_config_id = case_study.dimension_config_id

    logger.info(
        f"[update_case_study_dimension_config_statistic_V110] Begin update Case Study {case_study_id} Dimension config {dimension_config_id}")

    NLP_OUTPUT_PREFIX = {
        "voc": "",
        "voe": "voe_",
        "hra": "hra_"
    }
    nlp_pack = session.query(NLPPack).filter(
        NLPPack.id == case_study.nlp_pack_id).first()
    nlp_type = session.query(NLPType).filter(
        NLPType.id == nlp_pack.nlp_type_id).first()
    bq_sql = f"""
        SELECT
            dimension,
            modified_dimension,
            label,
            modified_label,
            customer_review_processed
        FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET}.{NLP_OUTPUT_PREFIX[nlp_type.name.lower()]}dimension_config_statistic`
        WHERE case_study_id = @case_study_id
            AND customer_review_processed > 0
        ORDER BY modified_label, label, modified_dimension, dimension
        """

    query_parameters = [
        bigquery.ScalarQueryParameter(
            "case_study_id", "INTEGER", case_study_id),
    ]

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )
    query_job = client.query(bq_sql, job_config=job_config)
    rows = query_job.result()
    if not rows.total_rows:
        logger.warning(
            f"Case study {case_study_id} has no dimensions, labels statistic")
        return False

    dimension_label_dict = dict()
    for row in rows:
        # dimension, modified_dimension, label, modified_label
        dimension_label_dict[(row.dimension, row.modified_dimension,
                              row.label, row.modified_label)] = row.customer_review_processed

    webapp_db_sql = f"""
        SELECT
            id,
            dimension,
            modified_dimension,
            label,
            modified_label
        FROM leo_dimension_config_info_v110
        WHERE dimension_config_id = {dimension_config_id}
    """

    result = session.execute(webapp_db_sql)
    dimension_datas = result.fetchall()

    count_arr = []
    for row in dimension_datas:
        dimension_config_info_v110_id = row[0]
        dimension = row[1]
        modified_dimension = row[2]
        label = row[3]
        modified_label = row[4]

        count_obj = {}
        if (dimension, modified_dimension, label, modified_label) in dimension_label_dict:
            count_obj["dimension_config_info_v110_id"] = dimension_config_info_v110_id
            count_obj["case_study_id"] = case_study.id
            count_obj["count"] = dimension_label_dict[(
                dimension, modified_dimension, label, modified_label)]

            count_arr.append(count_obj)

    session.query(CaseStudyDimensionStatisticV110)\
        .filter(CaseStudyDimensionStatisticV110.case_study_id == case_study_id).delete()
    session.commit()

    # logger.info(f"Dimension stats: {dimension_label_dict}")
    label_stat_objects = []
    for inser_info in count_arr:
        label_stat_objects.append(
            CaseStudyDimensionStatisticV110(
                case_study_id=inser_info["case_study_id"],
                dimension_config_info_v110_id=inser_info["dimension_config_info_v110_id"],
                count=inser_info["count"]
            )
        )
    logger.info(f"case_study_id: {case_study_id} dimensions, labels stats")
    session.bulk_save_objects(label_stat_objects)
    session.commit()
    logger.info("End")
    return True


@auto_session
def update_case_study_progress(case_study_id, payload, session=None):
    assert case_study_id is not None, "Case study id is None"
    try:
        progress_value = float(payload["progress"])
    except Exception as error:
        invalid_progress_value = payload["progress"]
        logger.error(
            f"Case study ID {case_study_id} has invalid progress value {invalid_progress_value}, error={error}")
        return False

    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id,
        CaseStudy.is_active
    ).first()
    if not case_study:
        logger.error(f"Invalid/In active case study {case_study_id}")
        return

    # Important: progress value -1 represent for a case study is be started:
    # - Set progress to 0
    # - Update the official data source version for the case study
    if progress_value == -1:
        case_study.crawl_progress["progress"] = case_study.case_study_crawl_progress_default(
        )
        update_case_study_company_data_source_data_version(
            case_study_id, payload["data_versions"])
        session.commit()
        return True

    if progress_value > case_study.crawl_progress["progress"]:
        del payload["event"]
        del payload["case_study_id"]
        case_study.crawl_progress = payload
        session.commit()
        return True

    return True


@auto_session
def update_company_data_source_nlp_statistic(case_study_id, session=None):
    case_study_company_datasources = session.query(CaseStudyCompanyDataSource)\
        .filter(CaseStudyCompanyDataSource.case_study_id == case_study_id).all()

    company_data_source_ids = list(
        map(lambda x: x.data_source_id, case_study_company_datasources))
    company_data_source_ids_str = ','.join(map(str, company_data_source_ids))

    nlp_data_sql = f"""
        WITH voc_total_review as (
        SELECT 
            company_datasource_id,
            created_at as crawl_at,
            num_reviews as crawl_reviews
        FROM {config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voc_crawl_statistics
        WHERE company_datasource_id IN ({company_data_source_ids_str})
            AND data_type = 'review'
        ),
        voc_cte as (
        SELECT 
            company_datasource_id,
            nlp_pack,
            processed_at,
            SUM( num_processed_reviews ) as processed_reviews,
            (SELECT sum(crawl_reviews) FROM voc_total_review WHERE company_datasource_id= c.company_datasource_id AND crawl_at <= c.processed_at) as total_reviews
        FROM {config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voc_nlp_statistics c
        WHERE company_datasource_id IN ({company_data_source_ids_str})
        GROUP BY 
            company_datasource_id,
            nlp_pack,
            processed_at
        ),
        voe_total_review as (
        SELECT 
            company_datasource_id,
            created_at as crawl_at,
            num_reviews as crawl_reviews
        FROM {config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voe_crawl_statistics
        WHERE company_datasource_id IN ({company_data_source_ids_str})
            AND data_type = 'review'
        ),
        voe_cte as (
        SELECT 
            company_datasource_id,
            nlp_pack,
            processed_at,
            SUM( num_processed_reviews ) as processed_reviews,
            (SELECT sum(crawl_reviews) FROM voe_total_review WHERE company_datasource_id= e.company_datasource_id AND crawl_at <= e.processed_at) as total_reviews
        FROM {config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voe_nlp_statistics e
        WHERE company_datasource_id IN ({company_data_source_ids_str})
        GROUP BY 
            company_datasource_id,
            nlp_pack,
            processed_at
        )
        SELECT 
            company_datasource_id,
            processed_at,
            nlp_pack,
            total_reviews,
            processed_reviews
        FROM voc_cte
        UNION ALL 
        SELECT 
            company_datasource_id,
            processed_at,
            nlp_pack,
            total_reviews,
            processed_reviews
        FROM voe_cte
        ORDER BY
        processed_at DESC,
        nlp_pack
    """

    client = bigquery.Client()
    query_job = client.query(nlp_data_sql)
    rows = query_job.result()
    company_data_source_nlp_statistics = {}

    for row in rows:
        _id = row.company_datasource_id
        company_data_source_nlp_statistics.setdefault(_id, [])
        company_data_source_nlp_statistics[_id].append({
            "total_review": row.total_reviews,
            "new_review": row.processed_reviews,
            "date": row.processed_at.strftime("%d/%m/%Y"),
            "pack": row.nlp_pack,
        })

    for _company_data_source_id in company_data_source_ids:
        company_data_source = session.query(CompanyDataSource)\
            .filter(CompanyDataSource.id == _company_data_source_id).first()

        _company_data_source_statistic = company_data_source.statistic
        _company_data_source_statistic_data = company_data_source_nlp_statistics.get(
            _company_data_source_id, None)

        if _company_data_source_statistic_data:
            _company_data_source_statistic[COMPANY_DATA_SOURCE_NLP_REVIEW_KEY_NAME] = _company_data_source_statistic_data

        company_data_source.set_jsonb_attr(
            "statistic", _company_data_source_statistic)

    session.commit()


@auto_session
def update_case_study_dimension_config_info_statistic(case_study_id, session=None):
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    nlp_pack = session.query(NLPPack).filter(
        NLPPack.id == case_study.nlp_pack_id).first()
    nlp_type = session.query(NLPType).filter(
        NLPType.id == nlp_pack.nlp_type_id).first()
    dimension_config = session.query(DimensionConfigVersion).filter(
        DimensionConfigVersion.dimension_config_id == case_study.dimension_config_id
    ).order_by(desc(DimensionConfigVersion.id)).first()

    table_1 = "casestudy_custom_dimension_statistics" \
        if nlp_type.name.lower() == "voc" else \
        "voe_casestudy_custom_dimension_statistics"

    table_2 = "case_study_run_id" if nlp_type.name.lower() == "voc"\
        else "voe_case_study_run_id"

    table_3 = "casestudy_dimension_config" if nlp_type.name.lower() == "voc"\
        else "voe_casestudy_dimension_config"

    nlp_data_sql = f"""
    WITH latest_run_id AS (
        SELECT run_id
        FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET_STAGING}.{table_2}`
        WHERE case_study_id = {case_study_id}
        ORDER BY created_at DESC
        LIMIT 1
    ),
    custom_dimension_label_data AS (
        SELECT 
            case_study_id, 
            dimension_config_id, 
            run_id, 
            dimension, 
            label,
            word,
            word_count AS count,
            review_count AS reviews
        FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.{table_1}`
        WHERE case_study_id = {case_study_id}
            AND dimension_config_id = {dimension_config.id}
            AND run_id in (SELECT run_id FROM latest_run_id)
    ),
    custom_dimension_label AS (
        SELECT * 
        FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.{table_3}`
        WHERE is_user_defined
            AND case_study_id = {case_study_id}
            AND dimension_config_id = {dimension_config.id}
            AND run_id in (SELECT run_id FROM latest_run_id)
    )
    SELECT 
        origin.dimension, 
        origin.label, 
        di_config.modified_dimension, 
        di_config.modified_label, 
        origin.word, 
        origin.count, 
        origin.reviews
    FROM custom_dimension_label AS di_config
        LEFT JOIN custom_dimension_label_data AS origin
        ON di_config.case_study_id = origin.case_study_id
        AND di_config.dimension_config_id = origin.dimension_config_id
        AND di_config.run_id = origin.run_id
        AND di_config.nlp_dimension = origin.dimension
        AND di_config.nlp_label = origin.label
    WHERE origin.dimension IS NOT NULL
        AND origin.label IS NOT NULL
    ORDER BY
        origin.dimension,
        origin.label,
        origin.count DESC, 
        origin.reviews DESC
    """

    client = bigquery.Client()
    query_job = client.query(nlp_data_sql)
    rows = query_job.result()
    data_dict = {}

    for row in rows:
        key = (row.dimension, row.label)
        data_dict.setdefault(key, [])
        data_dict[key].append({
            "name": row.word,
            "count": row.count,
            "review": row.reviews
        })

    logger.info(f"Word phrase result: {data_dict}")

    for key, item in data_dict.items():
        dimension_config_info = session.query(DimensionConfigInfoV110).filter(
            DimensionConfigInfoV110.dimension == key[0], DimensionConfigInfoV110.label == key[1],
            DimensionConfigInfoV110.dimension_config_id == case_study.dimension_config_id
        ).first()

        case_study_dimension_config_statistic = session.query(CaseStudyDimensionStatisticV110).filter(
            CaseStudyDimensionStatisticV110.dimension_config_info_v110_id == dimension_config_info.id
        ).first()

        case_study_dimension_config_statistic.set_jsonb_attr("statistic", item)

    session.commit()


@auto_session
def update_chart_filter_data(case_study_id, session=None):
    case_study = session.query(CaseStudy).filter(
        CaseStudy.id == case_study_id).first()
    if not case_study:
        logger.warning(
            f"Could not find active case study with ID {case_study_id}")
        return False

    nlp_pack = session.query(NLPPack).filter(
        NLPPack.id == case_study.nlp_pack_id).first()
    nlp_type = session.query(NLPType).filter(
        NLPType.id == nlp_pack.nlp_type_id).first()

    language_table_name = "summary_table_*"

    if nlp_type.name.lower() == "voe":
        language_table_name = "voe_summary_table_*"

    country_sql = f"""
    WITH 
    BATCH_LIST AS (
        SELECT
            batch_id
        FROM
            `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET_STAGING}.batch_status`
        WHERE
            status = 'Active'
    )
    SELECT
        DISTINCT case_study_id,
        CASE
            WHEN review_country is null OR review_country = 'Unknown' THEN 'blank'
            ELSE review_country
        END AS country_name,
        CASE
            WHEN country_code is null OR country_code = 'Unknown' THEN 'blank'
            ELSE country_code
        END AS country_code
    FROM
        `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET_STAGING}.review_country_mapping`
    WHERE
        case_study_id = {case_study_id}
        AND
        batch_id in (
                SELECT
                    batch_id
                FROM
                    BATCH_LIST)
    """

    language_sql = f"""
    SELECT DISTINCT case_study_id,
        CASE 
        WHEN language is null AND (language_code is null OR language_code = '') THEN 'blank'
        WHEN language is null AND language_code is not null THEN language_code
        ELSE language
        END as language_name,
        CASE
        WHEN language_code is null OR language_code = '' THEN 'blank'
        ELSE language_code
        END as language_code,
    FROM `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.BQ_DATASET_DATAMART_CS}.{language_table_name}`
    WHERE _TABLE_SUFFIX = CAST({case_study_id} AS STRING)
    """

    client = bigquery.Client()
    country_query_job = client.query(country_sql)
    language_query_job = client.query(language_sql)
    country_rows = country_query_job.result()
    language_rows = language_query_job.result()

    result = {
        "country": [],
        "language": []
    }

    for row in country_rows:
        result["country"].append(
            {
                "id": row.country_code,
                "name": row.country_name
            }
        )

    for row in language_rows:
        result["language"].append(
            {
                "id": row.language_code,
                "name": row.language_name
            }
        )

    case_study_chart_filter = session.query(CaseStudyChartFilter)\
        .filter(CaseStudyChartFilter.case_study_id == case_study_id).first()

    if case_study_chart_filter:
        chart_filter = case_study_chart_filter.chart_filter
        chart_filter["country"] = result["country"]
        chart_filter["language"] = result["language"]
        case_study_chart_filter.set_jsonb_attr("chart_filter", chart_filter)
        case_study_chart_filter.update_time = str(
            dt.datetime.utcnow().isoformat())
    else:
        case_study_chart_filter = CaseStudyChartFilter(
            case_study_id=case_study_id,
            chart_filter=result,
            is_active=True
        )
        session.add(case_study_chart_filter)
    session.commit()

    return True
