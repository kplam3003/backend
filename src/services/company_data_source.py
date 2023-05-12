from operator import itemgetter

import dateparser

import config
import datetime as dt
import logger
import json

import pymongo
from sqlalchemy.orm.attributes import flag_modified
from database import NLPType, auto_session, CompanyDataSource
from database import DataSource
from google.cloud import bigquery, pubsub_v1
from pubsub import GCPPubsubSender
import src.services.mongo_utils as mongo_utils


logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
COMPANY_DATA_SOURCE_CRAWL_REVIEW_KEY_NAME = "crawl_review"
COMPANY_DATA_SOURCE_SUMMARY_KEY_NAME = "summary"
FAILED_SEVERITY = ["CRITICAL", "MAJOR"]
# init long-live MongoDB connection, auth by X509 self-signed SSL
mongo_client = pymongo.MongoClient(
    config.MONGODB_URI,
    authMechanism="MONGODB-X509",
    directConnection=True,
    tls=True,
    tlsCertificateKeyFile=config.MONGODB_CLIENT_KEY,
    tlsCAFile=config.MONGODB_ROOT_CA,
    tlsAllowInvalidHostnames=True
)


# support serialize datetime
def default(o):
    if isinstance(o, (dt.date, dt.datetime)):
        return o.isoformat()


def publish(logger, gcp_product_id, topic, payload):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_product_id, topic)
    data = json.dumps(payload, default=default)

    data = data.encode("utf-8")

    logger.info(f"Published to {topic_path}, data: {data}")

    future = publisher.publish(topic_path, data)
    result = future.result()
    logger.info(f"Published a message {result}")


def trigger_company_data_source(payload=None):
    assert payload is not None, "payload is not null"
    try:
        publish(logger, config.GCP_DATA_PLATFORM_PROJECT_ID,
                config.GCP_PUBSUB_REQUEST_TOPIC, payload)
        return True
    except Exception as error:
        logger.error(
            f"[TRIGGER COMPANY DATA SOURCE] payload={payload}, error={error}")
        return False


def validate_company_data_source(payload=None):
    assert payload is not None, "payload is not null"
    try:
        pubsub_sender = GCPPubsubSender(
            config.GCP_WEBAPP_PROJECT_ID,
            config.GCP_WEBAPP_PUBSUB_VALIDATE_COMPANY_DS_TOPIC,
            config.GCP_WEBAPP_SERVICE_ACCOUNT_KEY_PATH
        )
        pubsub_sender.send(payload)
        return True
    except Exception as error:
        logger.error(
            f"[VALIDATE COMPANY DATASOURCE] payload={payload}, error={error}")
        return False


@auto_session
def update_company_datasource(company_datasource_id, validate_progress, is_valid, session=None):
    assert company_datasource_id is not None, "company_datasource_id is None"
    try:
        validate_progress = float(validate_progress)
    except Exception as error:
        logger.error(
            f"Company Datasource ID {company_datasource_id} has invalid progress value {validate_progress}, error={error}")
        return False

    cmp_ds = session.query(CompanyDataSource)\
        .filter(
            CompanyDataSource.id == company_datasource_id
    ).first()

    if not cmp_ds:
        logger.warning(
            f"Can not find active company data source with ID {company_datasource_id}")
        return False

    if validate_progress < 1:
        cmp_ds.validate_progress = validate_progress
        cmp_ds.is_valid = is_valid
        session.commit()
        return True
    elif validate_progress == 1:
        cmp_ds.validate_progress = validate_progress
        cmp_ds.is_valid = is_valid
        session.commit()
        return True

    return True


@auto_session
def check_company_datasource_validation(company_datasource_id, session=None):
    assert company_datasource_id is not None, "company_datasource_id is None"
    cmp_ds = session.query(CompanyDataSource).filter(
        CompanyDataSource.id == company_datasource_id).first()

    if not cmp_ds:
        logger.warning(
            f"Can not find active company data source with ID {company_datasource_id}")
        return False

    if cmp_ds.validate_progress > 0 and cmp_ds.validate_progress <= 1.0:
        return True

    return False


def _get_statistic_data(company_datasource_id, nlp_type_name):

    crawl_sql = f"""
        WITH cte as (
        SELECT 
            MAX(company_name) as company_name,
            MAX(source_name) as source_name,
            company_datasource_id,
            data_version,
            data_type,
            MIN(DATE(created_at)) as date,
            SUM(num_reviews) as new_reviews,
        FROM {config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.{nlp_type_name}_crawl_statistics
        WHERE company_datasource_id = {company_datasource_id}
            AND data_type NOT LIKE 'origin_%'
        GROUP BY 
            company_datasource_id,
            data_version,
            data_type
        )
        SELECT 
            company_name,
            source_name,
            company_datasource_id,
            data_version, 
            data_type,
            date,
            new_reviews,
            SUM(new_reviews) OVER (PARTITION BY company_datasource_id, data_type ORDER BY data_version ASC)  as total_reviews
        FROM cte
        ORDER BY  data_version  DESC
        """

    client = bigquery.Client()
    crawl_query_job = client.query(crawl_sql)
    crawl_rows = crawl_query_job.result()

    is_data = False

    crawl_review = {"job": {}, "review": {}, "hr": {}}

    for row in crawl_rows:
        if not is_data:
            is_data = True

        data_type = row.data_type if row.data_type in [
            "review", "coresignal_employees"] else "job"
        if data_type != "coresignal_employees":
            crawl_review[data_type].setdefault(row.data_version, {
                "total_review": 0,
                "new_review": 0,
                "date": row.date.strftime("%d/%m/%Y"),
                "data_version": row.data_version
            })
            crawl_review[data_type][row.data_version]["total_review"] += row.total_reviews
            crawl_review[data_type][row.data_version]["new_review"] += row.new_reviews
        else:
            crawl_review["hr"].setdefault(row.data_version, {
                "total_employee": 0,
                "new_employee": 0,
                "date": row.date.strftime("%d/%m/%Y"),
                "data_version": row.data_version
            })
            crawl_review["hr"][row.data_version]["total_employee"] += row.total_reviews
            crawl_review["hr"][row.data_version]["new_employee"] += row.new_reviews

    _job = list(crawl_review["job"].values())
    _review = list(crawl_review["review"].values())
    _coresignal_employees = list(crawl_review["hr"].values())
    crawl_review = {
        "job": _job,
        "review": _review,
        "hr": _coresignal_employees
    }

    return crawl_review, is_data


@auto_session
def update_company_datasource_progress(company_datasource_id, payload, session=None):

    company_datasource = session.query(CompanyDataSource).filter(
        CompanyDataSource.id == company_datasource_id
    ).first()

    data_type = payload.get("data_type")

    if not data_type:
        logger.error(f"Missing data_type in payload")
        return False

    if data_type not in ["job", "review", "review_stats", "coresignal_employees", "coresignal_stats"]:
        logger.info(f"Skip invalid data type")
        return False

    if not company_datasource:
        logger.error(f"Invalid/In company datasource {company_datasource_id}")
        return False

    if payload["progress"] == -1:
        company_datasource.crawl_progress[data_type] = {
            "progress": 0,
            "details": {
                "crawl": 0,
                "load": 0,
                "translate": 0,
                "preprocess": 0,
            },
            "status": CompanyDataSource.STATUS_IN_PROGRESS
        }
        flag_modified(company_datasource, "crawl_progress")
        session.commit()
        return True

    try:
        progress_value = float(payload["progress"])
        float(payload["details"]["crawl"])
        float(payload["details"]["load"])
        float(payload["details"]["translate"])
        float(payload["details"]["preprocess"])
    except Exception as error:
        logger.error(
            f"Datasource ID {company_datasource_id} has invalid progress value, error={error}")
        return False

    if progress_value > company_datasource.crawl_progress[data_type]["progress"]:
        del payload["event"]
        del payload["company_datasource_id"]
        del payload["data_type"]
        payload["status"] = CompanyDataSource.STATUS_IN_PROGRESS
        company_datasource.crawl_progress[data_type] = payload
        flag_modified(company_datasource, "crawl_progress")

    if progress_value == 1:
        data_source = session.query(DataSource).filter(
            DataSource.id == company_datasource.data_source_id
        ).first()

        nlp_type = session.query(NLPType).filter(
            NLPType.id == data_source.nlp_type_id
        ).first()

        nlp_type_name = nlp_type.name.lower()

        _company_datasource_statistic = company_datasource.statistic

        if data_type == "coresignal_stats":
            statistic_profile = fetch_company_stats_data(company_datasource_id)
            logger.info(f"Get HR Profile Data: {statistic_profile}")
            _company_datasource_statistic["hr_profile"] = statistic_profile
            company_datasource.set_jsonb_attr(
                "statistic", _company_datasource_statistic)

            company_datasource.crawl_progress[data_type]["status"] = CompanyDataSource.STATUS_COMPLETED
            flag_modified(company_datasource, "crawl_progress")
            session.commit()
            return True

        if "_stats" in data_type:
            statistic_profile = fetch_review_stats_data(
                company_datasource_id, nlp_type_name)

            logger.info(f"Get Profile Data: {statistic_profile}")
            _company_datasource_statistic["profile"] = statistic_profile
            company_datasource.set_jsonb_attr(
                "statistic", _company_datasource_statistic)

            company_datasource.crawl_progress[data_type]["status"] = CompanyDataSource.STATUS_COMPLETED
            flag_modified(company_datasource, "crawl_progress")
            session.commit()
            return True

        company_data_source_statistic, is_data = _get_statistic_data(
            company_datasource_id, nlp_type_name)
        if company_data_source_statistic:
            _company_datasource_statistic[COMPANY_DATA_SOURCE_CRAWL_REVIEW_KEY_NAME] = company_data_source_statistic

        num_reviews_total, num_reviews_yearly, num_reviews_quarterly = fetch_review_summary(
            company_datasource_id=company_datasource_id,
            nlp_type=nlp_type_name
        )
        rating_total, rating_yearly, rating_quarterly = fetch_rating_summary(
            company_datasource_id=company_datasource_id,
            nlp_type=nlp_type_name
        )
        polarity_total, polarity_yearly, polarity_quarterly = fetch_polarity_summary(
            company_datasource_id=company_datasource_id,
            nlp_type=nlp_type_name
        )

        return_data = {
            "num_reviews": {
                'total': num_reviews_total,
                'yearly': num_reviews_yearly,
                'quarterly': num_reviews_quarterly,
            },
            "rating": {
                'total': rating_total,
                'yearly': rating_yearly,
                'quarterly': rating_quarterly,
            },
            "polarity": {
                'total': polarity_total,
                'yearly': polarity_yearly,
                'quarterly': polarity_quarterly,
            },
        }

        _company_datasource_statistic[COMPANY_DATA_SOURCE_SUMMARY_KEY_NAME] = return_data

        company_datasource.set_jsonb_attr(
            "statistic", _company_datasource_statistic)
        if not company_datasource.is_data:
            company_datasource.is_data = True if is_data else False

        company_datasource.is_valid = True
        company_datasource.validate_progress = 1

        company_datasource.crawl_progress[data_type]["status"] = CompanyDataSource.STATUS_COMPLETED
        flag_modified(company_datasource, "crawl_progress")

        if company_datasource.crawl_progress["review"]["status"] == CompanyDataSource.STATUS_COMPLETED\
                and company_datasource.crawl_progress["job"]["status"] != CompanyDataSource.STATUS_IN_PROGRESS:
            company_datasource.status = CompanyDataSource.STATUS_COMPLETED

        if company_datasource.crawl_progress["review"]["status"] != CompanyDataSource.STATUS_IN_PROGRESS\
                and company_datasource.crawl_progress["job"]["status"] == CompanyDataSource.STATUS_COMPLETED:
            company_datasource.status = CompanyDataSource.STATUS_COMPLETED

    session.commit()

    return True


@auto_session
def update_company_datasource_error(company_datasource_id, payload, session=None):

    company_datasource = session.query(CompanyDataSource).filter(
        CompanyDataSource.id == company_datasource_id
    ).first()

    if not company_datasource:
        logger.error(f"Invalid/In company datasource {company_datasource_id}")
        return False

    company_datasource.crawl_progress[payload["data_type"]
                                      ]["status"] = CompanyDataSource.STATUS_FAILED

    if company_datasource.crawl_progress["review"]["status"] == CompanyDataSource.STATUS_FAILED\
            and company_datasource.crawl_progress["job"]["status"] == CompanyDataSource.STATUS_FAILED:
        company_datasource.status = CompanyDataSource.STATUS_FAILED

    if company_datasource.crawl_progress["review"]["status"] == CompanyDataSource.STATUS_FAILED\
            and not company_datasource.crawl_progress["job"]["status"]:
        company_datasource.status = CompanyDataSource.STATUS_FAILED

    if not company_datasource.crawl_progress["review"]["status"]\
            and company_datasource.crawl_progress["job"]["status"] == CompanyDataSource.STATUS_FAILED:
        company_datasource.status = CompanyDataSource.STATUS_FAILED

    session.commit()
    return True


# Rating queries
def _fetch_avg_rating(company_datasource_id: int, nlp_type: str):
    """Fetch and calculate average rating across the entire dataset 
    for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                "company_datasource_id": {'$eq': company_datasource_id},
                'rating': {'$ne': None}
            }
        },
        {
            '$project': {
                '_id': 0,
                'parent_review_id': 1,
                'rating': {'$toDouble': '$rating'}
            }
        },
        {
            '$group': {
                '_id': '$parent_review_id',
                'rating': {'$first': '$rating'}
            },
        },
        {
            '$group': {
                "_id": company_datasource_id,
                "average_rating": {'$avg': '$rating'}
            }
        }
    ], allowDiskUse=True)
    results = []
    for doc in cursor:
        results.append({
            'average_rating': doc['average_rating'],
        })
    return results


@mongo_utils.calculate_change(
    sort_keys=['year'],
    value_key='average_rating'
)
def _fetch_yearly_avg_rating(company_datasource_id: int, nlp_type: str):
    """Fetch and calculate average rating across years and 
    year-over-year changes in value and in percentage
    for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                'company_datasource_id': {'$eq': company_datasource_id},
                'rating': {'$ne': None}
            }
        },
        {
            '$project': {
                'parent_review_id': 1,
                'review_date': 1,
                'rating': {'$toDouble': '$rating'}
            }
        },
        {
            '$addFields': {
                'year': mongo_utils.YEAR_FIELD
            }
        },
        {
            '$group': {
                '_id': '$parent_review_id',
                'year': {'$first': '$year'},
                'rating': {'$first': '$rating'}
            }
        },
        {
            '$group': {
                '_id': {
                    'year': '$year',
                },
                'average_rating': {'$avg': '$rating'}
            }
        }
    ], allowDiskUse=True)
    results = []
    for doc in cursor:
        results.append({
            'year': doc['_id']['year'],
            'average_rating': doc['average_rating'],
        })
    return results


@mongo_utils.calculate_change(
    sort_keys=['year', 'quarter'],
    value_key='average_rating'
)
def _fetch_quarterly_avg_rating(company_datasource_id: int, nlp_type: str):
    """Fetch and calculate average rating across quarters and 
    quarter-over-quarter changes in value and in percentage
    for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                'company_datasource_id': {'$eq': company_datasource_id},
                'rating': {'$ne': None}
            }
        },
        {
            '$project': {
                'parent_review_id': 1,
                'review_date': 1,
                'rating': {'$toDouble': '$rating'}
            }
        },
        {
            '$addFields': {
                'year': mongo_utils.YEAR_FIELD,
                'quarter': mongo_utils.QUARTER_FIELD
            }
        },
        {
            '$group': {
                '_id': '$parent_review_id',
                'year': {'$first': '$year'},
                'quarter': {'$first': '$quarter'},
                'rating': {'$first': '$rating'}
            }
        },
        {
            '$group': {
                '_id': {
                    'year': '$year',
                    'quarter': '$quarter'
                },
                'average_rating': {'$avg': '$rating'}
            }
        }
    ], allowDiskUse=True)
    results = []
    for doc in cursor:
        results.append({
            'year': doc['_id']['year'],
            'quarter': doc['_id']['quarter'],
            'average_rating': doc['average_rating'],
        })
    return results


def fetch_rating_summary(company_datasource_id: int, nlp_type: str):
    """Fetch all summary info for rating"""
    return (
        _fetch_avg_rating(company_datasource_id, nlp_type),
        _fetch_yearly_avg_rating(company_datasource_id, nlp_type),
        _fetch_quarterly_avg_rating(company_datasource_id, nlp_type)
    )


# Number of reviews
def _fetch_num_reviews(company_datasource_id: int, nlp_type: str):
    """Fetch number of reviews for a given company_datasource 
    and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                "company_datasource_id": {'$eq': company_datasource_id}
            }
        },
        {
            '$group': {
                '_id': '$parent_review_id',
            }
        },
        {
            '$group': {
                '_id': company_datasource_id,
                'num_reviews': {'$sum': 1}
            }
        }
    ], allowDiskUse=True)
    results = []
    for doc in cursor:
        results.append({
            'num_reviews': doc['num_reviews'],
        })
    return results


@mongo_utils.calculate_change(
    sort_keys=['year'],
    value_key='num_reviews'
)
def _fetch_yearly_num_reviews(company_datasource_id: int, nlp_type: str):
    """Fetch number of reviews collected across years and 
    year-over-year changes in value and in percentage
    for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                "company_datasource_id": {'$eq': company_datasource_id}
            }
        },
        {
            '$group': {
                '_id': '$parent_review_id',
                'review_date': {'$first': '$review_date'},
            }
        },
        {
            '$addFields': {
                'year': mongo_utils.YEAR_FIELD,
            }
        },
        {
            '$group': {
                '_id': {'year': '$year'},
                'num_reviews': {'$sum': 1}
            }
        }
    ], allowDiskUse=True)
    results = []
    for doc in cursor:
        results.append({
            'year': doc['_id']['year'],
            'num_reviews': doc['num_reviews'],
        })
    return results


@mongo_utils.calculate_change(
    sort_keys=['year', 'quarter'],
    value_key='num_reviews'
)
def _fetch_quarterly_num_reviews(company_datasource_id: int, nlp_type: str):
    """Fetch and calculate average rating across quarters and 
    quarter-over-quarter changes in value and in percentage
    for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                "company_datasource_id": {'$eq': company_datasource_id}
            }
        },
        {
            '$group': {
                '_id': '$parent_review_id',
                'review_date': {'$first': '$review_date'},
            }
        },
        {
            '$addFields': {
                'year': mongo_utils.YEAR_FIELD,
                'quarter': mongo_utils.QUARTER_FIELD
            }
        },
        {
            '$group': {
                '_id': {'year': '$year', 'quarter': '$quarter'},
                'num_reviews': {'$sum': 1},
            }
        },
    ], allowDiskUse=True)
    results = []
    for doc in cursor:
        results.append({
            'year': doc['_id']['year'],
            'quarter': doc['_id']['quarter'],
            'num_reviews': doc['num_reviews'],
        })
    return results


def fetch_review_summary(company_datasource_id: int, nlp_type: str):
    """Fetch all summary info for num reviews"""
    return (
        _fetch_num_reviews(company_datasource_id, nlp_type),
        _fetch_yearly_num_reviews(company_datasource_id, nlp_type),
        _fetch_quarterly_num_reviews(company_datasource_id, nlp_type)
    )


# Polarity count
def _fetch_polarity(company_datasource_id: int, nlp_type: str):
    """Fetch number of occurence for each available nlp_pack 
    and polarity types for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                'company_datasource_id': {
                    '$eq': company_datasource_id
                }
            }
        },
        {
            '$unwind': {
                'path': '$nlp_results.__NLP_PACKS__'
            }
        },
        {
            '$addFields': {
                'nlp_pack': '$nlp_results.__NLP_PACKS__',
                'polarities': mongo_utils.POLARITY_FIELD
            }
        },
        {
            '$unwind': {
                'path': '$polarities.v'
            }
        },
        {
            '$group': {
                '_id': {
                    'nlp_pack': '$nlp_results.__NLP_PACKS__',
                    'polarity': '$polarities.v.polarity'
                },
                'count': {'$sum': 1}
            }
        }
    ], allowDiskUse=True)
    list_df = []
    for doc in cursor:
        list_df.append({
            'nlp_pack': doc['_id']['nlp_pack'],
            'polarity': doc['_id']['polarity'],
            'count': doc['count']
        })
    return list_df


@mongo_utils.calculate_change(
    sort_keys=['year'],
    value_key='count',
    filter_keys=['nlp_pack', 'polarity']
)
def _fetch_yearly_polarity(company_datasource_id: int, nlp_type: str):
    """Fetch and calculate number of occurence for each available nlp_pack 
    and polarity types across years and year-over-year changes in value 
    and in percentage for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                'company_datasource_id': {
                    '$eq': company_datasource_id
                }
            }
        },
        {
            '$unwind': {
                'path': '$nlp_results.__NLP_PACKS__'
            }
        },
        {
            '$addFields': {
                'year': mongo_utils.YEAR_FIELD,
                'nlp_pack': '$nlp_results.__NLP_PACKS__',
                'polarities': mongo_utils.POLARITY_FIELD
            }
        },
        {
            '$unwind': {
                'path': '$polarities.v'
            }
        },
        {
            '$group': {
                '_id': {
                    'nlp_pack': '$nlp_results.__NLP_PACKS__',
                    'year': '$year',
                    'polarity': '$polarities.v.polarity'
                },
                'count': {'$sum': 1}
            }
        }
    ], allowDiskUse=True)
    list_df = []
    for doc in cursor:
        list_df.append({
            'nlp_pack': doc['_id']['nlp_pack'],
            'year': doc['_id']['year'],
            'polarity': doc['_id']['polarity'],
            'count': doc['count']
        })
    return list_df


@mongo_utils.calculate_change(
    sort_keys=['year', 'quarter'],
    value_key='count',
    filter_keys=['nlp_pack', 'polarity']
)
def _fetch_quarterly_polarity(company_datasource_id: int, nlp_type: str):
    """Fetch and calculate number of occurence for each available nlp_pack 
    and polarity types across quarters and quarter-over-quarter changes in 
    value and in percentage for a given company_datasource and its nlp_type"""
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[nlp_type].aggregate([
        {
            '$match': {
                'company_datasource_id': {
                    '$eq': company_datasource_id
                }
            }
        },
        {
            '$unwind': {  # unwind available nlp packs for each document
                'path': '$nlp_results.__NLP_PACKS__'
            }
        },
        {
            '$addFields': {
                'year': mongo_utils.YEAR_FIELD,
                'quarter': mongo_utils.QUARTER_FIELD,
                'nlp_pack': '$nlp_results.__NLP_PACKS__',
                'polarities': mongo_utils.POLARITY_FIELD
            }
        },
        {
            '$unwind': {
                'path': '$polarities.v'
            }
        },
        {
            '$group': {
                '_id': {
                    'nlp_pack': '$nlp_results.__NLP_PACKS__',
                    'year': '$year',
                    'quarter': '$quarter',
                    'polarity': '$polarities.v.polarity'
                },
                'count': {'$sum': 1}
            }
        }
    ], allowDiskUse=True)
    results = []
    for doc in cursor:
        results.append({
            'nlp_pack': doc['_id']['nlp_pack'],
            'year': doc['_id']['year'],
            'quarter': doc['_id']['quarter'],
            'polarity': doc['_id']['polarity'],
            'count': doc['count']
        })
    return results


def fetch_polarity_summary(company_datasource_id: int, nlp_type: str):
    """Fetch all summary info for polarity"""
    return (
        _fetch_polarity(company_datasource_id, nlp_type),
        _fetch_yearly_polarity(company_datasource_id, nlp_type),
        _fetch_quarterly_polarity(company_datasource_id, nlp_type)
    )


@auto_session
def update_company_data_sources_health_check_status(company_id, session=None):
    company_datasources = session.query(CompanyDataSource).filter(
        CompanyDataSource.company_id == company_id,
        CompanyDataSource.is_active
    ).all()

    company_datasource_ids=[x.id for x in company_datasources]

    sql = f"""
    WITH
        stats AS(
        WITH
            stats_all AS(
            WITH
                voc_stats AS(
                WITH
                    origin_review AS (
                    SELECT
                        company_datasource_id,
                        source_name,
                        DATE(created_at) AS date_created,
                        num_reviews AS n_origin_reviews,
                        ROW_NUMBER() OVER (PARTITION BY company_datasource_id, DATE(created_at)
                        ORDER BY
                            created_at) AS date_order
                    FROM
                        `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voc_crawl_statistics`
                    WHERE
                        company_id = {company_id}
                        AND data_type = 'origin_review_stats' ),
                    crawled_review AS (
                    SELECT
                        company_datasource_id,
                        source_name,
                        DATE(created_at) AS date_created,
                        SUM(num_reviews) AS n_crawled_reviews,
                        data_version
                    FROM
                        `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voc_crawl_statistics`
                    WHERE
                        company_id = {company_id}
                        AND data_type = 'origin_review'
                    GROUP BY
                        company_datasource_id,
                        source_name,
                        DATE(created_at),
                        data_version )
                SELECT
                    origin_review.company_datasource_id,
                    origin_review.source_name,
                    origin_review.date_created,
                    origin_review.n_origin_reviews,
                    crawled_review.n_crawled_reviews,
                    ROW_NUMBER() OVER (PARTITION BY origin_review.company_datasource_id, origin_review.date_created ORDER BY origin_review.date_order DESC, crawled_review.data_version DESC ) AS stats_version
                FROM
                    origin_review
                LEFT JOIN
                    crawled_review
                ON
                    origin_review.company_datasource_id = crawled_review.company_datasource_id
                    AND origin_review.date_created = crawled_review.date_created )
            SELECT
                company_datasource_id,
                source_name,
                'review' AS data_type,
                date_created,
                n_origin_reviews AS total_review,
                n_crawled_reviews AS process_review,
                (CASE
                    WHEN n_crawled_reviews > 0 AND n_origin_reviews > 0 THEN ROUND(n_crawled_reviews / n_origin_reviews * 100)
                    ELSE
                    NULL
                END
                    ) AS successful_rate
            FROM
                voc_stats
            WHERE
                stats_version = 1
            UNION ALL (
            WITH
                voe_review_stats AS(
                WITH
                origin_review AS (
                SELECT
                    company_datasource_id,
                    source_name,
                    DATE(created_at) AS date_created,
                    num_reviews AS n_origin_reviews,
                    ROW_NUMBER() OVER (PARTITION BY company_datasource_id, DATE(created_at)
                    ORDER BY
                    created_at) AS date_order
                FROM
                    `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voe_crawl_statistics`
                WHERE
                    company_id = {company_id}
                    AND data_type = 'origin_review_stats' ),
                crawled_review AS (
                SELECT
                    company_datasource_id,
                    source_name,
                    DATE(created_at) AS date_created,
                    SUM(num_reviews) AS n_crawled_reviews,
                    data_version
                FROM
                    `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voe_crawl_statistics`
                WHERE
                    company_id = {company_id}
                    AND data_type = 'origin_review'
                GROUP BY
                    company_datasource_id,
                    source_name,
                    DATE(created_at),
                    data_version )
            SELECT
                origin_review.company_datasource_id,
                origin_review.source_name,
                origin_review.date_created,
                origin_review.n_origin_reviews,
                crawled_review.n_crawled_reviews,
                ROW_NUMBER() OVER (PARTITION BY origin_review.company_datasource_id, origin_review.date_created ORDER BY origin_review.date_order DESC, crawled_review.data_version DESC ) AS stats_version
            FROM
                origin_review
            LEFT JOIN
                crawled_review
            ON
                origin_review.company_datasource_id = crawled_review.company_datasource_id
                AND origin_review.date_created = crawled_review.date_created )
        SELECT
            company_datasource_id,
            source_name,
            'review' AS data_type,
            date_created,
            n_origin_reviews AS total_review,
            n_crawled_reviews AS process_review,
            (CASE
                WHEN n_crawled_reviews > 0 AND n_origin_reviews > 0 THEN ROUND(n_crawled_reviews / n_origin_reviews * 100)
            ELSE
            NULL
            END
            ) AS successful_rate
        FROM
            voe_review_stats
        WHERE
            stats_version = 1 )
        UNION ALL (
        WITH
            voe_job_stats AS(
            WITH
            origin_job AS (
            SELECT
                company_datasource_id,
                source_name,
                DATE(created_at) AS date_created,
                num_reviews AS n_origin_jobs,
                ROW_NUMBER() OVER (PARTITION BY company_datasource_id, DATE(created_at)
                ORDER BY
                created_at) AS date_order
            FROM
                `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voe_crawl_statistics`
            WHERE
                company_id = {company_id}
                AND data_type = 'origin_job'
                AND step_detail_id IS NULL ),
            crawled_job AS (
            SELECT
                company_datasource_id,
                source_name,
                DATE(created_at) AS date_created,
                SUM(num_reviews) AS n_crawled_jobs,
                data_version
            FROM
                `{config.GCP_DATA_PLATFORM_PROJECT_ID}.{config.GCP_DATA_PLATFORM_DATA_WAREHOUSE}.voe_crawl_statistics`
            WHERE
                company_id = {company_id}
                AND data_type = 'origin_job'
                AND step_detail_id IS NOT NULL
            GROUP BY
                company_datasource_id,
                source_name,
                DATE(created_at),
                data_version )
            SELECT
                origin_job.company_datasource_id,
                origin_job.source_name,
                origin_job.date_created,
                origin_job.n_origin_jobs,
                crawled_job.n_crawled_jobs,
                ROW_NUMBER() OVER (PARTITION BY origin_job.company_datasource_id, origin_job.date_created ORDER BY origin_job.date_order DESC, crawled_job.data_version DESC ) AS stats_version
            FROM
                origin_job
            LEFT JOIN
                crawled_job
            ON
                origin_job.company_datasource_id = crawled_job.company_datasource_id
                AND origin_job.date_created = crawled_job.date_created )
        SELECT
            company_datasource_id,
            source_name,
            'job' AS data_type,
            date_created,
            n_origin_jobs AS total_review,
            n_crawled_jobs AS process_review,
            (CASE
                WHEN n_crawled_jobs > 0 AND n_origin_jobs > 0 THEN ROUND(n_crawled_jobs / n_origin_jobs * 100)
            ELSE
            NULL
            END
            ) AS successful_rate
        FROM
            voe_job_stats
        WHERE
            stats_version = 1) )
    SELECT
        company_datasource_id,
        source_name,
        data_type,
        total_review,
        process_review,
        successful_rate,
        ROW_NUMBER() OVER (PARTITION BY company_datasource_id, data_type ORDER BY date_created DESC) AS stats_version
    FROM
        stats_all )
    SELECT
        company_datasource_id,
        source_name,
        data_type,
        total_review,
        process_review,
        successful_rate
    FROM
        stats
    WHERE
        stats_version = 1
    ORDER BY
        company_datasource_id;
    """

    client = bigquery.Client()
    query_job = client.query(sql)
    rows = query_job.result()

    result = {
        "company_data_sources_have_error": [],
        "company_data_sources_failed": [],
        "details": []
    }

    company_data_sources_have_error = []
    company_data_sources_failed = []

    for row in rows:
        _row = dict(row)
        if _row["company_datasource_id"] not in company_datasource_ids:
            continue
        if _row["data_type"] not in ["job", "review"]:
            continue

        logger.info(f"Health Check row data: {_row}")
        _row["status"] = "trigger finished"
        _row["company_id"] = company_id

        result["details"].append(_row)

    result["company_data_sources_have_error"] = company_data_sources_have_error
    result["company_data_sources_failed"] = company_data_sources_failed

    return result


def fetch_review_stats_data(company_datasource_id: int, nlp_type: str):
    """
    Sample output:
    [
        {
            'company_datasource_id': 9998,
            'total_ratings': 1234,
            'total_reviews': None,
            'review_stats_date': '2022-06-12 20:00:00',
            'data_version': 1,
            'average_rating': 4.3,
        },
        {
            'company_datasource_id': 9998,
            'total_ratings': 2345,
            'total_reviews': None,
            'review_stats_date': '2022-06-15 20:00:00',
            'data_version': 2,
            'average_rating': 4.5,
        }
    ]


    """
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db[f"{nlp_type}_review_stats"].find(
        filter={
            "company_datasource_id": company_datasource_id,
        },
        projection={
            "_id": 0,
            "company_datasource_id": 1,
            "total_reviews": 1,
            "total_ratings": 1,
            "average_rating": 1,
            "review_stats_date": 1,
            "data_version": 1
        },
    ).sort([("data_version", pymongo.DESCENDING)])

    query_results = []
    for item in cursor:
        # NOTE: either total_ratings or total_reviews is available
        # if one is available, the other is null
        if "total_ratings" not in item:
            item["total_ratings"] = None
        elif "total_reviews" not in item:
            item["total_reviews"] = None

        query_results.append(item)

    query_results = list(
        map(
            lambda x: {
                **x,
                'temp_review_stats_date': dateparser.parse(x['review_stats_date'], settings={"TIMEZONE": "US/Eastern"})
            },
            query_results
        )
    )
    query_results.sort(key=itemgetter(
        'temp_review_stats_date', 'data_version'), reverse=True)
    list(map(lambda x: x.pop('temp_review_stats_date'), query_results))

    return query_results


def fetch_company_stats_data(company_datasource_id: int):
    db = mongo_client[config.MONGODB_DB_NAME]
    cursor = db["coresignal_stats"].find(
        filter={
            "company_datasource_id": company_datasource_id,
        },
        projection={
            "_id": 0,
            "company_datasource_id": 1,
            "employees_count": 1,
            "stats_collection_date": 1,
            "data_version": 1
        },
    ).sort([("data_version", pymongo.DESCENDING)])

    query_results = []
    for item in cursor:
        query_results.append(item)

    return query_results
