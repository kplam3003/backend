import config
import const
import datetime as dt
import logger
import json

from elasticsearch import Elasticsearch
from database import Task, auto_session, CaseStudy, CaseStudySchedule, \
    NLPType, Chart
from database import CASE_STUDY_STATUS_APPROVED
from datetime import datetime
from google.cloud import pubsub_v1


logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


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


@auto_session
def request_task(key=None, params=None, session=None):
    assert key is not None, "key is not null"
    assert params is not None, "params is not null"

    task = session.query(Task).filter(Task.key == key).first()

    if task:
        # return if avaliable
        # if task.status == const.StatusEnum.SUCCESS.value:
        #     return task.to_json()

        # renew task if too late
        # n_seconds_ago = datetime.datetime.utcnow() - timedelta(seconds=config.GCP_BQ_TIMEOUT)
        # n_seconds_ago = n_seconds_ago.replace(tzinfo=None)
        # task_created_at = task.created_at.replace(tzinfo=None)
        # if task_created_at < n_seconds_ago:
        #     task.status = const.StatusEnum.RENEW.value
        #     session.commit()
        #     publish(logger, config.GCP_DATA_PLATFORM_PROJECT_ID, config.GCP_PUBSUB_CS_CHART_TOPIC, task.to_json())
        task_payload = task.to_json()
        del task_payload['data']
        publish(logger, config.GCP_DATA_PLATFORM_PROJECT_ID, config.GCP_PUBSUB_CS_CHART_TOPIC, task_payload)

        return task.to_json()
    else:  # new task
        task = Task(
            key=key,
            params=params,
            status=const.StatusEnum.NEW.value
        )
        session.add(task)
        session.commit()
        publish(logger, config.GCP_DATA_PLATFORM_PROJECT_ID, config.GCP_PUBSUB_CS_CHART_TOPIC, task.to_json())
        return task.to_json()


@auto_session
def update_data(key=None, data=None, session=None):
    assert key is not None, "key is not null"
    assert data is not None, "data is not null"

    task = session.query(Task).filter(Task.key == key).first()

    if not task:
        return False

    task.status = const.StatusEnum.SUCCESS.value
    task.data = data

    session.commit()

    return task.to_json()


def sync_request(payload=None):
    assert payload is not None, "payload is not null"
    try:
        publish(logger, config.GCP_DATA_PLATFORM_PROJECT_ID, config.GCP_PUBSUB_SCHEDULE_TOPIC, payload)
        return True
    except Exception as error:
        logger.error(f"[SYNC_REQUEST] payload={payload}, error={error}")
        return False


def list_case_study_schedule(session):
    return session.query(CaseStudySchedule)\
        .join(CaseStudy)\
        .filter(
            CaseStudy.is_active,
            CaseStudy.status == CASE_STUDY_STATUS_APPROVED,
            CaseStudySchedule.is_active,
            CaseStudySchedule.end_date >= datetime.utcnow()
        ).all()


@auto_session
def get_case_study_schedule(case_study_id, session=None):
    case_study_schedule = session.query(CaseStudySchedule)\
        .join(CaseStudy)\
        .filter(
            CaseStudy.status == CASE_STUDY_STATUS_APPROVED,
            CaseStudy.id == case_study_id
        ).first()
    if case_study_schedule:
        return case_study_schedule.to_json()
    return False


@auto_session
def get_export_chart_mapping_by_nlp_type_id(nlp_type_id, session=None):
    nlp_type_obj = session.query(NLPType).filter(NLPType.id == nlp_type_id).first()

    if not nlp_type_obj:
        return []

    nlp_type_name_upper = nlp_type_obj.name.upper()

    datamart_table_chart_mapping = config.CHART_DATA_MART_TABLE_MAPPING

    return_data = []
    charts = session.query(Chart).filter(
        Chart.nlp_type_id == nlp_type_id,
        Chart.is_active
    ).order_by(Chart.code_name)
    if not charts:
        return []

    for chart in charts:
        upper_chart_code = chart.code_name.upper()
        chart_data = dict()
        try:
            chart_data["code"] = upper_chart_code
            chart_data["table"] = datamart_table_chart_mapping[nlp_type_name_upper][upper_chart_code]
            cleaned_chart_name = "".join([s for s in chart.name if s == " " or s.isalnum()])
            chart_data["prefix"] = cleaned_chart_name.lower().replace(" ", "_")
            return_data.append(chart_data)
        except Exception as e:
            logger.error(f"Get export chart mapping error: chart {upper_chart_code} === {e}")

    for key, value in config.NLP_DATA_MART_TABLE_MAPPING[nlp_type_name_upper].items():
        raw_data_dict = dict()
        raw_data_dict["code"] = key
        raw_data_dict["table"] = value["table"]
        raw_data_dict["prefix"] = value["prefix"]
        return_data.append(raw_data_dict)

    return return_data

def get_case_study_nlp_output(case_study_id, request_param_dict=None):
    logger.info(f"Case study raw data: case study id {case_study_id} - params: {request_param_dict}")
    return_data = {}
    return_data["count"] = 0
    return_data["results"] = []

    try:
        if request_param_dict is None:
            request_param_dict = dict()

        def _generate_sorting_dict():
            sort_query_dict = {}
            sort_query_string = request_param_dict.get("sorting")
            if not sort_query_string:
                return {
                "review_date": "ASC",
                "review_id": "ASC",
                "label": "ASC"
            }

            sort_query_list = sort_query_string.split(",")

            special_column_map = {
                "created_date": "review_date",
                "company": "company_name",
            }

            for item in sort_query_list:
                column_name = special_column_map.get(item, item)
                sort_type = "ASC"
                if item.startswith("-"):
                    column_name = special_column_map.get(item[1:], item[1:])
                    sort_type = "DESC"

                sort_query_dict[column_name] = sort_type

            return sort_query_dict

        sort_query_dict = _generate_sorting_dict()

        query_dict = {}

        page = 1
        try:
            page = int(request_param_dict.get("page"))
        except:
            pass
        page_size = 10
        try:
            page_size = int(request_param_dict.get("page_size"))
        except:
            pass

        from_index = (page - 1) * page_size
        size = page_size
        query_dict["from"] = from_index
        query_dict["size"] = size

        query_dict["sort"] = []
        if sort_query_dict:
            for key, value in sort_query_dict.items():
                sort_item_tmp = {key: value.lower()}
                query_dict["sort"].append(sort_item_tmp)

        query_body_dict = query_dict.setdefault("query", {})
        query_body_dict["bool"] = {}
        query_body_dict["bool"]["must"] = []

        query_body_dict["bool"]["must"].append(
            {
                "range": {
                    "review_date": {
                        "gte": request_param_dict.get("start_date"),
                        "lte": request_param_dict.get("end_date")
                    }
                }
            }
        )

        match_in_list_fields = [
            ("company_name", "company_names"),
            ("source_name", "source_names"),
            ("dimension", "dimensions"),
            ("label", "labels"),
            ("polarity", "polarities")
        ]
        for match_in_list_field in match_in_list_fields:
            query_string = request_param_dict.get(match_in_list_field[1], "")
            if not query_string:
                continue
            value_list = json.loads(query_string)

            query_tmp = {}
            query_tmp["bool"] = {}

            query_tmp["bool"].setdefault("should", [])
            for value in value_list:
                query_tmp["bool"]["should"].append(
                    {
                        "term": {
                            match_in_list_field[0]: value
                        }
                    }
                )
            query_body_dict["bool"]["must"].append(query_tmp)

        keywords_string = request_param_dict.get("keywords", "")
        if keywords_string:
            keywords = json.loads(keywords_string)

            for keyword in keywords:
                query_tmp = {}
                query_tmp["match"] = {
                            "trans_review": {
                                "query": keyword
                            }
                        }

                query_body_dict["bool"]["must"].append(query_tmp)


        es_client = Elasticsearch(
            hosts=[config.ELASTICSEARCH_HOST]
        )

        case_study_index = f"{config.CASE_STUDY_NLP_INDEX_PREFIX}{case_study_id}"

        # Count query dict only support query
        count_query_dict = {}
        count_query_dict["query"] = query_dict["query"]
        count_res = es_client.count(index=case_study_index, body=count_query_dict)
        count = count_res["count"]
        return_data["count"] = count
        if count == 0:
            return return_data
        if page_size > count:
            query_dict["size"] = count

        logger.info(f"Query to ES with index: {case_study_index}, query: {query_dict}")
        response = es_client.search(index=case_study_index, body=query_dict)
        for result in response["hits"]["hits"]:
            return_data["results"].append(
                result["_source"]
            )

        logger.info(f"Success get case study nlp output: {case_study_index}, query: {query_dict}")

    except Exception as error:
        logger.error(f"Error while get case study NLP output: error={error}")

    return return_data
