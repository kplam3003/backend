from jsonschema import validate
import logger
import config
from google.cloud import pubsub_v1
import json
import time

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


def get_sorted_query_string(request):
    params = request.args.to_dict()
    sorted_params = []
    for key in sorted(params.keys()):
        sorted_params.append(f"{key}={params[key]}")
    return "&".join(sorted_params)


schema = {
    "type": "object",
    "properties": {
        "case_study_id": {
            "type": "number"
        },
        "case_study_name": {
            "type": "string"
        },
        "dimension_config_id": {
            "type": "number"
        },
        "polarities": {
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "properties": {
                        "nlp_polarity": {
                            "type": "string"
                        },
                        "modified_polarity": {
                            "type": "number"
                        },
                    },
                    "required": ["nlp_polarity", "modified_polarity"]
                }

            ]
        },
        "companies": {
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "properties": {
                        "company_name": {
                            "type": "string"
                        },
                        "source_name": {
                            "type": "string"
                        },
                        "source_id": {
                            "type": "number"
                        },
                        "nlp_pack": {
                            "type": "string"
                        },
                        "nlp_type": {
                            "type": "string"
                        },
                        "is_target": {
                            "type": "boolean"
                        },
                        "url": {
                            "type": "string"
                        }
                    },
                    "required": ["company_name", "source_name", "source_id", "nlp_pack", "nlp_type", "is_target", "url"]
                }
            ]
        }
    },
    "required": ["case_study_id", "case_study_name", "polarities"]
}


def is_valid_request_payload(payload):
    try:
        validate(instance=payload, schema=schema)
        return True
    except Exception as error:
        logger.error(f"[JSON_ERROR] payload={payload}, error={error}")
        return False


def publish(logger, gcp_product_id, topic, payload):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_product_id, topic)
    data = json.dumps(payload)

    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    result = future.result()
    logger.info(f"Published a message {result}")


def subscribe(logger, gcp_product_id, gcp_pubsub_subscription, handle_task, delay):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        gcp_product_id, gcp_pubsub_subscription)
    with subscriber:
        while True:
            # The subscriber pulls a message.
            logger.info(
                f"Waiting for a message on {gcp_pubsub_subscription}...")
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 1}
            )

            if not response:
                time.sleep(delay)
                continue

            ack_ids = []
            messages = []
            for received_message in response.received_messages:
                logger.info(f"Received: {received_message.message.data}.")
                ack_ids.append(received_message.ack_id)
                messages.append(received_message.message.data)

            # Acknowledges the received messages so they will not be sent again.
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

            for message in messages:
                try:
                    data = json.loads(message, strict=False)
                    handle_task(data)
                except Exception as error:
                    logger.error(f"Error: {error}")


def construct_final_query(nlp_type, chart_code, filter_query, logger):
    """
    Construct a final query statement to be sent to BigQuery to fetch chart data

    Parameters
    ----------
    nlp_type : string
        either 'VOC' or 'VOE'
    chart_code : string
        This is the code name for chart.
        Must match exactly the keys of config.CHART_DATA_MART_TABLE_MAPPING['VOC'] (all uppercase)
    filter_query : string
        Filter query for chart data
    logger : logger object

    Returns
    -------
    string
        Final query to be sent to BigQuery
    """
    try:
        # prepare some variables
        datamart_path = f'./datamart/sql/{nlp_type.strip().lower()}/'
        summary_table_file = f'{nlp_type.strip().lower()}_summary_table.sql'
        nlp_type = nlp_type.strip().upper()

        # prepare first component: summary_table query
        with open(datamart_path + summary_table_file, 'r') as file:
            summary_table_sql = file.read()

        # prepare third component: chart query
        valid_chart_codes = list(
            config.CHART_DATA_MART_SCRIPT_MAPPING[nlp_type].keys())
        if chart_code.upper() not in valid_chart_codes:
            raise Exception(
                f'Invalid chart_code {chart_code.upper()}. Must be one of {valid_chart_codes}')

        chart_sql_path = datamart_path + f'{chart_code.lower()}.sql'
        with open(chart_sql_path, 'r') as file:
            chart_sql = file.read()

        # voc_chart_sql always ends with ',' due to it containing only CTE
        # WITH keyword is already provided within voc_summary_table_sql
        summary_table_sql = summary_table_sql.strip()
        filter_query = filter_query.strip()
        chart_sql = chart_sql.strip()

        if filter_query.lower().startswith('with'):
            # omit WITH if filter query starts with a CTE
            final_filter_query = filter_query[5:]
        elif filter_query.lower().startswith('select'):
            if chart_sql.endswith(','):
                # omit ',' at the end if filter query starts with SELECT
                chart_sql = chart_sql[:-1]
            final_filter_query = filter_query[:]  # keep as-is
        else:
            raise Exception('Invalid start for filter_query')

        # construct final query
        final_query = '\n'.join(
            [summary_table_sql, chart_sql, final_filter_query])

        # inject configs into final_query
        final_query = (
            final_query
            .replace('GCP_DATA_PLATFORM_PROJECT_ID', config.GCP_DATA_PLATFORM_PROJECT_ID)
            .replace('BQ_DATASET_DATAMART_CS', config.BQ_DATASET_DATAMART_CS)
            .replace('BQ_DATASET_STAGING', config.BQ_DATASET_STAGING)
            .replace('BQ_DATASET', config.BQ_DATASET)
        )
        final_query = f'-- CHART {chart_code.upper()}\n' + final_query

        return final_query

    except Exception as error:
        logger.exception(f'Error happens during query construction!. {error}')
        return None
