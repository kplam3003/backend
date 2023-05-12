import config
import datetime
import gevent
import json
import logger
import service
import time

from datamart import hra, voc, voe
from google.cloud import pubsub_v1

import gevent.monkey
gevent.monkey.patch_socket()

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

chart_collecter = {}
chart_collecter['VOC_1_1'] = voc.fetch_voc_1_1
chart_collecter['VOC_1_2'] = voc.fetch_voc_1_2
chart_collecter['VOC_1_3_1'] = voc.fetch_voc_1_3_1
chart_collecter['VOC_1_3_2'] = voc.fetch_voc_1_3_2
chart_collecter['VOC_1_4'] = voc.fetch_voc_1_4
chart_collecter['VOC_1_4_1'] = voc.fetch_voc_1_4_1
chart_collecter['VOC_1_5'] = voc.fetch_voc_1_5
chart_collecter['VOC_1_5_1'] = voc.fetch_voc_1_5_1
chart_collecter['VOC_1_6'] = voc.fetch_voc_1_6
chart_collecter['VOC_1_7'] = voc.fetch_voc_1_7
chart_collecter['VOC_1_8'] = voc.fetch_voc_1_8
chart_collecter['VOC_1_9'] = voc.fetch_voc_1_9
chart_collecter['VOC_2'] = voc.fetch_voc_2
chart_collecter['VOC_3'] = voc.fetch_voc_3
chart_collecter['VOC_4_1'] = voc.fetch_voc_4_1
chart_collecter['VOC_4_2'] = voc.fetch_voc_4_2
chart_collecter['VOC_4_3'] = voc.fetch_voc_4_3
chart_collecter['VOC_4_4'] = voc.fetch_voc_4_4
chart_collecter['VOC_5'] = voc.fetch_voc_5
chart_collecter['VOC_6_1'] = voc.fetch_voc_6_1
chart_collecter['VOC_6_2'] = voc.fetch_voc_6_2
chart_collecter['VOC_6_3'] = voc.fetch_voc_6_3
chart_collecter['VOC_6_4'] = voc.fetch_voc_6_4
chart_collecter['VOC_6_5'] = voc.fetch_voc_6_5
chart_collecter['VOC_6_6'] = voc.fetch_voc_6_6
chart_collecter['VOC_6_7'] = voc.fetch_voc_6_7
chart_collecter['VOC_6_7_1'] = voc.fetch_voc_6_7_1
chart_collecter['VOC_6_8'] = voc.fetch_voc_6_8
chart_collecter['VOC_6_9'] = voc.fetch_voc_6_9
chart_collecter['VOC_11'] = voc.fetch_voc_11
chart_collecter['VOC_12'] = voc.fetch_voc_12
chart_collecter['VOC_12_1'] = voc.fetch_voc_12_1
chart_collecter['VOC_12_2'] = voc.fetch_voc_12_2
chart_collecter['VOC_13'] = voc.fetch_voc_13
chart_collecter['VOC_13_1'] = voc.fetch_voc_13_1
chart_collecter['VOC_13_2'] = voc.fetch_voc_13_2
chart_collecter['VOC_14'] = voc.fetch_voc_14
chart_collecter['VOC_15'] = voc.fetch_voc_15
chart_collecter['VOC_16'] = voc.fetch_voc_16
# VoE charts

chart_collecter['VOE_1_2'] = voe.fetch_voe_1_2
chart_collecter["VOE_1_3_2"] = voe.fetch_voe_1_3_2
chart_collecter["VOE_1_4"] = voe.fetch_voe_1_4
chart_collecter["VOE_1_5"] = voe.fetch_voe_1_5
chart_collecter["VOE_1_6"] = voe.fetch_voe_1_6
chart_collecter["VOE_1_7"] = voe.fetch_voe_1_7
chart_collecter["VOE_2"] = voe.fetch_voe_2
chart_collecter["VOE_3"] = voe.fetch_voe_3
chart_collecter['VOE_4_2'] = voe.fetch_voe_4_2
chart_collecter['VOE_5'] = voe.fetch_voe_5
chart_collecter["VOE_6_1"] = voe.fetch_voe_6_1
chart_collecter["VOE_6_2"] = voe.fetch_voe_6_2
chart_collecter["VOE_6_3"] = voe.fetch_voe_6_3
chart_collecter["VOE_6_5"] = voe.fetch_voe_6_5
chart_collecter["VOE_6_6"] = voe.fetch_voe_6_6
chart_collecter["VOE_6_8"] = voe.fetch_voe_6_8
chart_collecter["VOE_6_9"] = voe.fetch_voe_6_9
chart_collecter['VOE_7_1'] = voe.fetch_voe_7_1
chart_collecter['VOE_7_2'] = voe.fetch_voe_7_2
chart_collecter['VOE_7_3'] = voe.fetch_voe_7_3
chart_collecter['VOE_7_4'] = voe.fetch_voe_7_4
chart_collecter['VOE_7_5'] = voe.fetch_voe_7_5
chart_collecter['VOE_7_6'] = voe.fetch_voe_7_6
chart_collecter['VOE_8_1'] = voe.fetch_voe_8_1
chart_collecter['VOE_8_4'] = voe.fetch_voe_8_4
chart_collecter['VOE_9_1'] = voe.fetch_voe_9_1
chart_collecter['VOE_9_3'] = voe.fetch_voe_9_3
chart_collecter['VOE_9_5'] = voe.fetch_voe_9_5
chart_collecter['VOE_9_5_1'] = voe.fetch_voe_9_5_1
chart_collecter['VOE_9_5_2'] = voe.fetch_voe_9_5_2
chart_collecter['VOE_9_4'] = voe.fetch_voe_9_4
chart_collecter['VOE_9_4_1'] = voe.fetch_voe_9_4_1
chart_collecter['VOE_9_4_2'] = voe.fetch_voe_9_4_2
chart_collecter['VOE_10_1'] = voe.fetch_voe_10_1
chart_collecter['VOE_11'] = voe.fetch_voe_11
chart_collecter['VOE_11_1'] = voe.fetch_voe_11_1
chart_collecter['VOE_12'] = voe.fetch_voe_12
chart_collecter['VOE_12_1'] = voe.fetch_voe_12_1
chart_collecter['VOE_12_2'] = voe.fetch_voe_12_2
chart_collecter['VOE_13'] = voe.fetch_voe_13
chart_collecter['VOE_14'] = voe.fetch_voe_14
chart_collecter['VOE_15'] = voe.fetch_voe_15
chart_collecter['VOE_16'] = voe.fetch_voe_16
# HRA charts

chart_collecter['HRA_1'] = hra.fetch_hra_1


def handle_task(payload):
    logger.info(f"Begin task: {payload}")
    code_name = payload['params']['code_name']
    data = chart_collecter[code_name](payload)

    if not service.update_data(payload['key'], data):
        logger.error(
            f"NOT UPDATE DATA FOR TASK: key={payload['key']}, data={data}")
    # logger.info(f"Chart data: {data}")
    logger.info(f"End task: {payload}")


def handle_casestudy_chart_task(items):
    tasks = [gevent.spawn(handle_task, item) for item in items]
    gevent.joinall(tasks)
    time.sleep(1)


# support serialize datetime
def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()


def publish(logger, gcp_product_id, topic, payload):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_product_id, topic)
    data = json.dumps(payload, default=default)

    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    result = future.result()
    logger.info(f"Published a message {result}")
