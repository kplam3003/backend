import os
import datetime
import sys
import time
import json
import  math
import random as rd
import logger
import config

from google.cloud import pubsub_v1
from google.oauth2 import service_account

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

class GCPPubsubSender:
    def __init__(self, project_id, topic_name, service_account_key_path):
        self._credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
        self.__publisher = pubsub_v1.PublisherClient(credentials = self._credentials)
        self.__topic_path = self.__publisher.topic_path(project_id, topic_name)

    def send(self, payload):
        def default(o):
            if isinstance(o, (dt.date, dt.datetime)):
                return o.isoformat()
        data = json.dumps(payload, default=default)
        data = data.encode("utf-8")
        future = self.__publisher.publish(self.__topic_path, data=data)
        result = future.result()
        logger.info(f"Published a message {result}")

class GCPPubsubReceiver:
    def __init__(self, project_id, pubsub_subscription, service_account_key_path):
        self._credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
        self.__subscriber = pubsub_v1.SubscriberClient(credentials = self._credentials)
        self.__subscription_path = self.__subscriber.subscription_path(project_id, pubsub_subscription)

    def pull_message(self, max_messages):
        response = self.__subscriber.pull(
                request={"subscription": self.__subscription_path, "max_messages": max_messages}
            )
        return response

    def ack_messages(self, ack_ids):
        self.__subscriber.acknowledge(
                    request={"subscription": self.__subscription_path, "ack_ids": ack_ids}
                )
