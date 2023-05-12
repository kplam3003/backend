import json
import time
import logger
import config
import click

from google.cloud import pubsub_v1
from handler import handle_casestudy_error_task
from handler import handle_casestudy_progress_task
from handler import handle_casestudy_chart_task
from handler import handle_company_datasource_progress_task
from handler import handle_company_datasource_error_task

# to be env
MAX_MESSAGES = 100
POOL_SIZE = 100
DELAY_TIME = 300

# Topic: dev_cs_error
# Sub: dev_cs_error_webplatform

# Topic:
# Sub: dev_cs_progress_webplatform

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


WORKER_HANDLER = {
    "casestudy_error": handle_casestudy_error_task,
    "casestudy_progress": handle_casestudy_progress_task,
    "casestudy_chart": handle_casestudy_chart_task,
    "company_datasource_progress": handle_company_datasource_progress_task,
    "company_datasource_error": handle_company_datasource_error_task
}


def subscribe(
    logger, gcp_product_id, gcp_pubsub_subscription, handler, delay_time=None,
    executor=None, max_messages=MAX_MESSAGES, ack_deadline_seconds=None
):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        gcp_product_id, gcp_pubsub_subscription
    )

    if delay_time:
        delay_time = int(delay_time)
    with subscriber:
        while True:
            # The subscriber pulls a message.
            logger.info(f"Waiting for a message on {gcp_pubsub_subscription}...")
            response = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": max_messages,
                }
            )

            if not response and delay_time:
                time.sleep(delay_time)
                continue

            ack_ids = []
            messages = []
            for received_message in response.received_messages:
                logger.info(f"Received: {received_message.message.data}.")
                ack_ids.append(received_message.ack_id)
                messages.append(received_message.message.data)

            payloads = map(
                lambda message: json.loads(message, strict=False), messages
            )

            if ack_deadline_seconds:
                subscriber.modify_ack_deadline(
                    request={
                        "subscription": subscription_path,
                        "ack_ids": ack_ids,
                        "ack_deadline_seconds": ack_deadline_seconds,
                    }
                )

            if executor:
                try:
                    executor.submit(handler, payloads)
                except Exception as error:
                    logger.error(f"Exception: error={error}, messages={messages}")
                finally:
                    # Acknowledges the received messages so they will not be sent again.
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": ack_ids}
                    )
            else:
                try:
                    handler(payloads)
                    # Acknowledges the received messages so they will not be sent again.
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": ack_ids}
                    )
                except Exception as error:
                    logger.error(f"Exception: error={error}, messages={messages}")
                finally:
                    # logger.info(f"Acknowledging {ack_ids}")
                    # Acknowledges the received messages so they will not be sent again.
                    subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": ack_ids}
                    )


@click.command()
@click.option("--name", help="Worker name")
@click.option("--delay", default=None, help="Interval time to check payload")
@click.option("--subscription", default=None, help="")
@click.option("--topic", default=None, help="")
@click.option("--max-messages", default=100, help="")
@click.option("--ack-deadline-seconds", default=None, help="", type=(int))
def launch_worker(name, delay, subscription, topic, max_messages, ack_deadline_seconds):
    """Simple CLI accepts task name and trigger handler"""
    subscribe(
        logger,
        config.GCP_DATA_PLATFORM_PROJECT_ID,
        subscription,
        WORKER_HANDLER[name],
        delay_time=delay,
        max_messages=max_messages,
        ack_deadline_seconds=ack_deadline_seconds
    )


if __name__ == "__main__":
    launch_worker()
