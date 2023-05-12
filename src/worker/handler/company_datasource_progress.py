import logger
import config

from src.services.company_data_source import update_company_datasource_progress

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
PAYLOAD_EVENT_PROGRESS = "progress"

def handle_task(payload):
    try:
        logger.info(f"Begin task: {payload}")
        if not payload:
            logger.error("NULL payload.")
            return

        '''Payload example
            {
                "event": "progress",
                "company_datasource_id": 8888,
                "progress": 0.6832706766917294,
                "details": {
                    "crawl": 0.9924812030075187,
                    "preprocess": 0.6109022556390977,
                    "translate": 0.5770676691729323,
                    "load": 0.5526315789473685
                }
            }
        '''
        if payload["event"] != PAYLOAD_EVENT_PROGRESS:
            logger.error(f"Invalid payload event type {payload['event']}.")
            return
        company_datasource_id = payload.get("company_datasource_id")
        progress_value = payload.get("progress")
        if not company_datasource_id:
            logger.error(f"Invalid company_datasource_id {company_datasource_id}.")
            return

        if not update_company_datasource_progress(company_datasource_id, payload):
            logger.error(f"Can not update crawl progress: company_datasource_id={company_datasource_id}, progress={progress_value}")
            return

        logger.info(f"End task: {payload}")
    except Exception as e:
        logger.exception(e)

def handle_company_datasource_progress_task(payloads):
    for payload in payloads:
        handle_task(payload)
