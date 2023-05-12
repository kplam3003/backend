from logging import error
import logger
import config

from src.services.company_data_source import update_company_datasource_error

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
PAYLOAD_EVENT_ERROR = "error"
PAYLOAD_EVENT_ERROR_CODE_FAILED = "FAILED"

def handle_task(payload):
    try:
        logger.info(f"Begin task: {payload}")
        if not payload:
            logger.error("NULL payload.")
            return

        '''Payload 1: error
                {
                    "event": "error",
                    "company_datasource_id": 235,
                    "error": "error message",
                    "error_code": "IN_PROGRESS",
                    'data_type' : 'job'/'overview'/'review'
                }
           Payload 2: complete failure
                {
                "event": "error",
                "company_datasource_id": 235,
                "error": "error message",
                "error_code": "FAILED",
                'data_type' : 'job'/'overview'/'review'
                }
        '''
        if payload["event"] != PAYLOAD_EVENT_ERROR:
            logger.error(f"Invalid payload event type {payload['event']}.")
            return
        company_datasource_id = payload.get("company_datasource_id")
        error_code = payload.get("error_code")
        if not company_datasource_id:
            logger.error(f"Invalid company_datasource_id {company_datasource_id}.")
            return
        if not error_code or error_code != PAYLOAD_EVENT_ERROR_CODE_FAILED:
            logger.info(f"Invalid error code or not {PAYLOAD_EVENT_ERROR_CODE_FAILED} code. Skipping...")
            return

        # Only lookup error code FAILED and update company datasource status
        if not update_company_datasource_error(company_datasource_id, payload):
            logger.error(f"Can not update error progress: company_datasource_id={company_datasource_id}, progress={error_code}")
            return

        logger.info(f"End task: {payload}")
    except Exception as e:
        logger.exception(e)

def handle_company_datasource_error_task(payloads):
    for payload in payloads:
        handle_task(payload)
