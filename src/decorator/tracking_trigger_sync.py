import functools
import logger
import config
import sys

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


def tracking_data_trigger_sync(func):
    data_storage = {}

    def tracker(frame, event, arg):
        nonlocal data_storage
        data_storage = frame.f_locals.copy() if event == 'return' else ...

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        sys.setprofile(tracker)
        try:
            func(*args, **kwargs)
            sys.setprofile(None)
            health_checks = data_storage.get('result')['data']
            wrong_data = list(filter(lambda health_check: not isinstance(health_check, dict), health_checks))
            if wrong_data:
                logger.warning("Some data not right")
                logger.warning(f"wrong data: {wrong_data}")
                logger.warning(f"details from bigquery: {data_storage.get('health_check_report')['details']}")
                logger.warning(f"report_failed_by_timeout: {data_storage.get('report_failed_by_timeout')}")
                logger.warning(f"report_failed_trigger: {data_storage.get('report_failed_trigger')}", )
                logger.warning(
                    f"report_company_data_source_passed: {data_storage.get('report_company_data_source_passed')}")
                logger.warning(f"total data: {health_checks}")
        except Exception as e:
            logger.error(f"error at function {func.__name__}: {e}")

    return wrapper
