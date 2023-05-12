import logger
import config

# to be env
from database import auto_session, CaseStudy
from src.services import casestudy as service

import gevent.monkey
gevent.monkey.patch_socket()

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


def handle_casestudy_progress_task(payloads):
    tasks = [gevent.spawn(handle_task, payload) for payload in payloads]
    gevent.joinall(tasks)


@auto_session
def handle_task(payload, session=None):
    try:
        logger.info(f"Begin task: {payload}")
        if not payload:
            logger.error("NULL payload.")
            return

        case_study_id = payload.get("case_study_id")
        progress_value = payload.get("progress")

        case_study = session.query(CaseStudy).filter(
            CaseStudy.id == case_study_id,
            CaseStudy.is_active
        ).first()
        if not case_study:
            logger.error(f"Invalid/In active case study {case_study_id}")
            return
            
        logger.info(f"""Current progress: {case_study.crawl_progress["progress"]}""")

        if progress_value == 1 and case_study.crawl_progress["progress"] != 1:
            service.update_case_study_dimension_config_statistic_V110(case_study_id)
            service.update_npl_type_latest_review_date(case_study_id)
            service.update_finished_case_study_status(case_study_id)
            service.update_case_study_company_data_source_statistic(case_study_id)
            # service.prepare_chart_data(case_study_id)
            service.update_voe_chart_filter_data(case_study_id)
            service.update_company_data_source_nlp_statistic(case_study_id)
            service.update_case_study_dimension_config_info_statistic(case_study_id)
            service.update_chart_filter_data(case_study_id)

            
        if not service.update_case_study_progress(case_study_id, payload):
            logger.error(f"Can not update crawl progress: case_study_id={case_study_id}, progress={progress_value}")
            return

        logger.info(f"End task: {payload}")
    except Exception as e:
        logger.exception(e)
