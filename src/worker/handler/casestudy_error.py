import logger
import config

from database import auto_session, CaseStudy
# from sqlalchemy.orm.attributes import flag_modified

# Topic: dev_cs_error
# Sub: dev_cs_error_webplatform

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

ERROR_CODE_IN_PROGRESS = "IN_PROGRESS"
ERROR_CODE_FAILED = "FAILED"


@auto_session
def handle_casestudy_error_task(payloads, session=None):
    """
    Args:
        payloads ([type]): [description]
        session ([type], optional): [description]. Defaults to None.
    """
    for payload in payloads:
        if not payload:
            logger.error("NULL payload.")
            continue

        logger.info(f"Begin task: {payload}")
        case_study_id = payload.get("case_study_id")
        error_text = payload.get("error")
        error_code = payload.get("error_code")

        if error_code not in [ERROR_CODE_FAILED, ERROR_CODE_IN_PROGRESS]:
            logger.error(f"Invalid error code: error_code={error_code}")
            continue

        case_study = (
            session.query(CaseStudy).filter(CaseStudy.id == case_study_id).first()
        )
        if not case_study:
            logger.error(f"Cannot find Case Study: case_study_id={case_study_id}")
            continue

        crawl_progress = case_study.crawl_progress.get("progress")
        if crawl_progress == 1:
            logger.error(f"Received error after case study finish - Ignore error: case_study_id={case_study_id}")
            continue

        logger.info(f"Received error: {error_text}")
        current_errors = case_study.crawl_error_reason
        current_errors.append(error_text)
        case_study.set_jsonb_attr("crawl_error_reason", list(set(current_errors)))
        if error_code == ERROR_CODE_FAILED:
            case_study.status = error_code

        session.commit()
        logger.info(f"End task: {payload}")
