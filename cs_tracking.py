import time
import logger
import config

from database import Session, CaseStudy
from datetime import datetime

from src.services.email import send_announcement_email, send_report_email

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
TIME_SLEEP = 3600
REPORT_TIME = 43200
FINISHED_TYPE = "finished"
TIME_OUT_TYPE = "timeout"


def track_cs(case_study):
    cs_progress = case_study.crawl_progress.get("progress", 0)
    if cs_progress == 1:
        logger.info(
            f"CS Tracking, cs finished: {case_study.id}")
        case_study.is_tracking = False
        return FINISHED_TYPE, case_study.id, case_study.name

    current_time = datetime.now()
    wait_time = (current_time - case_study.update_time).total_seconds()

    if wait_time >= REPORT_TIME:
        logger.info(
            f"CS Tracking, cs run too long: {case_study.id}")
        case_study.update_time = current_time
        return TIME_OUT_TYPE, case_study.id, case_study.name

    return "", 0, ""


def main():
    while True:
        session = Session()
        list_finished_case_study_objects_info = []
        list_report_case_study_objects_info = []

        logger.info(
            f"CS Tracking, checked db -----------------------------")
        case_studies = session.query(CaseStudy).filter(
            CaseStudy.is_active, CaseStudy.is_tracking)

        cs_ids = [str(cs.id) for cs in case_studies]
        logger.info(
            f"CS Tracking, list tracking cs: {', '.join(cs_ids)}")

        for case_study in case_studies:
            try:
                type, cs_id, cs_name = track_cs(case_study)
                if type == FINISHED_TYPE:
                    list_finished_case_study_objects_info.append(
                        {"id": cs_id, "name": cs_name})

                if type == TIME_OUT_TYPE:
                    list_report_case_study_objects_info.append(
                        {"id": cs_id, "name": cs_name})

            except Exception as e:
                logger.error(f"CS Tracking, ERROR: {e}")
                continue

        if list_finished_case_study_objects_info:
            send_announcement_email(list_finished_case_study_objects_info)

        if list_report_case_study_objects_info:
            send_report_email(list_report_case_study_objects_info)

        session.commit()
        session.close()
        time.sleep(TIME_SLEEP)


if __name__ == "__main__":
    main()
