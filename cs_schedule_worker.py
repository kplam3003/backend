import json
import time
import logger
import config
import helper
import service
import threading
import queue
import schedule

from database import auto_session, Task, CaseStudySchedule, CaseStudy
from dateutil.parser import parse
from datetime import datetime
from vedis import Vedis

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
DELAY_TIME = 30
PUBSUB_DELAY_TIME = 10

task_queue = queue.Queue()
tag_storage = Vedis(':mem:')

RUN_AT_FIELD = "run_at"
CRAWL_ON_APPROVAL_FIELD = "crawl_on_approval"
SCHEDULE_TIME_FIELD = "schedule_time"
START_DATE_FIELD = "start_date"
END_DATE_FIELD = "end_date"
TYPE_FIELD = "type"
EVENT_TYPE = "action"
EVENT_TYPE_UPDATE = "update"

TYPE_FIELD_VALUE_NO_REPEAT = "no_repeat"
TYPE_FIELD_VALUE_DAILY = "daily"
TYPE_FIELD_VALUE_WEEKLY = "weekly"
TYPE_FIELD_VALUE_MONTHLY = "monthly"

TYPE_TASK = '__type_task__'
TYPE_TASK_FROM_PUBSUB = 'PUBSUB'
TYPE_TASK_FROM_DB = 'DB'


@auto_session
def trigger_sync(payload, session=None):
    case_study_schedule = session.query(CaseStudySchedule)\
        .join(CaseStudy)\
        .filter(
            CaseStudy.status == service.CASE_STUDY_STATUS_APPROVED,
            CaseStudy.id == payload["case_study_id"]
    ).first()

    payload = case_study_schedule.payload
    logger.info(
        f"Begin - Send Case Study request to DP with payload: {payload}")
    helper.publish(
        logger, config.GCP_DATA_PLATFORM_PROJECT_ID,
        config.GCP_PUBSUB_REQUEST_TOPIC, payload
    )
    if payload[EVENT_TYPE] != EVENT_TYPE_UPDATE:
        payload[EVENT_TYPE] = EVENT_TYPE_UPDATE
        case_study_schedule.set_jsonb_attr("payload", payload)

    case_study = session.query(CaseStudy)\
        .filter(
            CaseStudy.status == service.CASE_STUDY_STATUS_APPROVED,
            CaseStudy.id == payload["case_study_id"]
    ).first()

    case_study.set_jsonb_attr(
        "crawl_progress", CaseStudy.case_study_crawl_progress_default())

    session.commit()
    logger.info(f"""
    Case study schedule
    cs_id: {case_study.id}
    cs_crawl_progress: {case_study.crawl_progress}
    """)


@auto_session
def clear_case_study_chart_data_cache(case_study_id, session=None):
    try:
        session.query(Task).filter(Task.key.like(
            f"case_study_id={case_study_id}%")).delete(
                synchronize_session='fetch'
        )
        session.commit()
        return True
    except Exception as error:
        logger.error(
            f"Cannot delete chart cache when re-run case study ID {case_study_id}, error={error}")
        return False


def execute_onetime(payload):
    logger.info(f"Execute execute_onetime at {datetime.utcnow().isoformat()}")
    case_study_id = payload['case_study_id']
    case_study_schedule = service.get_case_study_schedule(case_study_id)
    if not case_study_schedule:
        logger.exception(
            f"Can't get case_study_schedule from DB: {case_study_id}")
    trigger_sync(payload)

    tag = f"onetime_{case_study_id}"
    logger.info(f"Clear expire tag={tag}")
    schedule.clear(tag)
    del tag_storage.Hash('___')[tag]
    logger.info(f"After clear expire tag={tag}")
    clear_case_study_chart_data_cache(case_study_id)


def execute_daily(payload):
    logger.info(f"Execute execute_daily at {datetime.utcnow().isoformat()}")
    case_study_id = payload['case_study_id']
    case_study_schedule = service.get_case_study_schedule(case_study_id)
    if not case_study_schedule:
        logger.exception(
            f"Can't get case_study_schedule from DB: {case_study_id}")
    trigger_sync(payload)
    clear_case_study_chart_data_cache(case_study_id)


def execute_weekly(payload):
    logger.info(f"Execute execute_weekly at {datetime.utcnow().isoformat()}")
    case_study_id = payload['case_study_id']
    case_study_schedule = service.get_case_study_schedule(case_study_id)
    if not case_study_schedule:
        logger.exception(
            f"Can't get case_study_schedule from DB: {case_study_id}")
    trigger_sync(payload)
    clear_case_study_chart_data_cache(case_study_id)


def execute_monthly(payload):
    start_time = parse(payload[SCHEDULE_TIME_FIELD][START_DATE_FIELD])
    dd_schedule = datetime.fromtimestamp(
        datetime.timestamp(start_time)).strftime('%d')
    dd_today = datetime.fromtimestamp(
        datetime.timestamp(datetime.utcnow())).strftime('%d')

    if dd_schedule != dd_today:
        return
    logger.info(f"Execute execute_monthly at {datetime.utcnow().isoformat()}")
    case_study_id = payload['case_study_id']
    case_study_schedule = service.get_case_study_schedule(case_study_id)
    if not case_study_schedule:
        logger.exception(
            f"Can't get case_study_schedule from DB: case_study_id={case_study_id}")
    trigger_sync(payload)
    clear_case_study_chart_data_cache(case_study_id)


def schedule_onetime(payload):
    case_study_id = payload['case_study_id']
    tag = f"onetime_{case_study_id}"

    start_time = parse(payload[SCHEDULE_TIME_FIELD][START_DATE_FIELD])
    hh_mm = datetime.fromtimestamp(
        datetime.timestamp(start_time)).strftime('%H:%M')

    logger.info(f"Schedule execute_onetime at {hh_mm}")
    schedule.every().day.at(hh_mm).do(execute_onetime, payload).tag(tag)

    tag_storage.Hash('___')[tag] = json.dumps(payload)


def schedule_daily(payload):
    case_study_id = payload['case_study_id']
    tag = f"daily_{case_study_id}"

    start_time = parse(payload[SCHEDULE_TIME_FIELD][START_DATE_FIELD])
    hh_mm = datetime.fromtimestamp(
        datetime.timestamp(start_time)).strftime('%H:%M')

    logger.info(f"Schedule execute_daily at {hh_mm}")
    schedule.every().day.at(hh_mm).do(execute_daily, payload).tag(tag)

    tag_storage.Hash('___')[tag] = json.dumps(payload)


def schedule_weekly(payload):
    case_study_id = payload['case_study_id']
    tag = f"weekly_{case_study_id}"

    start_time = parse(payload[SCHEDULE_TIME_FIELD][START_DATE_FIELD])
    hh_mm = datetime.fromtimestamp(
        datetime.timestamp(start_time)).strftime('%H:%M')

    logger.info(f"Schedule execute_weekly at {hh_mm}")

    # https://docs.python.org/3/library/datetime.html#datetime.date.weekday
    func_map = [
        schedule.every().monday,
        schedule.every().tuesday,
        schedule.every().wednesday,
        schedule.every().thursday,
        schedule.every().friday,
        schedule.every().saturday,
        schedule.every().sunday
    ]

    func = func_map[start_time.weekday()]
    func.at(hh_mm).do(execute_weekly, payload).tag(tag)

    tag_storage[tag] = json.dumps(payload)


def schedule_monthly(payload):
    case_study_id = payload['case_study_id']
    tag = f"monthly_{case_study_id}"

    start_time = parse(payload[SCHEDULE_TIME_FIELD][START_DATE_FIELD])
    hh_mm = datetime.fromtimestamp(
        datetime.timestamp(start_time)).strftime('%H:%M')

    logger.info(f"Schedule execute_monthly at {hh_mm}")
    schedule.every().day.at(hh_mm).do(execute_monthly, payload).tag(tag)

    tag_storage[tag] = json.dumps(payload)


@auto_session
def handle_task_pubsub(payload, session=None):
    payload[TYPE_TASK] = TYPE_TASK_FROM_PUBSUB
    task_queue.put(payload)


def handle_task_pubsub_thread():
    helper.subscribe(logger, config.GCP_DATA_PLATFORM_PROJECT_ID,
                     config.GCP_PUBSUB_SCHEDULE_SUBSCRIPTION, handle_task_pubsub, PUBSUB_DELAY_TIME)


def handle_task(payload):
    try:
        current_date = datetime.now()

        if EVENT_TYPE not in payload:
            logger.info(f"Ignore reasion: {EVENT_TYPE} not in payload")
            return

        if payload[EVENT_TYPE] == "dimension_change":
            logger.info("Run now...with event=dimension_change")
            helper.publish(logger, config.GCP_DATA_PLATFORM_PROJECT_ID,
                           config.GCP_PUBSUB_REQUEST_TOPIC, payload)
            return

        if CRAWL_ON_APPROVAL_FIELD not in payload:
            logger.info(
                f"Ignore reasion: {CRAWL_ON_APPROVAL_FIELD} not in payload")
            pass
        else:
            crawl_on_approval = payload[CRAWL_ON_APPROVAL_FIELD]
            if crawl_on_approval and payload[TYPE_TASK] == TYPE_TASK_FROM_PUBSUB:
                logger.info("Run now...with crawl_on_approval=True")
                trigger_sync(payload)
                # helper.publish(logger, config.GCP_DATA_PLATFORM_PROJECT_ID, config.GCP_PUBSUB_REQUEST_TOPIC, payload)

        if SCHEDULE_TIME_FIELD not in payload:
            logger.info(
                f"Ignore reasion: {SCHEDULE_TIME_FIELD} not in payload")
            return

        # schedule non-repeat
        if payload[SCHEDULE_TIME_FIELD][TYPE_FIELD] == TYPE_FIELD_VALUE_NO_REPEAT:
            logger.info('Schedule onetime')
            schedule_onetime(payload)
        else:  # schedule repeat
            schedule_end = parse(payload[SCHEDULE_TIME_FIELD][END_DATE_FIELD])
            if datetime.timestamp(current_date) > datetime.timestamp(schedule_end):
                return

            if payload[SCHEDULE_TIME_FIELD][TYPE_FIELD] == TYPE_FIELD_VALUE_DAILY:
                logger.info('Schedule daily')
                schedule_daily(payload)
                return
            if payload[SCHEDULE_TIME_FIELD][TYPE_FIELD] == TYPE_FIELD_VALUE_WEEKLY:
                logger.info('Schedule weekly')
                schedule_weekly(payload)
                return
            if payload[SCHEDULE_TIME_FIELD][TYPE_FIELD] == TYPE_FIELD_VALUE_MONTHLY:
                logger.info('Schedule monthly')
                schedule_monthly(payload)
                return
    except Exception as error:
        logger.error(f"Error: {error}")


def handle_clear_expire_tag():
    while True:
        try:
            logger.info("handle_clear_expire_tag...")
            for item in tag_storage.Hash('___').items():
                tagStr = item[0].decode("utf-8")
                payload = json.loads(item[1].decode("utf-8"))
                current_date = datetime.now()
                end_time = parse(payload[SCHEDULE_TIME_FIELD][END_DATE_FIELD])

                current_timestamp = datetime.timestamp(current_date)
                end_timestamp = datetime.timestamp(end_time)
                if int(current_timestamp) > int(end_timestamp):
                    logger.info(f"Clear expire tag={tagStr}")
                    schedule.clear(tagStr)
                    del tag_storage.Hash('___')[tagStr]
                    logger.info(f"After clear expire tag={tagStr}")
            # logger.info(f"Tags: {tag_storage.Hash('___').keys()}")
            time.sleep(5)
        except Exception as error:
            logger.error(f"Error: {error}")


def handle_task_queue():
    while True:
        try:
            payload = task_queue.get()
            if payload is None:
                continue
            logger.info(f'Received: {payload}')
            handle_task(payload)
            task_queue.task_done()
            logger.info(f'Finished: {payload}')
        except Exception as error:
            logger.error(f"Error: {error}")


@auto_session
def main(session=None):
    # LOAD FROM DB
    schedule_items = service.list_case_study_schedule(session)
    for schedule_item in schedule_items:
        # logger.info(schedule_item.to_json()['payload'])
        if not schedule_item.to_json()['payload']:
            continue
        payload = schedule_item.to_json()['payload']
        payload[TYPE_TASK] = TYPE_TASK_FROM_DB
        task_queue.put(payload)

    t1 = threading.Thread(target=handle_task_pubsub_thread)
    t1.setDaemon(True)
    t1.start()

    t2 = threading.Thread(target=handle_task_queue)
    t2.setDaemon(True)
    t2.start()

    t3 = threading.Thread(target=handle_clear_expire_tag)
    t3.setDaemon(True)
    t3.start()

    while True:
        # logger.info(f"{datetime.utcnow().isoformat()}")
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
