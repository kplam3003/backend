import json
import time
import logger
import config
import requests
import os
import numbers

from database import CaseStudyCompanyDataSource, Company, DataSource, HealthCheckConfig, HealthCheckReport, Session, CompanyDataSource
from datetime import datetime

from src.decorator.tracking_trigger_sync import tracking_data_trigger_sync
from src.services.email import send_email
from src.services.company_data_source import update_company_data_sources_health_check_status

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
SCHEDULE_TIME_FIELD = "schedule_time"
START_DATE_FIELD = "start_date"
END_DATE_FIELD = "end_date"
NUMBER_OF_WEEKS_TO_RUN = "number_of_weeks_to_run"
DATA = 'data'
MAX_TIME_WAITED = 86400
last_run_time = None


def trigger_sync(payload):
    company_id = payload["company_id"]
    username = os.environ.get('ADMIN_USERNAME')
    password = os.environ.get('ADMIN_PASSWORD')
    base_url = os.environ.get('URL')

    health_check_company_datasources = []
    # clear old company data source
    session = Session()
    old_company_datasources = session.query(CompanyDataSource).filter(
        CompanyDataSource.company_id == company_id
    )
    old_company_datasource_ids = []
    for old_company_datasource in old_company_datasources:
        old_company_datasource_ids.append(old_company_datasource.id)
        health_check_company_datasources.append(
            {
                "data_source_id": old_company_datasource.data_source_id,
                "urls": list(map(lambda x: {
                    "url": x.get("url", ""),
                    "type": CompanyDataSource.URL_TYPE_MAP_3[x.get("url_type", "VOC_LEONARDO")]
                }, old_company_datasource.url))
            }
        )

    # need to delete all case study company data source ids if have before delete company data source ids
    session.query(CaseStudyCompanyDataSource).filter(
        CaseStudyCompanyDataSource.data_source_id in old_company_datasource_ids
    ).delete()

    logger.info(
        f"Health Check health check company datasources: {health_check_company_datasources}")

    # delete all old company data source ids
    session.query(CompanyDataSource).filter(
        CompanyDataSource.company_id == company_id
    ).delete()
    session.commit()
    session.close()

    logger.info(
        f"Health Check start .... company: {company_id} with payload: {payload}")

    response = requests.post(base_url + "admin/login", data={
        "username": username,
        "password": password
    })
    if response.status_code != 200:
        logger.exception(
            "login to client rest fail, please check again")

    data = response.json()
    token = data.get('token')
    if not token:
        logger.exception(
            "wrong username or password, please config variable USERNAME and PASSWORD again")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0',  # noqa: E501
        "Authorization": f"Token {token}",
        "Content-Type": "application/json"
    }

    is_created_sucess = True
    # create new company data source for health check
    for health_check_company_datasource in health_check_company_datasources:
        logger.info(
            f"Health Check create new company data source: {health_check_company_datasource}")
        body = json.dumps(health_check_company_datasource)
        response = requests.put(
            base_url + f"company/{company_id}/company-data-source", data=body, headers=headers,  verify=False)
        if response.status_code != 200:
            is_created_sucess = False
            logger.info(
                f"Health Check Error: {response.text}")
            break
        time.sleep(3)

    if not is_created_sucess:
        logger.info(
            f"Health Check Error: fail to create company data source")
        return

    response = requests.get(
        base_url + f"company/{company_id}/trigger", headers=headers, verify=False)

    if response.status_code != 200:
        logger.exception(
            "error when call trigger all in client rest api, please check again")

    result = response.json()

    company_data_source_ids_trigger_passed = list(filter(
        lambda x: x["data_type"] in ["job", "reivew"], result["data_sources_triggered"]))
    company_data_source_ids_trigger_failed = list(filter(lambda x: x["data_type"] in [
                                                  "job", "reivew"], result["invalid_company_data_sources"]))

    company_data_source_ids_trigger_passed = list(set(map(
        lambda x: x["id"], result["data_sources_triggered"])))
    company_data_source_ids_trigger_failed = list(set(map(
        lambda x: x["id"], result["invalid_company_data_sources"])))

    start_time = datetime.now().strftime("%d/%m/%Y")
    email_list = payload["email_list"]

    session = Session()

    health_check_config = session.query(HealthCheckConfig)\
        .filter(
        HealthCheckConfig.is_active
    ).first()

    health_check_config.set_jsonb_attr(
        "term_data",
        {
            'company_id': company_id,
            'start_time': start_time,
            'email_list': email_list,
            'company_data_source_ids_trigger_passed': company_data_source_ids_trigger_passed,
            'company_data_source_ids_trigger_failed': company_data_source_ids_trigger_failed
        }
    )
    health_check_config.is_running = True

    session.commit()
    session.close()

    follow_health_check_running_state(
        company_id=company_id,
        start_time=start_time,
        email_list=email_list,
        company_data_source_ids_trigger_passed=company_data_source_ids_trigger_passed,
        company_data_source_ids_trigger_failed=company_data_source_ids_trigger_failed
    )


@tracking_data_trigger_sync
def follow_health_check_running_state(
    company_id,
    start_time,
    email_list,
    company_data_source_ids_trigger_passed,
    company_data_source_ids_trigger_failed
):
    total_company_data_source_ids = company_data_source_ids_trigger_passed + \
        company_data_source_ids_trigger_failed

    total_company_data_source_ids = list(set(total_company_data_source_ids))

    logger.info(
        f"Health Check Logger, total company data source ids: {total_company_data_source_ids}")

    company_data_source_ids_complete_crawl = []
    company_data_source_ids_in_progress = company_data_source_ids_trigger_passed

    # check until all company data source crawled 24h after trigger
    while True:
        current_time = datetime.now()
        waited_time = current_time - datetime.strptime(start_time, "%d/%m/%Y")
        session = Session()

        company_data_sources_trigger_in_progress = session.query(CompanyDataSource)\
            .filter(CompanyDataSource.id.in_(company_data_source_ids_in_progress), CompanyDataSource.is_active).all()

        logger.info(
            f"Health Check Logger, company data source ids in progress: {company_data_source_ids_in_progress}")
        for company_data_source_trigger_in_progress in company_data_sources_trigger_in_progress:
            logger.info(
                f"Health Check Logger, company data source id checking: {company_data_source_trigger_in_progress.id}")
            crawl_progress = company_data_source_trigger_in_progress.crawl_progress
            logger.info(
                f"Health Check Logger, company data source crawl progress checking: {crawl_progress}")
            logger.info(
                f"Health Check Logger, company data source status checking: {crawl_progress['job']['status']}, {crawl_progress['review']['status']}")
            if crawl_progress["job"]["status"] != CompanyDataSource.STATUS_IN_PROGRESS\
                    and crawl_progress["review"]["status"] != CompanyDataSource.STATUS_IN_PROGRESS:
                _id = company_data_source_trigger_in_progress.id
                logger.info(
                    f"Health Check Logger, company data source id pass: {_id}")

                urls = company_data_source_trigger_in_progress.url
                logger.info(
                    f"Heal Check Logger, company data source: {_id}, urls: {urls}")
                for _url in urls:
                    if _url["url"] != "":
                        company_data_source_ids_complete_crawl.append(_id)

                company_data_source_ids_in_progress.remove(_id)

        if len(company_data_source_ids_in_progress) == 0:
            break

        if waited_time.total_seconds() >= MAX_TIME_WAITED:
            break

        session.close()
        time.sleep(300)

    logger.info(f"Health Check Logger: done")

    health_check_report = update_company_data_sources_health_check_status(
        company_id)

    logger.info(f"Health Check Logger, report: {health_check_report}")

    company_data_source_errors = list(
        filter(lambda x:
               (x["successful_rate"] if isinstance(
                   x["successful_rate"], numbers.Number) else 0) < 60,
               health_check_report["details"]))

    company_data_source_ids_error = list(set(map(
        lambda x: x["company_datasource_id"], company_data_source_errors)))

    company_data_source_ids_no_error = list(filter(
        lambda x: x not in company_data_source_ids_error, company_data_source_ids_complete_crawl))

    session = Session()
    company = session.query(Company).filter(Company.id == company_id).first()

    logger.info(
        f"Health Check Logger, trigger failed company data source ids: {company_data_source_ids_trigger_failed}")

    report_failed_trigger = []
    report_failed_trigger_ids = []
    for _id in company_data_source_ids_trigger_failed:
        _company_data_source = session.query(CompanyDataSource).filter(
            CompanyDataSource.id == _id).first()
        _data_source = session.query(DataSource).filter(
            DataSource.id == _company_data_source.data_source_id).first()
        urls = _company_data_source.url

        for url in urls:
            data_type = CompanyDataSource.URL_TYPE_MAP[url["url_type"]]

            # skip overview data type
            if data_type == "overview":
                continue

            # double check if company data source already passed
            _is_company_data_source_already_passed = list(filter(
                lambda x: x["company_datasource_id"] == _id and x["data_type"] == data_type, health_check_report["details"]))
            if _is_company_data_source_already_passed:
                continue

            if url["url"] != "":
                report_failed_trigger_ids.append(_id)
                report_failed_trigger.append(
                    {
                        "company_datasource_id": _id,
                        "company_id": company_id,
                        "company_name": company.name,
                        "source_name": _data_source.name,
                        "data_type": data_type,
                        "status": "trigger failed",
                        "process_review": "N/A",
                        "total_review": "N/A",
                        "successful_rate": "N/A",
                    }
                )

    logger.info(
        f"Health Check Logger, timeout failed company data source ids: {company_data_source_ids_in_progress}")

    report_failed_by_timeout = []
    report_failed_by_timeout_ids = []

    for _id in company_data_source_ids_in_progress:
        _company_data_source = session.query(CompanyDataSource).filter(
            CompanyDataSource.id == _id).first()
        _data_source = session.query(DataSource).filter(
            DataSource.id == _company_data_source.data_source_id).first()
        urls = _company_data_source.url

        for url in urls:
            if url["url"] != "":
                data_type = CompanyDataSource.URL_TYPE_MAP[url["url_type"]]

                # skip overview data type
                if data_type == "overview":
                    continue

                # double check if company data source already passed
                _is_company_data_source_already_passed = list(filter(
                    lambda x: x["company_datasource_id"] == _id and x["data_type"] == data_type, health_check_report["details"]))
                if _is_company_data_source_already_passed:
                    continue

                report_failed_by_timeout_ids.append(_id)
                report_failed_by_timeout.append(
                    {
                        "company_datasource_id": _id,
                        "company_id": company_id,
                        "company_name": company.name,
                        "source_name": _data_source.name,
                        "data_type": data_type,
                        "status": "time out",
                        "process_review": "N/A",
                        "total_review": "N/A",
                        "successful_rate": "N/A",
                    }
                )

    # for some company data source with auto fail in a very first step
    report_company_data_source_fail_completely = []

    total_failed_company_data_source_ids = company_data_source_ids_error + \
        report_failed_trigger_ids + \
        report_failed_by_timeout_ids

    total_failed_company_data_source_ids = list(
        set(total_failed_company_data_source_ids))

    company_data_source_ids_no_error = list(
        set(company_data_source_ids_no_error))

    company_data_source_ids_no_error = list(filter(
        lambda x: x not in total_failed_company_data_source_ids, company_data_source_ids_no_error))

    logger.info(
        f"""Health Check, fail: {total_failed_company_data_source_ids}""")
    logger.info(
        f"""Health Check, no error: {company_data_source_ids_no_error}""")
    logger.info(
        f"""Health Check, fail by time out: {report_failed_by_timeout}""")
    logger.info(
        f"""Health Check, fail by trigger: {report_failed_trigger}""")
    # logger.info(f"""Health Check, pass: {report_company_data_source_passed}""")

    result = {
        "check_date": datetime.now().strftime("%d/%m/%Y"),
        "total_company_data_sources": len(total_company_data_source_ids),
        "total_failed_company_data_source": len(total_company_data_source_ids) - len(company_data_source_ids_no_error),
        "total_passed_company_data_source": len(company_data_source_ids_no_error),
        "company_id": company_id,
        "data": health_check_report["details"] + report_failed_by_timeout + report_failed_trigger + report_company_data_source_fail_completely
    }

    logger.info(
        f"Health Check Logger, final result:{result}")

    _health_check_report = HealthCheckReport(
        company_id=result["company_id"],
        number_of_company_data_source_total=result["total_company_data_sources"],
        number_of_company_data_source_passed=result["total_passed_company_data_source"],
        number_of_company_data_source_failed=result["total_failed_company_data_source"],
        data=result["data"],
        csv_data=[]
    )

    session.add(_health_check_report)
    session.commit()
    logger.info(
        f"Health Check Logger, health check report:{_health_check_report.id}")
    session.close()

    send_email(email_list=email_list, check_date=result["check_date"],
               data=result["data"], health_check_report_id=_health_check_report.id)


def execute_weekly(payload):
    logger.info(f"Execute health check at {datetime.utcnow().isoformat()}")
    trigger_sync(payload)


def trunc_datetime(_date):
    return _date.replace(hour=0, minute=0, second=0, microsecond=0)


def schedule_weekly(payload):
    global last_run_time
    number_of_weeks = payload['number_of_weeks_to_run']
    date_of_week = payload['date_of_week']
    create_time = datetime.strptime(payload['create_time'], "%d/%m/%Y")

    today = datetime.now()
    if last_run_time:
        if trunc_datetime(today) == trunc_datetime(last_run_time):
            return

    today_day = today.day

    logger.info(
        f"Health Check, check schedule: today-{today.weekday()}, runday-{date_of_week}, weekperrun-{number_of_weeks}")

    if number_of_weeks != 1:
        if (abs(today_day - create_time.day)//7) % number_of_weeks != 0:
            return

    if today.weekday() != date_of_week:
        return

    last_run_time = today
    execute_weekly(payload)


def main():

    while True:
        session = Session()
        try:
            logger.info(
                f"Health Check checked db -----------------------------")

            # LOAD FROM DB
            health_check_config = session.query(HealthCheckConfig)\
                .filter(
                    HealthCheckConfig.is_active
            ).first()
            logger.info(
                f"Health Check worker is_running: {health_check_config.is_running} is_run_now: {health_check_config.is_run_now} term_data: {health_check_config.term_data}")
            payload = {}
            if health_check_config:
                payload = health_check_config.to_json()
                if health_check_config.is_running:
                    if health_check_config.term_data:
                        company_id = health_check_config.term_data.get(
                            'company_id')
                        start_time = health_check_config.term_data.get(
                            'start_time')
                        email_list = health_check_config.term_data.get(
                            'email_list')
                        company_data_source_ids_trigger_passed = health_check_config.term_data.get(
                            'company_data_source_ids_trigger_passed')
                        company_data_source_ids_trigger_failed = health_check_config.term_data.get(
                            'company_data_source_ids_trigger_failed')
                        health_check_config.is_run_now = False
                        session.commit()
                        session.close()
                        follow_health_check_running_state(
                            company_id=company_id,
                            start_time=start_time,
                            email_list=email_list,
                            company_data_source_ids_trigger_passed=company_data_source_ids_trigger_passed,
                            company_data_source_ids_trigger_failed=company_data_source_ids_trigger_failed
                        )
                    else:
                        health_check_config.is_running = False
                        session.commit()
                        session.close()
                elif health_check_config.is_run_now:
                    health_check_config.is_run_now = False
                    session.commit()
                    session.close()
                    trigger_sync(payload=payload)
                else:
                    session.commit()
                    session.close()
                    schedule_weekly(payload)
            else:
                session.close()
        except Exception as e:
            logger.error(f"Health Check ERROR: {e}")
        finally:
            time.sleep(300)


if __name__ == "__main__":
    main()
