import config
import csv
from database import CompanyDataSource, DataSource, HealthCheckConfig, HealthCheckReport, NLPType, Session
import logger
import os
from sparkpost import SparkPost
from sparkpost.exceptions import SparkPostAPIException
from datetime import datetime

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)
SPARKPOST_API_KEY = os.environ.get("SPARKPOST_API_KEY")
EMAIL_DOMAIN = os.environ.get("EMAIL_DOMAIN", "mail-leo.tpptechnology.com")
logger.info(f"Health Check email key: {SPARKPOST_API_KEY}")
sp = SparkPost(SPARKPOST_API_KEY)
CS_URL = os.environ.get(
    "CS_URL", None)


def send_email(email_list, check_date, data, health_check_report_id):
    session = Session()
    logger.info(f"Health Check email list: {email_list}")
    file = "health_check_report.csv"
    try:
        # get last report csv data
        health_check_last_report = None
        i = 1
        while health_check_report_id != i:
            health_check_last_report = session.query(HealthCheckReport).filter(
                HealthCheckReport.id == (health_check_report_id - i)).first()
            if health_check_last_report:
                break
            i += 1

        last_report_csv_data = health_check_last_report.csv_data
        last_report_date = health_check_last_report.update_time.strftime(
            '%d/%m/%Y')

        csv_data = []
        today = datetime.now().strftime('%d/%m/%Y')

        with open(file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["", "", "", last_report_date,
                             "", "", today, "", "", "", ""])
            writer.writerow(["Data Type", "Type", "Source", "Total crawled reviews/jobs",
                             "Total records from web", "Successful Rate", "Total crawled reviews/jobs",
                             "Total records from web", "Successful Rate", "Successful Rate Diff", "Success", "Url"])
            logger.info(
                f"Health Check, company data source data : {data}")

            for item in data:
                logger.info(
                    f"Health Check, company data source item : {item}, type: {type(item)}")
                if isinstance(item, dict):
                    company_data_source_id = item["company_datasource_id"]
                    _company_data_source = session.query(CompanyDataSource).filter(
                        CompanyDataSource.id == company_data_source_id).first()
                    urls = _company_data_source.url
                    logger.info(
                        f"Health Check, company data source : {company_data_source_id}, urls: {urls}")
                    url = urls[0]
                    data_type = None

                    # find correct url for VOE data source
                    if len(urls) > 1:
                        data_type = CompanyDataSource.URL_TYPE_MAP_2[item["data_type"]]
                        for _url in urls:
                            if _url["url_type"] == data_type:
                                url = _url
                                break

                    _data_source = session.query(DataSource).filter(
                        DataSource.id == _company_data_source.data_source_id).first()
                    _nlp_type = session.query(NLPType).filter(
                        NLPType.id == _data_source.nlp_type_id).first()

                    # current report data
                    source_name = _data_source.name
                    nlp_type = _nlp_type.name
                    url = url["url"]
                    status = item["status"]
                    crawled_records = item["process_review"] if item["process_review"] else 0
                    total_records = item["total_review"] if item["total_review"] else 0
                    successful_rate = item["successful_rate"] if item["successful_rate"] else 0
                    _data_type = item["data_type"]

                    # last report data
                    _matching_data_object = list(
                        filter(lambda x: x["Url"] == url, last_report_csv_data))
                    if _matching_data_object:
                        _object = _matching_data_object[0]
                        last_crawled_records = _object["Crawled Records"] if _object["Crawled Records"] else 0
                        last_total_records = _object["Total Records"] if _object["Total Records"] else 0
                        last_successful_rate = _object["Successful Rate"] if _object["Successful Rate"] else 0
                        successful_rate_diff = (successful_rate - last_successful_rate) if "N/A" not in [
                            successful_rate, last_successful_rate] else "N/A"
                    else:
                        last_crawled_records = "N/A"
                        last_total_records = "N/A"
                        last_successful_rate = "N/A"
                        successful_rate_diff = successful_rate

                    if successful_rate_diff == "N/A":
                        success = "Fail"
                    else:
                        success = "True" if successful_rate_diff >= 0 else "Fail"

                    writer.writerow([_data_type, nlp_type, source_name, last_crawled_records, last_total_records, last_successful_rate,
                                     crawled_records, total_records, successful_rate, successful_rate_diff, success, url])

                    csv_data.append(
                        {
                            "Source": source_name,
                            "Type": nlp_type,
                            "Data Type": _data_type,
                            "Url": url,
                            "Crawled Records": crawled_records,
                            "Total Records": total_records,
                            "Successful Rate": successful_rate,
                            "Status": status,
                        }
                    )

        logger.info(
            f"Health Check, csv data : {csv_data}")
        health_check_report = session.query(HealthCheckReport).filter(
            HealthCheckReport.id == health_check_report_id).first()
        health_check_report.set_jsonb_attr("csv_data", csv_data)
        session.commit()

        sp.transmissions.send(
            recipients=email_list,
            html="""
            <p>
            Please review the attached report.<br></br>
            Best regards,<br></br>
            <br></br>
            <br></br>
            <br></br>
            Vitruvian Support Team
            </p>
            """,
            from_email=f'health-check@{EMAIL_DOMAIN}',
            subject=f'Crawl Health Check Report {check_date}',
            attachments=[
                {
                    "name": file,
                    "type": "text/csv",
                    "filename": "./health_check_report.csv"
                }
            ]
        )
        logger.info(f"Health Check, send email success")
    except SparkPostAPIException as err:
        error = "<br>".join(err.errors)
        logger.error(f"Health Check Email Error, {error}")
    except Exception as error:
        logger.error(f"Health Check Email Error, {error}")
    finally:
        health_check_config = session.query(HealthCheckConfig)\
            .filter(
            HealthCheckConfig.is_active
        ).first()

        health_check_config.set_jsonb_attr(
            "term_data", {})
        health_check_config.is_running = False
        health_check_config.is_run_now = False
        session.commit()
        session.close()
        if (os.path.exists(file) and os.path.isfile(file)):
            os.remove(file)
            logger.info(f"Health Check, delete csv file after sent")

        session.close()


def send_announcement_email(list_finished_case_study_objects_info):
    email_list = [
        "cuong.nguyen@tpptechnology.com",
        "anh.trantuan@tpptechnology.com",
    ]

    prod_email_list = [
        "arcadi.gonzalez-graells@vitruvianpartners.com",
        "ian.lee@vitruvianpartners.com",
        "raymond.lal@vitruvianpartners.com",
    ]

    if CS_URL == "https://leonardo.vitruvianpartners.com/analysis":
        email_list += prod_email_list

    cs_info_format = """
    <li>Case study name: {}</li>
    <li>Case study ID: {}</li>
    <br>
    """
    cs_link_format = """<li><a href="{}/{}">{}/{}</a></li>"""
    cs_info = ""
    cs_link = ""

    for list_case_study_object_info in list_finished_case_study_objects_info:
        cs_id = list_case_study_object_info["id"]

        cs_info += cs_info_format.format(
            list_case_study_object_info["name"],
            cs_id
        )

        cs_link += cs_link_format.format(CS_URL, cs_id, CS_URL, cs_id)

    sp.transmissions.send(
        recipients=email_list,
        html=f"""
            <p>
            Dear, <br>
            We would like to inform you that the following case study has been finished. Please review the information of these case studies:
            <ul>
            {cs_info}
            </ul>
            <br>
            Please view these case studies in detail via:
            <ul>
            {cs_link}
            </ul>
            <br>
            <br>
            Best regards,<br>
            <br>
            Vitruvian Support Team
            </p>
            """,
        from_email=f'leonardo-case-study-notification@{EMAIL_DOMAIN}',
        subject=f' [LEO system] Notification for your case studies',
        attachments=[]
    )
    logger.info(f"CS Tracking, send announcement email success")


def send_report_email(list_report_case_study_objects_info):
    email_list = [
        "cuong.nguyen@tpptechnology.com",
        "anh.trantuan@tpptechnology.com"
    ]

    cs_info_format = """
    <li>Case study name: {}</li>
    <li>Case study ID: {}</li>
    <br>
    """
    cs_link_format = """<li><a href="{}/{}">{}/{}</a></li>"""
    cs_info = ""
    cs_link = ""

    for list_case_study_object_info in list_report_case_study_objects_info:
        cs_id = list_case_study_object_info["id"]

        cs_info += cs_info_format.format(
            list_case_study_object_info["name"],
            cs_id
        )

        cs_link += cs_link_format.format(CS_URL, cs_id, CS_URL, cs_id)

    sp.transmissions.send(
        recipients=email_list,
        html=f"""
            <p>
            Dear, <br>
            We would like to inform you that the following case study has been running for too long. Please check these case studies:
            <ul>
            {cs_info}
            </ul>
            <br>
            Please view these case studies in detail via:
            <ul>
            {cs_link}
            </ul>
            <br>
            <br>
            Best regards,<br>
            <br>
            Vitruvian Support Team
            </p>
            """,
        from_email=f'leonardo-case-study-report@{EMAIL_DOMAIN}',
        subject=f' [LEO system] Report case studies running too long',
        attachments=[]
    )
    logger.info(f"CS Tracking, send report email success")
