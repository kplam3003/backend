import urllib
import urllib.parse

import requests

import config
import logger
from database import CaseStudyCompany, Session

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

class PreapreVoCChartData:
    def prepare_chart_VOC_6_7_data(self, chart_code, start_date, end_date, *args, **kwargs):
        code_name = "VOC_6_7"
        query_dict = {
            "case_study_id": self.case_study.id,
            "code_name": code_name,
            "start_date": start_date,
            "end_date": end_date,
        }
        session = Session()
        case_study_company = session.query(CaseStudyCompany).filter(
            CaseStudyCompany.case_study_id == self.case_study.id
        ).all()
        company_names = [item.company_name for item in case_study_company]

        if not company_names:
            logger.error(f"Error: Case study ID {self.case_study.id} - Chart VOC_6_7 missing company")
        for company_name in company_names:
            query_dict["company_names"] = company_name
            query_dict["page"] = 1
            query_dict["page_size"] = config.INIT_VOC_6_7_PAGESIZE
            query_string = urllib.parse.urlencode(query_dict)
            request_url = f"{config.CHART_REQUEST_URI_ENDPOINT}?{query_string}"
            logger.info(request_url)
            try:
                requests.get(url=request_url)
                logger.error(f"Prepare chart data - Case study ID {self.case_study.id} - Chart {code_name} - Success")
                continue
            except Exception as error:
                logger.error(f"Prepare chart data - Case study ID {self.case_study.id} - Chart {code_name} - error {error}")
                continue


