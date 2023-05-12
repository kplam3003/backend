import urllib
import urllib.parse

import gevent
import requests
from dateutil.relativedelta import relativedelta

import config
import logger
from database import Session, NLPPack, Chart
from src.chart.get_voc_chart import PreapreVoCChartData

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

ma_chart_codes = [
    "VOC_1_1", "VOC_1_4", "VOC_1_6", "VOC_1_8", "VOC_1_9",
    "VOC_3",
    "VOC_4_1", "VOC_4_2", "VOC_4_3", "VOC_4_4",
    "VOC_6_1", "VOC_6_6",
    "VOC_6_8", "VOC_6_9",
    "VOC_12", "VOC_13", "VOC_13_1", "VOC_13_2",
    "VOC_14", "VOC_16",
    "VOE_1_4", "VOE_1_6", "VOE_3", "VOE_4_2",
    "VOE_6_1", "VOE_6_6", "VOE_6_8", "VOE_6_9", "VOE_12",
    "VOE_9_5", "VOE_9_5_1", "VOE_9_5_2",
    "VOE_9_4", "VOE_9_4_1", "VOE_9_4_2",
    "VOE_14", "VOE_16",
]
MA_DAILY = "Daily"
MA_7 = "MA 7"
MA_14 = "MA 14"
MA_30 = "MA 30"
MA_60 = "MA 60"
MA_90 = "MA 90"
MA_120 = "MA 120"
MA_150 = "MA 150"
MA_180 = "MA 180"


class PrepareChartDataRunner(PreapreVoCChartData):
    chart_codes = []
    case_study = None

    def _get_case_study_chart_codes(self):
        if not self.case_study:
            return []

        session = Session()
        nlp_pack_id = self.case_study.nlp_pack_id
        nlp_pack = session.query(NLPPack).filter(NLPPack.id == nlp_pack_id).first()

        charts = session.query(Chart).filter(Chart.nlp_type_id == nlp_pack.nlp_type_id, Chart.is_active).all()
        chart_codes = [chart.code_name for chart in charts]
        session.close()

        return chart_codes

    def __init__(self, case_study=None, chart_codes=None):
        session = Session()
        self.case_study = case_study

        self.chart_codes = self._get_case_study_chart_codes()
        if chart_codes and isinstance(chart_codes, list):
            case_study_chart_code = self._get_case_study_chart_codes()
            self.chart_codes = [item for item in chart_codes if item in case_study_chart_code]

        self.end_date = case_study.nlp_type_latest_review_date
        if not case_study.nlp_type_latest_review_date:
            logger.error(
                f"In progress - Prepare chart data - Case study ID {self.case_study.id} - NLP type latest review date does not exists")
            return

        self.end_date = case_study.nlp_type_latest_review_date
        self.start_date = self.end_date - relativedelta(years=3)

        self.formatted_end_date = self.end_date.strftime("%Y-%m-%d")
        self.formatted_start_date = self.start_date.strftime("%Y-%m-%d")
        session.close()

    def prepare_common_chart(self, chart_code, start_date, end_date,):
        code_name = chart_code
        start_date = start_date
        end_date = end_date
        query_dict = {
            "case_study_id": self.case_study.id,
            "code_name": code_name,
            "start_date": start_date,
            "end_date": end_date,
        }
        if code_name in ma_chart_codes:
            query_dict.update({
                "ma_type": MA_90
            })

        query_string = urllib.parse.urlencode(query_dict)
        request_url = f"{config.CHART_REQUEST_URI_ENDPOINT}?{query_string}"
        logger.info(request_url)
        try:
            requests.get(url=request_url)
            logger.error(f"Prepare chart data - Case study ID {self.case_study.id} - Chart {code_name} - Success")
            return True
        except Exception as error:
            logger.error(f"Prepare chart data - Case study ID {self.case_study.id} - Chart {code_name} - error {error}")
            return False

    def get_chart_function(self, chart_code):
        try:
            chart_function_name = f"prepare_chart_{chart_code.upper()}_data"
            chart_function = getattr(self, chart_function_name)
        except Exception as error:
            logger.error(f"Error={error}")
            chart_function = self.prepare_common_chart

        return chart_function

    def run(self, *args, **kwargs):
        threads = []
        for chart_code in self.chart_codes:
            chart_function = self.get_chart_function(chart_code)
            if not chart_function:
                logger.error(f"Error while prepare chart data: case study ID {self.case_study.id} chart: {chart_code}")
                continue
            threads.append(
                gevent.spawn(
                    chart_function,
                    chart_code=chart_code,
                    start_date=self.formatted_start_date,
                    end_date=self.formatted_end_date,
                    *args, **kwargs
                )
            )

            gevent.joinall(threads)
