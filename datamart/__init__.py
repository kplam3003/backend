import os
from google.cloud import bigquery

DEFAULT_QUERY_TIMEOUT = int(os.getenv("DEFAULT_QUERY_TIMEOUT", 60*3))  # seconds

MA_PREFIX = "MA "
MA_DAILY = "Daily"
MA_7 = "MA 7"
MA_14 = "MA 14"
MA_30 = "MA 30"
MA_60 = "MA 60"
MA_90 = "MA 90"
MA_120 = "MA 120"
MA_150 = "MA 150"
MA_180 = "MA 180"

MA_TYPE_LIST = [
    MA_7,
    MA_14,
    MA_30,
    MA_60,
    MA_90,
    MA_120,
    MA_150,
    MA_180
]

MA_TYPES = {
    v: (
        int(v.split(" ")[1]),
        int(v.split(" ")[1])-1
    )
    for v in MA_TYPE_LIST
}

SS_DAILY = "ss_daily"
SS_MA = "SS_MA"

CR_DAILY = "records"
CR_MA = "CR_MA"

RECORDS_DAILY = "records_daily"
RECORDS_MA = "RECORDS_MA"

RATING_DAILY = "rating_daily"
RATING_MA = "RATING_MA"

RAW_AVERAGE_FILTER = "raw average"
ER_DAILY = "records"
ER_MA = "ER_MA"

CASE_STUDY_ID_FILTER_KEY = "case_study_id"
START_DATE_FILTER_KEY = "start_date"
END_DATE_FILTER_KEY = "end_date"
CURRENT_START_DATE_FILTER_KEY = "current_start_date"
CURRENT_END_DATE_FILTER_KEY = "current_end_date"
BEFORE_START_DATE_FILTER_KEY = "before_start_date"
BEFORE_END_DATE_FILTER_KEY = "before_end_date"
AFTER_START_DATE_FILTER_KEY = "after_start_date"
AFTER_END_DATE_FILTER_KEY = "after_end_date"
COMPANY_NAME_FILTER_KEY = "company_names"
SOURCE_NAME_FILTER_KEY = "source_names"
DIMENSION_TYPE_FILTER_KEY = "dimension_type"
DIMENSION_FILTER_KEY = "dimension"
IS_DEFAULT_KEY = "is_default"

client = bigquery.Client()
