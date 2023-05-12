import os

PORT = os.environ.get('PORT', 5000)

DATABASE_URI = os.environ.get('DATABASE_URI')

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
LUMINATI_HTTP_PROXY = os.environ.get(
    'LUMINATI_HTTP_PROXY', 'http://lum-auth-token:Y3gPGvaXkzYjh9z5DYbRXqKPTEUKpPnF@35.222.253.25:24000')
LUMINATI_WEBUNBLOCKER_HTTP_PROXY = os.environ.get(
    'LUMINATI_WEBUNBLOCKER_HTTP_PROXY', 'http://lum-auth-token:Y3gPGvaXkzYjh9z5DYbRXqKPTEUKpPnF@35.222.253.25:24001')
GCP_DATA_PLATFORM_PROJECT_ID = os.environ.get(
    'GCP_DATA_PLATFORM_PROJECT_ID', 'leo-etlplatform')

# Backend crawl Pitchbook
PITCHBOOK_SOURCE_NAME = "pitchbook"
PITCHBOOK_CRAWL_COLLECTION = "pitchbook_crawl_data"
PITCHBOOK_CRAWL_SCHEMA = {
    "company_name": {"type": "string", "required": True, "nullable": True},
    "pitchbook_id": {"type": "string", "required": True, "nullable": True},
    "country": {"type": "string", "required": True, "nullable": True},
    "primary_industry": {"type": "string", "required": True, "nullable": True},
    "employees": {"type": "integer", "required": True, "nullable": True},
    "status": {"type": "string", "required": True, "nullable": True},
    "last_updated_date": {"type": "string", "required": True, "nullable": True},
    "website": {"type": "string", "required": True, "nullable": True},
    "description": {"type": "string", "required": True, "nullable": True},
    "latest_deal_type": {"type": "string", "required": True, "nullable": True},
    "financial_rounds": {"type": "integer", "required": True, "nullable": True},
    "financing_status": {"type": "string", "required": True, "nullable": True},
    "other_industries": {"type": "string", "required": True, "nullable": True},
    "ownership_status": {"type": "string", "required": True, "nullable": True}
}

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_STORAGE_BUCKET = os.environ.get("GCP_STORAGE_BUCKET")
# Subscription
GCP_PUBSUB_SUBSCRIPTION = os.environ.get('GCP_PUBSUB_SUBSCRIPTION')
GCP_PUBSUB_CS_PROGRESS_SUBSCRIPTION = os.environ.get(
    'GCP_PUBSUB_CS_PROGRESS_SUBSCRIPTION')
GCP_PUBSUB_CS_ERROR_SUBSCRIPTION = os.environ.get(
    'GCP_PUBSUB_CS_ERROR_SUBSCRIPTION')
GCP_PUBSUB_SCHEDULE_SUBSCRIPTION = os.environ.get(
    'GCP_PUBSUB_SCHEDULE_SUBSCRIPTION')
GCP_WEBAPP_PUBSUB_VALIDATE_COMPANY_DS_SUBSCRIPTION = os.environ.get(
    'GCP_WEBAPP_PUBSUB_VALIDATE_COMPANY_DS_SUBSCRIPTION')

# Topic
GCP_PUBSUB_CS_CHART_TOPIC = os.environ.get('GCP_PUBSUB_CS_CHART_TOPIC')
GCP_PUBSUB_CS_PROGRESS_TOPIC = os.environ.get('GCP_PUBSUB_CS_PROGRESS_TOPIC')
GCP_PUBSUB_CS_ERROR_TOPIC = os.environ.get('GCP_PUBSUB_CS_ERROR_TOPIC')
GCP_PUBSUB_REQUEST_TOPIC = os.environ.get('GCP_PUBSUB_REQUEST_TOPIC')
GCP_PUBSUB_SCHEDULE_TOPIC = os.environ.get('GCP_PUBSUB_SCHEDULE_TOPIC')
GCP_WEBAPP_PUBSUB_VALIDATE_COMPANY_DS_TOPIC = os.environ.get(
    'GCP_WEBAPP_PUBSUB_VALIDATE_COMPANY_DS_TOPIC')
GCP_BQ_TIMEOUT = int(os.environ.get('GCP_BQ_TIMEOUT', 120))

GCP_DATA_PLATFORM_DATA_WAREHOUSE = os.environ.get(
    'GCP_DATA_PLATFORM_DATA_WAREHOUSE')
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get(
    'GOOGLE_APPLICATION_CREDENTIALS')
GCP_WEBAPP_SERVICE_ACCOUNT_KEY_PATH = os.environ.get(
    'GCP_WEBAPP_SERVICE_ACCOUNT_KEY_PATH')
GCP_WEBAPP_PROJECT_ID = os.environ.get('GCP_WEBAPP_PROJECT_ID')

# data platform configs
BQ_DATASET = os.environ.get('BQ_DATASET')
BQ_DATASET_DATAMART_CS = os.environ.get('BQ_DATASET_DATAMART_CS')
BQ_DATASET_STAGING = os.environ.get('BQ_DATASET_STAGING')
MONGODB_URI = os.environ.get('MONGODB_URI')
MONGODB_DB_NAME = os.environ.get('MONGODB_DB_NAME')
MONGODB_ROOT_CA = os.environ.get('MONGODB_ROOT_CA')
MONGODB_CLIENT_KEY = os.environ.get('MONGODB_CLIENT_KEY')

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME')
LOGGER = "application"
ENABLE_CLOUD_LOGGING = int(os.environ.get('ENABLE_CLOUD_LOGGING', '0')) != 0
BACKEND_SERVICE_URI = f"http://{os.environ.get('BACKEND_SERVICE_URI')}"
CHART_PREFIX_ENDPOINT = "chart"
CHART_REQUEST_URI_ENDPOINT = f"{BACKEND_SERVICE_URI}/{CHART_PREFIX_ENDPOINT}"

INIT_VOC_6_7_PAGESIZE = 25

CHART_DATA_MART_TABLE_MAPPING = {
    "VOC": {
        "VOC_1_1": "VOC_1_1_cusrevtimecompany_v",
        "VOC_1_2": "VOC_1_2_cusrevstat_v",
        "VOC_1_3_1": "VOC_1_3_1_cusrevcompany_p_v",
        "VOC_1_3_2": "VOC_1_3_2_cusrevcompany_b_v",
        "VOC_1_4": "VOC_1_4_cusrevtimelangue_v",
        "VOC_1_4_1": "VOC_1_4_1_cusrevtimecountry_v",
        "VOC_1_5": "VOC_1_5_cusrevlangue_p_v",
        "VOC_1_5_1": "VOC_1_5_1_cusrevcountry_p_v",
        "VOC_1_6": "VOC_1_6_cusrevtimesource_v",
        "VOC_1_7": "VOC_1_7_cusrevprocessed_v",
        "VOC_1_8": "VOC_1_8_avgcusrevcompany",
        "VOC_1_9": "VOC_1_9_avgcusrevyearlycompany",
        "VOC_2": "VOC_2_polaritydistr_v",
        "VOC_3": "VOC_3_ratingtime",
        "VOC_4_1": "VOC_4_1_sstimecompany",
        "VOC_4_2": "VOC_4_2_cusrevsstime",
        "VOC_4_3": "VOC_4_3_avgsscompany",
        "VOC_4_4": "VOC_4_4_avgssyearlycompany",
        "VOC_5": "VOC_5_heatmapdim",
        "VOC_6_1": "VOC_6_1_sstimedimcompany",
        "VOC_6_2": "VOC_6_2_dimentioned",
        "VOC_6_3": "VOC_6_3_dimpolarity",
        "VOC_6_4": "VOC_6_4_dimss",
        "VOC_6_5": "VOC_6_5_competitor",
        "VOC_6_6": "VOC_6_6_sstimesourcecompany",
        "VOC_6_7": "VOC_6_7_commonterms",
        "VOC_6_7_1": "VOC_6_7_1_commonkeywords",
        "VOC_6_8": "VOC_6_8_avgssdimcompany",
        "VOC_6_9": "VOC_6_9_avgssdimyearlycompany",
        "nlp_output": "nlp_output",
        "review_raw_data": "review_raw_data",
        "VOC_11": "VOC_11_profilestats",
        "VOC_12": "VOC_12_sscausalmining",
        "VOC_12_1": "VOC_12_1_sscausaldim",
        "VOC_12_2": "VOC_12_2_sscausalpolarity",
        "VOC_13": "VOC_13_ratingtimecompany",
        "VOC_13_1": "VOC_13_1_avgratingcompany",
        "VOC_13_2": "VOC_13_2_avgratingperiodcompany",
        "VOC_14": "VOC_14_nvsrating",
        "VOC_15": "VOC_15_nvsratingtime",
        "VOC_16": "VOC_16_kpcsentcompany"
    },
    "VOE": {
        "VOE_1_2": "VOE_1_2_emprevstat_v",
        "VOE_1_3_2": "VOE_1_3_2_emprevcompany_b_v",
        "VOE_1_4": "VOE_1_4_emprevtimelangue_v",
        "VOE_1_5": "VOE_1_5_emprevlangue_p_v",
        "VOE_1_6": "VOE_1_6_emprevtimesource_v",
        "VOE_1_7": "VOE_1_7_emprevprocessed_v",
        "VOE_2": "VOE_2_polaritydistr_v",
        "VOE_3": "VOE_3_ratingtime",
        "VOE_4_2": "VOE_4_2_emprevsstime",
        "VOE_5": "VOE_5_heatmapdim",
        "VOE_6_1": "VOE_6_1_sstimedimcompany",
        "VOE_6_3": "VOE_6_3_dimpolarity",
        "VOE_6_5": "VOE_6_5_competitor",
        "VOE_6_6": "VOE_6_6_sstimesourcecompany",
        "VOE_6_8": "VOE_6_8_avgssdimcompany",
        "VOE_6_9": "VOE_6_9_avgssdimyearlycompany",
        "VOE_7_1": "VOE_7_1_jobfunctionquant",
        "VOE_7_2": "VOE_7_2_jobfunctionpercent",
        "VOE_7_3": "VOE_7_3_jobfunctionvFTE",
        "VOE_7_4": "VOE_7_4_jobbycountry_t",
        "VOE_7_5": "VOE_7_5_jobbycountry_p",
        "VOE_7_6": "VOE_7_6_jobfunctionbyday",
        "VOE_8_1": "VOE_8_1_roleseniorquant",
        "VOE_8_4": "VOE_8_4_roleseniorbyjobtype",
        "VOE_9_3": "VOE_9_3_ssaverage",
        "VOE_6_2": "VOE_6_2_dimentioned",
        "VOE_9_1": "VOE_9_1_reviewsbycompany_p",
        "VOE_9_4": "VOE_9_4_reviewsbycompany",
        "VOE_9_4_1": "VOE_9_4_1_avgcusrevcompany",
        "VOE_9_4_2": "VOE_9_4_2_avgcusrevyearlycompany",
        "VOE_9_5": "VOE_9_5_ssbytimecompany",
        "VOE_9_5_1": "VOE_9_5_1_avgsscompany",
        "VOE_9_5_2": "VOE_9_5_2_avgssyearlycompany",
        "VOE_10_1": "VOE_10_1_dimss",
        "VOE_11": "VOE_11_termcount",
        "VOE_11_1": "VOE_11_1_keywordcount",
        "VOE_12": "VOE_12_ratingtimecompany",
        "VOE_12_1": "VOE_12_1_avgratingcompany",
        "VOE_12_2": "VOE_12_2_avgratingperiodcompany",
        "VOE_13": "VOE_13_profilestats",
        "VOE_14": "VOE_14_nvsrating",
        "VOE_15": "VOE_15_nvsratingtime",
        "VOE_16": "VOE_16_signalsentcompany"
    },
    "HRA": {
        "HRA_1": "HRA_1_profilestats",
    },
}

NLP_DATA_MART_TABLE_MAPPING = {
    "VOC": {
        "nlp_output": {
            "table": "nlp_output",
            "prefix": "nlp_output"
        },
        "review_raw_data": {
            "table": "review_raw_data",
            "prefix": "customer_review_raw_data"
        }
    },
    "VOE": {
        "voe_nlp_output": {
            "table": "voe_nlp_output",
            "prefix": "voe_nlp_output"
        },
        "voe_review_raw_data": {
            "table": "voe_review_raw_data",
            "prefix": "employee_review_raw_data"
        },
        "voe_job_postings": {
            "table": "voe_job_postings",
            "prefix": "job_postings_data"
        },
    },
    "HRA": {
        "employee_experiences": {
            "table": "HRA_employee_experiences",
            "prefix": "job_experience"
        },
        "employee_profiles": {
            "table": "HRA_employee_profiles",
            "prefix": "employee_profiles"
        },
        "company_profiles": {
            "table": "HRA_company_profiles",
            "prefix": "company_profiles"
        },
        "monthly_dataset":
        {
            "table": "HRA_monthly_dataset",
            "prefix": "monthly_dataset"
        },
        "education_dataset":
        {
            "table": "HRA_education_dataset",
            "prefix": "education_dataset"
        },
        "turnover_dataset":
        {
            "table": "HRA_turnover_dataset",
            "prefix": "turnover_dataset"
        },
    }
}

CHART_DATA_MART_SCRIPT_MAPPING = {
    "VOC": {
        "VOC_1_1": "voc_1_1",
        "VOC_1_2": "voc_1_2",
        "VOC_1_3_1": "voc_1_3_1",
        "VOC_1_3_2": "voc_1_3_2",
        "VOC_1_4": "voc_1_4",
        "VOC_1_4_1": "voc_1_4_1",
        "VOC_1_5": "voc_1_5",
        "VOC_1_5_1": "voc_1_5_1",
        "VOC_1_6": "voc_1_6",
        "VOC_1_7": "voc_1_7",
        "VOC_1_8": "voc_1_1",
        "VOC_1_9": "voc_1_1",
        "VOC_2": "voc_2",
        "VOC_3": "voc_3",
        "VOC_4_1": "voc_4_1",
        "VOC_4_2": "voc_4_2",
        "VOC_4_3": "voc_4_1",
        "VOC_4_4": "voc_4_1",
        "VOC_5": "voc_5",
        "VOC_6_1": "voc_6_1",
        "VOC_6_2": "voc_6_2",
        "VOC_6_3": "voc_6_3",
        "VOC_6_4": "voc_6_4",
        "VOC_6_5": "voc_6_5",
        "VOC_6_6": "voc_6_6",
        "VOC_6_7": "voc_6_7",
        "VOC_6_7_1": "voc_6_7_1",
        "VOC_6_8": "voc_6_1",
        "VOC_6_9": "voc_6_1",
        "nlp_output": "nlp_output",
        "review_raw_data": "review_raw_data",
        "VOC_11": "voc_11",
        "VOC_12": "voc_4_1",
        "VOC_12_1": "voc_12_1",
        "VOC_12_2": "voc_12_2",
        "VOC_13": "voc_13",
        "VOC_13_1": "voc_13",
        "VOC_13_2": "voc_13",
        "VOC_14": "voc_14",
        "VOC_15": "voc_15",
        "VOC_16": "voc_16",
        "summary_table": "voc_summary_table"
    },
    "VOE": {
        "summary_table": "voe_summary_table",
        "VOE_1_2": "voe_1_2",
        "VOE_1_3_2": "voe_1_3_2",
        "VOE_1_4": "voe_1_4",
        "VOE_1_5": "voe_1_5",
        "VOE_1_6": "voe_1_6",
        "VOE_1_7": "voe_1_7",
        "VOE_2": "voe_2",
        "VOE_3": "voe_3",
        "VOE_4_2": "voe_4_2",
        "VOE_5": "voe_5",
        "VOE_6_1": "voe_6_1",
        "VOE_6_2": "voe_6_2",
        "VOE_6_3": "voe_6_3",
        "VOE_6_5": "voe_6_5",
        "VOE_6_6": "voe_6_6",
        "VOE_6_8": "voe_6_1",
        "VOE_6_9": "voe_6_1",
        "VOE_9_1": "voe_9_1",
        "VOE_9_3": "voe_9_3",
        "VOE_9_4": "voe_9_4",
        "VOE_9_4_1": "voe_9_4",
        "VOE_9_4_2": "voe_9_4",
        "VOE_9_5": "voe_9_5",
        "VOE_9_5_1": "voe_9_5",
        "VOE_9_5_2": "voe_9_5",
        "VOE_10_1": "voe_10_1",
        "VOE_11": "voe_11",
        "VOE_11_1": "voe_11_1",
        "VOE_12": "voe_12",
        "VOE_12_1": "voe_12",
        "VOE_12_2": "voe_12",
        "VOE_13": "voe_13",
        "VOE_14": "voe_14",
        "VOE_15": "voe_15",
        "VOE_16": "voe_16"
    },
    "VOE_JOB": {
        "summary_table": "voe_job_summary_table",
        "VOE_JOB_7_1": "voe_job_7_1",
        "VOE_JOB_7_2": "voe_job_7_2",
        "VOE_JOB_7_3": "voe_job_7_3",
        "VOE_JOB_7_4": "voe_job_7_4",
        "VOE_JOB_7_5": "voe_job_7_5",
        "VOE_JOB_7_6": "voe_job_7_6",
        "VOE_JOB_8_1": "voe_job_8_1",
        "VOE_JOB_8_4": "voe_job_8_4",
    },
    "HRA": {
        "HRA_1": "hra_1"
    },
}

CASE_STUDY_NLP_OUTPUT_TYPE = 'nlp_output'
CASE_STUDY_NLP_INDEX_PREFIX = os.environ.get(
    'CASE_STUDY_NLP_INDEX_PREFIX', 'dummy')
ELASTICSEARCH_HOST = os.environ.get('ELASTICSEARCH_HOST', 'dummy')
