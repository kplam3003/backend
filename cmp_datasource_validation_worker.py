import datetime
import json
import time
import logger
import config
import service
import pycountry
import re
import csv
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO
from pubsub import GCPPubsubReceiver
from google.cloud import pubsub_v1
from src.services.company_data_source import update_company_datasource
from src.services.company_data_source import check_company_datasource_validation

logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)

SLEEP_DEFAULT = 1
MAX_MESSAGES = 100
POOL_SIZE = 100
DELAY_TIME = 1
# for company
MIN_COMPANY_SIZE = 'min_company_size'
MAX_COMPANY_SIZE = 'max_company_size'
# for review
REVIEW_ID = 'review_id'
RATING = 'rating'
REVIEW = 'review'
REVIEW_AT = 'review_at'
#for job
JOB_ID = 'job_id'
TITLE = 'title'
LOCATION = 'location'
COUNTRY = 'country'
POSTED_AT = 'posted_at'
JOB_TYPE = 'job_type'
JOB_FUNCTION = 'job_function'

URL_TYPE_VOC_CSV = "VOC_CSV"
URL_TYPE_VOE_CSV = "VOE_CSV"

# Read the data from Google Cloud Storage
CREDENTIALS = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
STORAGE_CLIENT = storage.Client(credentials=CREDENTIALS, project=CREDENTIALS.project_id)

float_rex = re.compile(r'^-?\d+(?:\.\d+)$')
datetime_rex = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$')

def validate_unique_id(row, unique_id_collections, unique_id, header_config):
    _unique_id = row[header_config[unique_id]]
    if not _unique_id:
        return False, f"{unique_id} must be not null"
    # check unique review id
    if _unique_id in unique_id_collections:
        return False, f"{unique_id} Already Exists"
    unique_id_collections[_unique_id] = True
    return True, ''

def validate_rating(row, rating, header_config):
    _rating = row[header_config[rating]]
    if not _rating:
        return False, f"{rating} must be not NULL"

    if not float_rex.match(_rating) and not _rating.isdigit():
        return False, f"{rating} is invalid"

    _rating = float(_rating)
    if _rating < 0 or _rating > 5.0:
        return False, f"{rating} is out of range [0.0, 5.0]"
    return True, ''

def validate_text(row, text, header_config):
    _text = row[header_config[text]]
    if not _text:
        return False, f"{text} must be not NULL"
    return True, ''

def validate_datetime(row, datetime, header_config):
    _datetime = row[header_config[datetime]]
    if not _datetime:
        return False, f"{datetime} at is not NULL"
    if datetime_rex.match(_datetime) is None:
        return False, 'ReviewAt is not correct format[YYYY-MM-DDTHH:mm:ss.sssZ]'
    return True, ''

def validate_header(headers, header_template):
    if len(headers) != len(header_template):
        return False, 'Number of Header is incorrect!, it should be [' + '|'.join(header_template) + ']'
    str_header_errs = []
    for i in headers:
        if not i in header_template:
            str_header_errs.append(i)
    if len(str_header_errs) > 0:
        return False, ('|'.join(str_header_errs) + ': incorrect!, should be include in [' + '|'.join(header_template) + ']')
    return True, ''

def validate_company_size(row, company_size, header_config):
    _company_size = row[header_config[company_size]]
    if not _company_size:
        return False, f"{company_size} at is not NULL"
    if not _company_size.isdigit():
        return False, f"{company_size} all of characters are digits"
    if int(_company_size) < 0:
        return False, f"{company_size} must is greater than 0"
    return True, ""

def validate_country_name(row, key, header_config):
    _country_name = row[header_config[key]]
    if not _country_name:
        return False, f"{_country_name} must be not NULL"

    return True, ""



def validate_voc(payload):
    review_id_collections = {}
    header_template = [REVIEW_ID, RATING, REVIEW, REVIEW_AT]
    header_config = {
        v: i
        for i, v in enumerate(header_template)
    }

    bucket_name = payload['bucket_name']
    blob_download = payload['blob_name']
    company_datasource_id = payload['company_datasource_id']

    if check_company_datasource_validation(company_datasource_id):
        logger.warning(f"[VOC] Company data source ID {company_datasource_id} is processing...!")

    # Set buckets and blobs
    blob_name_split = blob_download.split('.')
    blob_error =  blob_name_split[0] + "_error." + blob_name_split[1]

    bucket = STORAGE_CLIENT.bucket(bucket_name)
    blob = bucket.blob(blob_download)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8-sig')
    blob = StringIO(blob)
    data = csv.reader(blob)
    headers = next(data)

    _flag_validate_header, _validate_header_error = validate_header(headers, header_template)
    csv_string_to_error_upload = []
    is_error = False
    if _flag_validate_header:
        csv_string_to_error_upload.append(','.join(['row'] + header_template))
        for index, row in enumerate(data, start = 1):
            csv_error_item = [str(index + 1)]
            _flag_review_id_error, _review_id_error = validate_unique_id(row, review_id_collections, REVIEW_ID, header_config)
            csv_error_item.append(f'"{_review_id_error}"')
            _flag_rating_error, _rating_error = validate_rating(row, RATING, header_config)
            csv_error_item.append(f'"{_rating_error}"')
            _flag_review_error, _review_error = validate_text(row, REVIEW, header_config)
            csv_error_item.append(f'"{_review_error}"')
            _flag_review_at_error, _review_at_error = validate_datetime(row, REVIEW_AT, header_config)
            csv_error_item.append(f'"{_review_at_error}"')
            if not (_flag_review_id_error and _flag_rating_error and _flag_review_error and _flag_review_at_error):
                csv_string_to_error_upload.append(','.join(csv_error_item))
                is_error = True

            validate_progress = float(0.5)
            if index%100 == 0:
                if not update_company_datasource(company_datasource_id, validate_progress, True):
                    logger.error(f"NOT UPDATE DATA FOR COMPANY DATASOURCE: company_datasource_id={company_datasource_id}, validate_progress={validate_progress}")
    else:
        is_error = True
        logger.info("Header is not correct")
        csv_string_to_error_upload.append(_validate_header_error)

    if is_error:
        blob_upload_error = bucket.blob(blob_error)
        blob_upload_error.upload_from_string(
                    data='\n'.join(csv_string_to_error_upload),
                    content_type='text/csv'
                )
        if not update_company_datasource(company_datasource_id, float(1), False):
            logger.error(f"NOT UPDATE DATA FOR COMPANY DATASOURCE: company_datasource_id={company_datasource_id}, validate_progress={1}")
    else:
        if not update_company_datasource(company_datasource_id, float(1), True):
            logger.error(f"NOT UPDATE DATA FOR COMPANY DATASOURCE: company_datasource_id={company_datasource_id}, validate_progress={1}")

def validate_voe(payload):
    review_id_collections = {}
    job_id_collections = {}
    company_header = [MIN_COMPANY_SIZE, MAX_COMPANY_SIZE]
    job_header = [JOB_ID, TITLE, LOCATION, COUNTRY, POSTED_AT, JOB_TYPE, JOB_FUNCTION]
    review_header = [REVIEW_ID, RATING, REVIEW, REVIEW_AT]
    header_template = company_header + job_header + review_header
    header_config = {
        v: i
        for i, v in enumerate(header_template)
    }

    bucket_name = payload['bucket_name']
    blob_download = payload['blob_name']
    company_datasource_id = payload['company_datasource_id']

    if check_company_datasource_validation(company_datasource_id):
        logger.warning(f"[VOE] Company data source ID {company_datasource_id} is processing...!")

    # Set buckets and blobs
    blob_name_split = blob_download.split('.')
    blob_error =  blob_name_split[0] + "_error." + blob_name_split[1]
    blob_company = blob_name_split[0] + "_overview." + blob_name_split[1]
    blob_job = blob_name_split[0] + "_job." + blob_name_split[1]
    blob_review = blob_name_split[0] + "_review." + blob_name_split[1]

    bucket = STORAGE_CLIENT.bucket(bucket_name)
    blob = bucket.blob(blob_download)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8-sig')
    blob = StringIO(blob)
    data = csv.reader(blob)
    headers = next(data)

    _flag_validate_header, _validate_header_error = validate_header(headers, header_template)
    csv_string_to_error_upload = []
    company_string_csv = []
    job_string_csv = []
    review_string_csv = []
    is_error = False
    if _flag_validate_header:
        csv_string_to_error_upload.append(','.join(['row'] + header_template))
        company_string_csv.append(','.join(company_header))
        job_string_csv.append(','.join(job_header))
        review_string_csv.append(','.join(review_header))
        for index, row in enumerate(data, start = 1):
            csv_error_item = [str(index + 1)]
            # validate company
            _flag_min_company_size_error, _min_company_size_error = validate_company_size(row, MIN_COMPANY_SIZE, header_config)
            _flag_max_company_size_error, _max_company_size_error = validate_company_size(row, MAX_COMPANY_SIZE, header_config)
            company_condition =  _flag_min_company_size_error + _flag_max_company_size_error
            if company_condition > 0:
                csv_error_item.extend([f'"{_min_company_size_error}"', f'"{_max_company_size_error}"'])
            else:
                csv_error_item.extend(['']*len(company_header))

            if company_condition == len(company_header):
                company_string_csv.append(','.join([f'{row[header_config[MIN_COMPANY_SIZE]]}', f'{row[header_config[MAX_COMPANY_SIZE]]}']))

            # validate jobs
            _flag_job_id_error, _job_id_error = validate_unique_id(row, job_id_collections, JOB_ID, header_config)
            _flag_title_error, _title_error = validate_text(row, TITLE, header_config)
            _flag_location_error, _location_error = validate_text(row, LOCATION, header_config)
            _flag_country_error, _country_error = validate_country_name(row, COUNTRY, header_config)
            _flag_posted_at_error, _posted_at_error = validate_datetime(row, POSTED_AT, header_config)
            _flag_job_type_error, _job_type_error = validate_text(row, JOB_TYPE, header_config)
            _flag_job_function_error, _job_function_error = validate_text(row, JOB_FUNCTION, header_config)
            job_condition = _flag_job_id_error + _flag_title_error + _flag_location_error + _flag_country_error \
                            + _flag_posted_at_error + _flag_job_type_error + _flag_job_function_error
            if job_condition > 0:
                csv_error_item.extend([f'"{_job_id_error}"', f'"{_title_error}"', f'"{_location_error}"',
                                        f'"{_country_error}"', f'"{_posted_at_error}"', f'"{_job_type_error}"', f'"{_job_function_error}"'])
            else:
                csv_error_item.extend(['']*len(job_header))

            if job_condition == len(job_header):
                job_string_csv.append(','.join([f'"{row[header_config[JOB_ID]]}"', f'"{row[header_config[TITLE]]}"', f'"{row[header_config[LOCATION]]}"', f'"{row[header_config[COUNTRY]]}"',
                                                f'"{row[header_config[POSTED_AT]]}"', f'"{row[header_config[JOB_TYPE]]}"', f'"{row[header_config[JOB_FUNCTION]]}"']))

            # validate reviews
            _flag_review_id_error, _review_id_error = validate_unique_id(row, review_id_collections, REVIEW_ID, header_config)
            _flag_rating_error, _rating_error = validate_rating(row, RATING, header_config)
            _flag_review_error, _review_error = validate_text(row, REVIEW, header_config)
            _flag_review_at_error, _review_at_error = validate_datetime(row, REVIEW_AT, header_config)
            review_condition = _flag_review_id_error + _flag_rating_error + _flag_review_error + _flag_review_at_error
            if review_condition > 0:
                csv_error_item.extend([f'"{_review_id_error}"', f'"{_rating_error}"', f'"{_review_error}"', f'"{_review_at_error}"'])
            else:
                csv_error_item.extend(['']*len(review_header))

            if review_condition == len(review_header):
                review_string_csv.append(','.join([f'"{row[header_config[REVIEW_ID]]}"', f'"{row[header_config[RATING]]}"',
                                                    f'"{row[header_config[REVIEW]]}"', f'"{row[header_config[REVIEW_AT]]}"']))

            if company_condition > 0 and company_condition < len(company_header) or \
                job_condition > 0 and job_condition < len(job_header) or \
                review_condition > 0 and review_condition < len(review_header):
                is_error = True
                csv_string_to_error_upload.append(','.join(csv_error_item))
            validate_progress = float(0.5)
            if index%100 == 0:
                if not update_company_datasource(company_datasource_id, validate_progress, True):
                    logger.error(f"NOT UPDATE DATA FOR COMPANY DATASOURCE: company_datasource_id={company_datasource_id}, validate_progress={validate_progress}")
    else:
        is_error = True
        logger.info("Header is not correct")
        csv_string_to_error_upload.append(_validate_header_error)

    if is_error:
        blob_upload_error = bucket.blob(blob_error)
        blob_upload_error.upload_from_string(
                    data='\n'.join(csv_string_to_error_upload),
                    content_type='text/csv')
        if not update_company_datasource(company_datasource_id, float(1), False):
            logger.error(f"NOT UPDATE DATA FOR COMPANY DATASOURCE: company_datasource_id={company_datasource_id}, validate_progress={1}")
    else:
        blob_company, blob_job, blob_review
        blob_upload_company = bucket.blob(blob_company)
        blob_upload_company.upload_from_string(
                    data='\n'.join(company_string_csv),
                    content_type='text/csv')
        blob_upload_job = bucket.blob(blob_job)
        blob_upload_job.upload_from_string(
                    data='\n'.join(job_string_csv),
                    content_type='text/csv')
        blob_upload_review = bucket.blob(blob_review)
        blob_upload_review.upload_from_string(
                    data='\n'.join(review_string_csv),
                    content_type='text/csv')

        if not update_company_datasource(company_datasource_id, float(1), True):
            logger.error(f"NOT UPDATE DATA FOR COMPANY DATASOURCE: company_datasource_id={company_datasource_id}, validate_progress={1}")

def handle_ds_company_validation_progress_task(payload):
    try:
        logger.info(f"Begin task: {payload}")
        url_type = None
        if not payload:
            logger.error("NULL payload.")

        if 'url_type' in payload:
            url_type = payload['url_type']

        if url_type and url_type == URL_TYPE_VOE_CSV:
            validate_voe(payload)
        else:
            validate_voc(payload)

        logger.info(f"End task: {payload}")
    except Exception as e:
        logger.exception(e)

def subscribe(gcp_product_id, gcp_pubsub_subscription, service_account_key_path):
    subscriber = GCPPubsubReceiver(gcp_product_id, gcp_pubsub_subscription, service_account_key_path)
    while True:
        logger.info(f"Waiting for a message on {gcp_pubsub_subscription}...")
        response = subscriber.pull_message(MAX_MESSAGES)
        if not response:
            time.sleep(DELAY_TIME)
            logger.info(f"Sleeping for {DELAY_TIME}")
            continue

        ack_ids = []
        messages = []
        for received_message in response.received_messages:
            logger.info(f"Received: {received_message.message.data}.")
            ack_ids.append(received_message.ack_id)
            messages.append(received_message.message.data)

        try:
            for message in messages:
                data = json.loads(message, strict=False)
                handle_ds_company_validation_progress_task(data)
            subscriber.ack_messages(ack_ids)
        except Exception as error:
            logger.error(f"Exception: error={error}, messages={messages}")

if __name__ == "__main__":
    subscribe(config.GCP_WEBAPP_PROJECT_ID,
                config.GCP_WEBAPP_PUBSUB_VALIDATE_COMPANY_DS_SUBSCRIPTION,
                config.GCP_WEBAPP_SERVICE_ACCOUNT_KEY_PATH)
