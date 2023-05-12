import requests
import re
import json
import logging
import gzip
import io
import pymongo
from google.cloud import storage
import backend_crawler_logger
import xml.etree.ElementTree as ET
import time
import random
from bs4 import BeautifulSoup
from backend_crawler.mongo import add_data_to_pitchbook_crawl_collection
from backend_crawler.mongo import DB as mongo_db
from database import Session, BackendCrawler
from config import LUMINATI_HTTP_PROXY, LUMINATI_WEBUNBLOCKER_HTTP_PROXY, PITCHBOOK_CRAWL_COLLECTION, PITCHBOOK_SOURCE_NAME
import datetime
import pandas as pd
from config import GCP_STORAGE_BUCKET
import os
from mongo import get_data
time_list = [0.3, 0.6, 0.9, 1.2]

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    "authority": "pitchbook.com"
}
url_sitemap = 'https://pitchbook.com/sitemap.xml'
logger = backend_crawler_logger.logger


# @retry(tries=5, delay=1, jitter=2)
def request_get(*args, **kwargs):
    return requests.get(*args, **kwargs, verify=False)


def get_sitemap():
    response = request_get(url_sitemap)
    soup = BeautifulSoup(response.text, 'html.parser')
    file_urls = []
    for elm in soup.select("sitemap"):
        loc_elm = elm.select_one("loc")
        loc_val = loc_elm.get_text(strip=True) if loc_elm else ""
        if not loc_val.startswith("https://pitchbook.com/sitemap-public-profiles"):
            continue
        if not loc_val.endswith(".xml.gz"):
            continue

        lastmod_elm = elm.select_one("lastmod")
        lastmod_val = lastmod_elm.get_text(strip=True) if lastmod_elm else None

        file_urls.append(loc_val)

    return file_urls


def parse_sitemap_gz(xml_data):
    company_urls = []
    root = ET.fromstring(xml_data)

    namespace = {'sitemap': root.tag.split('}')[0].strip('{')}

    for url_element in root.findall('sitemap:url', namespace):
        company_urls.append({
            'url': url_element.find('sitemap:loc', namespace).text,
            'lastmod': url_element.find('sitemap:lastmod', namespace).text
        })
    return company_urls


def extract_gz(file_url):
    compressed_data = request_get(file_url).content
    with gzip.open(io.BytesIO(compressed_data), 'rt') as f:
        xml_data = f.read()

    company_urls = parse_sitemap_gz(xml_data)
    return company_urls


def get_id(url):
    pattern = r'https://pitchbook.com/profiles/(?:[a-zA-Z-]+)/(\d+-\d+)'
    match = re.match(pattern, url)
    if match:
        extracted_id = match.group(1)
        return extracted_id
    else:
        return None


def get_country(html_parser):
    raw_data = html_parser.find('script').get_text()
    data = json.loads(raw_data)
    country = ''
    for item in data:
        if item.get('@type') == 'Country':
            country = item.get('name')

    return country


def get_general_info_data(html_parser, url_type):
    if url_type == 'company':
        general_info = html_parser.find(
            "div", {"aria-label": "Company General information"})
    elif url_type == 'investor':
        general_info = html_parser.find(
            "div", {"aria-label": "Investor General information"})
    elif url_type == 'advisor':
        general_info = html_parser.find(
            "div", {"aria-label": "Advisor General information"})
    elif url_type == 'person':
        general_info = html_parser.find(
            "div", {"aria-label": "Person General information"})
    elif url_type == 'limited-partner':
        general_info = html_parser.find(
            "div", {"aria-label": "Limited Partner General information"})
    else:
        return {}
    head_general_info = html_parser.find_all(
        'ul', {'class': 'height-100 list-type-none flex-container flex-justify-center-XL flex-direction-column flex-end'})
    contact_info_blocks = general_info.findChildren(
        "div", recursive=False)[-1].findChildren("div", recursive=False)

    company_name = html_parser.find(
        "h1", {"class": "pp-search-wrap__title font-color-white mb-xl-0 pr-s-5 pr-xs-45"})
    if company_name is not None:
        company_name = company_name.text

    description = general_info.find(
        "p", {"class": "pp-description_title mb-xl-0"})
    if description is not None:
        description = description.get_text()

    info_data = {}
    _block_index = 0
    for contact_info_block in contact_info_blocks:
        contact_info_block_children = contact_info_block.findChildren(
            recursive=False)
        if _block_index != 2:
            for contact_info_block_child in contact_info_block_children:
                contact_info_block_child_data = contact_info_block_child.findChildren(
                    recursive=False)
                key = contact_info_block_child_data[0].get_text()
                value = ", ".join([i.get_text()
                                  for i in contact_info_block_child_data[1:]])
                info_data.setdefault(_format_key(key), value)
        else:
            text = contact_info_block_children[0].get_text().split('\n')
            text = [item for item in text if item != '']
            key = text[0]
            value = ", ".join([i for i in text[1:]])
            info_data.setdefault(_format_key(key), value)

            link_infos = contact_info_block.find_all("a")
            for link_info in link_infos:
                key = link_info["aria-label"]
                info_data.setdefault(_format_key(key), link_info["href"])

        _block_index += 1

    for ele in head_general_info:
        key = ele.find('li', {'class': 'dont-break text-small'}).text
        value = ele.find('h2').text.replace('\u200b', '')
        info_data[str(key).strip()] = value

    if info_data.get("Employees") is not None:
        info_data["Employees"] = int(
            info_data.get("Employees").replace(',', ''))
    if info_data.get("Financing Rounds") is not None:
        info_data["Financing Rounds"] = int(info_data.get("Financing Rounds"))

    info_data['description'] = description
    info_data['company_name'] = company_name
    return info_data


def _format_key(key):
    return key.replace("#", "number").replace(
        "%", "percent").replace(" ", "_").replace("._", "_").lower()


def get_url_type(url):
    pattern = r'/profiles/(company|advisor|person|investor|limited-partner)/'
    match = re.search(pattern, url)

    if match:
        profile_type = match.group(1)
        return profile_type
    else:
        return None

def upload_pitchbook_csv_to_gcs(data: list):
    time_write = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # save locally
    df = pd.DataFrame(data)
    # mkdirs_if_not_exists(pitchbook_id)
    output_file = f"{time_write}.csv"
    df.to_csv(output_file, index=False)
    # upload to gcs
    dst_file = f"{output_file}"
    url = upload_google_storage(src_file=output_file, dst_file=dst_file)
    print(f"Uploaded {dst_file} to {url}")
    delete_local_csv(output_file)

def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
    expiration = datetime.timedelta(days=1)
    url = blob.generate_signed_url(expiration=expiration)
    return url
def delete_local_csv(filename):
    if os.path.exists(filename):
        os.remove(filename)
        print(f"Deleted local file {filename}")
    else:
        print(f"Local file {filename} not found, cannot delete")
def execute(payload=None):
    logger.info(
        f"Backend crawler, pitchbook, start execute pitchbook crawler with payload: {payload}")
    result = []
    company_urls = []
    last_crawled_pitchbook_id = None
    if payload:
        last_crawled_pitchbook_id = payload["pitchbook_id"]

    # set is_runing = true and in_run_now = false when start
    session = Session()
    backend_crawler = session.query(BackendCrawler).filter(
        BackendCrawler.is_active,
        BackendCrawler.source_name == "pitchbook"
    ).first()
    backend_crawler.is_runing = True
    backend_crawler.is_run_now = False
    session.commit()
    session.close()

    try:
        params = {}
        if LUMINATI_HTTP_PROXY:
            params['proxies'] = {
                'http': LUMINATI_HTTP_PROXY,
                'https': LUMINATI_HTTP_PROXY
            }
        file_urls = get_sitemap()
        for file_url in file_urls:
            company_urls.extend(extract_gz(file_url))

        for item in company_urls:
            try:
                url = item.get('url')
                pitchbook_id = get_id(url)
                _last_updated_date = item.get('lastmod')
                logger.info(
                    f"Backend crawler, pitchbook, pitchbook_id: {pitchbook_id}, last_updated_date: {_last_updated_date}")

                # skip crawled pitchbook_id when crawler beark and re-crawl
                if last_crawled_pitchbook_id:
                    if pitchbook_id != last_crawled_pitchbook_id:
                        continue
                    last_crawled_pitchbook_id = None
                    continue

                # skip crawl unchange pitchbook_id
                _info = mongo_db[PITCHBOOK_CRAWL_COLLECTION].find_one(
                    filter={
                        f"{PITCHBOOK_SOURCE_NAME}_id": pitchbook_id,
                    },
                    sort=[("data_version", pymongo.DESCENDING)],
                    projection={
                        "_id": 0,
                        f"{PITCHBOOK_SOURCE_NAME}_id": 1,
                        "data_version": 1,
                        "last_updated_date": 1
                    },
                )

                if _info:
                    if _info["last_updated_date"] == _last_updated_date:
                        continue

                url_type = get_url_type(url)
                response = request_get(url, **params)
                html_parser = BeautifulSoup(response.content, "html.parser")

                general_info_data = get_general_info_data(
                    html_parser, url_type)

                _item = {
                    "company_name": general_info_data.get('company_name'),
                    "pitchbook_id": pitchbook_id,
                    "employees": general_info_data.get('Employees'),
                    "primary_industry": general_info_data.get('primary_industry'),
                    "status": general_info_data.get('Status'),
                    "country": get_country(html_parser),
                    "website": general_info_data.get('website'),
                    "last_updated_date": _last_updated_date,
                    "description": general_info_data.get('description'),
                    "latest_deal_type": general_info_data.get('Latest Deal Type'),
                    "financial_rounds": general_info_data.get('Financing Rounds'),
                    "financing_status": general_info_data.get('financing_status'),
                    "other_industries": general_info_data.get('other_industries'),
                    "ownership_status": general_info_data.get('ownership_status')
                }

                result.append(_item)
                add_data_to_pitchbook_crawl_collection(_item)
            except Exception as e:
                logger.info(
                    f"Backend crawler, pitchbook, [ERROR]: {e} -> url: {url}")
            finally:
                # update crawled pichbook_id
                session = Session()
                backend_crawler = session.query(BackendCrawler).filter(
                    BackendCrawler.is_active,
                    BackendCrawler.source_name == "pitchbook"
                ).first()
                backend_crawler.set_jsonb_attr(
                    "temp_data", {"pitchbook_id": pitchbook_id})
                session.commit()
                session.close()

                # random delay progress
                time.sleep(random.choice(time_list))
        upload_pitchbook_csv_to_gcs(get_data())
        # set is_runing = false when done
        session = Session()
        backend_crawler = session.query(BackendCrawler).filter(
            BackendCrawler.is_active,
            BackendCrawler.source_name == "pitchbook"
        ).first()
        backend_crawler.is_runing = False
        session.commit()
        session.close()

        return result

    except Exception as e:
        logger.info(
            f"Backend crawler, pitchbook , [ERROR]: {e}")


# if __name__ == '__main__':
#     execute()
