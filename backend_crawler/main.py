import time
import threading
import backend_crawler_logger

from datetime import datetime
from database import Session, BackendCrawler
from crawlers.pitchbook import execute as pitchbook_execute
from config import PITCHBOOK_SOURCE_NAME

BACKEND_CRAWLERS = [PITCHBOOK_SOURCE_NAME,]
EXECUTORS = {
    PITCHBOOK_SOURCE_NAME: pitchbook_execute,
}
logger = backend_crawler_logger.logger


def run_executor(crawler_name, temp_data=None):
    logger.info(
            f"Backend crawler, execute crawlers: {crawler_name}, temp_data: {temp_data}")
    # Add crawler name to set of running crawlers
    running_crawlers.add(crawler_name)
    EXECUTORS[crawler_name](temp_data)
    # Remove crawler name from set when executor finishes
    running_crawlers.remove(crawler_name)


while True:
    session = Session()
    running_crawlers = set()
    logger.info(
        f"Backend crawler -----------------------------")
    try:
        crawlers = session.query(BackendCrawler).filter(
            BackendCrawler.is_active
        ).all()
        _exist_crawlers = []

        for crawler in crawlers:
            crawler_name = crawler.source_name
            if crawler_name not in BACKEND_CRAWLERS:
                logger.info(
                    f"Backend crawler, skip invalid crawler")
                continue

            _exist_crawlers.append(crawler_name)
            if crawler.is_runing:
                if crawler_name in running_crawlers:
                    continue

                temp_data = crawler.temp_data
                threading.Thread(target=run_executor, args=(
                    crawler_name, temp_data)).start()
                continue

            if crawler.is_run_now:
                if crawler_name in running_crawlers:
                    continue

                threading.Thread(target=run_executor, args=(
                    crawler_name, None)).start()
                continue

            if datetime.now().day == 1:
                crawler.is_run_now = True
                continue

        session.commit()
        session.close()

        _missing_crawlers = list(filter(
            lambda x: x not in _exist_crawlers, BACKEND_CRAWLERS))
        logger.info(
            f"Backend crawler, missing crawlers: {', '.join(_missing_crawlers)}")
        for missing_crawler in _missing_crawlers:
            session = Session()
            _backend_crawler = BackendCrawler(
                source_name=missing_crawler,
                is_runing=False,
                is_run_now=False,
                temp_data={}
            )

            session.add(_backend_crawler)
            session.commit()
            session.close()
    except Exception as e:
        logger.info(
            f"Backend crawler, [ERROR]: {e}")
    finally:
        time.sleep(600)
