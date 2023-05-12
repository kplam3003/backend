from sqlalchemy import create_engine, Float, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime

import config
import logger
logger = logger.init_logger(config.LOGGER_NAME, config.LOGGER)


CASE_STUDY_STATUS_APPROVED = "APPROVED"


class BaseModelMixin():

    def set_jsonb_attr(self, attr_name, value):
        if not hasattr(self, attr_name):
            raise
        setattr(self, attr_name, value)
        flag_modified(self, attr_name)


Base = declarative_base(cls=BaseModelMixin)
Base.metadata.schema = 'public'


class Task(Base):
    __tablename__ = 'leo_worker_task'

    id = Column(Integer, primary_key=True)
    key = Column(String(255))
    status = Column(String(10))
    params = Column(JSONB)
    data = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    def to_json(self):
        return {
            'id': self.id,
            'key': self.key,
            'status': self.status,
            'params': self.params,
            'data': self.data,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class CaseStudy(Base):
    __tablename__ = 'leo_case_study'

    def case_study_crawl_progress_default(self):
        return {
            "progress": 0,
            "details": {
                "crawl": 0,
                "preprocess": 0,
                "translate": 0,
                "nlp": 0,
                "load": 0,
                "export": 0,
                "keyword_extract": 0,
                "word_frequency": 0
            }
        }

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    crawl_progress = Column(JSONB)
    crawl_error_reason = Column(JSONB)
    is_active = Column(Boolean)
    status = Column(String(10))
    crawl_on_approval = Column(Boolean)
    dimension_config_id = Column(Integer)
    nlp_type_latest_review_date = Column(DateTime, nullable=True)
    nlp_pack_id = Column(Integer)
    schedule_id = Column(Integer, ForeignKey("leo_case_study_schedule.id"))
    is_tracking = Column(Boolean)

    def to_json(self):
        return {
            "id": self.id,
            "name": self.name,
            "progress": self.crawl_progress,
            "crawl_error_reason": self.crawl_error_reason,
            "status": self.status,
            "crawl_on_approval": self.crawl_on_approval
        }


class CaseStudySchedule(Base):
    __tablename__ = 'leo_case_study_schedule'

    id = Column(Integer, primary_key=True)
    is_dataplatform_sync = Column(Boolean, default=False)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    type = Column(String(10))
    is_active = Column(Boolean, default=True)
    create_time = Column(DateTime, default=datetime.utcnow)
    update_time = Column(DateTime, default=datetime.utcnow,
                         onupdate=datetime.utcnow)
    payload = Column(JSONB)

    def to_json(self):
        return {
            "id": self.id,
            "is_dataplatform_sync": self.is_dataplatform_sync,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "type": self.type,
            "create_time": self.create_time.isoformat(),
            "update_time": self.update_time.isoformat(),
            "payload": self.payload
        }


class CaseStudyChartFilter(Base):
    __tablename__ = "leo_case_study_chart_filter"

    id = Column(Integer, primary_key=True)
    case_study_id = Column(Integer)
    chart_filter = Column(JSONB, default={})
    create_time = Column(DateTime, default=datetime.utcnow)
    update_time = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    is_active = Column(Boolean)


class CaseStudyCompany(Base):
    __tablename__ = "leo_case_study_company"

    id = Column(Integer, primary_key=True)
    case_study_id = Column(Integer)
    company_name = Column(String(255))


class CaseStudyCompanyDataSource(Base):
    __tablename__ = "leo_case_study_company_data_source"

    id = Column(Integer, primary_key=True)
    case_study_id = Column(Integer)
    data_source_id = Column(Integer, ForeignKey("leo_company_data_source.id"))
    data_source_name = Column(String(255))
    url = Column(JSONB)
    extra_data = Column(JSONB)


class CaseStudyDimensionStatistic(Base):
    __tablename__ = "leo_case_study_dimension_statistic"

    id = Column(Integer, primary_key=True)
    case_study_id = Column(Integer)
    dimension_label_id = Column(Integer)
    dimension_config_id = Column(Integer)
    count = Column(Integer)
    dimension_total_count = Column(Integer)
    is_active = Column(Boolean, default=True)
    create_time = Column(DateTime, default=datetime.utcnow)
    update_time = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class CaseStudyDimensionStatisticV110(Base):
    __tablename__ = "leo_case_study_dimension_statistic_v110"

    id = Column(Integer, primary_key=True)
    case_study_id = Column(Integer)
    dimension_config_info_v110_id = Column(Integer)
    count = Column(Integer)
    is_active = Column(Boolean, default=True)
    create_time = Column(DateTime, default=datetime.utcnow)
    update_time = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    statistic = Column(JSONB, default=[])


# Deprecated in next version v1.1.1
class DimensionConfigVersion(Base):
    __tablename__ = "leo_dimension_config_version"

    id = Column(Integer, primary_key=True)
    dimension_config_id = Column(Integer)
    created_at = Column(DateTime)


class DimensionConfig(Base):
    __tablename__ = "leo_dimension_config"

    id = Column(Integer, primary_key=True)


class DimensionConfigInfoV110(Base):
    __tablename__ = "leo_dimension_config_info_v110"

    id = Column(Integer, primary_key=True)
    dimension_config_id = Column(Integer)
    dimension_type_id = Column(Integer)
    dimension = Column(String)
    modified_dimension = Column(String)
    label = Column(String)
    modified_label = Column(String)
    is_use_for_analysis = Column(Boolean, default=True)


class DimensionConfigInfo(Base):
    __tablename__ = "leo_dimension_config_info"

    id = Column(Integer, primary_key=True)
    dimension_id = Column(Integer)
    dimension_config_id = Column(Integer)


class Dimension(Base):
    __tablename__ = "leo_dimension"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    modified_name = Column(String)


class DimensionLabel(Base):
    __tablename__ = "leo_dimension_label"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    modified_name = Column(String)
    dimension_id = Column(Integer)


class NLPType(Base):
    __tablename__ = "leo_nlp_type"

    id = Column(Integer, primary_key=True)
    name = Column(String(255))


class NLPPack(Base):
    __tablename__ = "leo_nlp_pack"

    id = Column(Integer, primary_key=True)
    nlp_type_id = Column(Integer)
    name = Column(String(255))


class Chart(Base):
    __tablename__ = "leo_chart"

    id = Column(Integer, primary_key=True)
    code_name = Column(String(255))
    name = Column(String(255))
    nlp_type_id = Column(Integer)
    is_active = Column(Boolean)


class Company(Base):
    __tablename__ = "leo_company"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class CompanyDataSource(Base):
    __tablename__ = "leo_company_data_source"

    URL_TYPE_MAP = {
        "VOE_LEONARDO_COMPANY_INFO": "overview",
        "VOE_LEONARDO_JOB_LIST": "job",
        "VOE_LEONARDO_EMPLOYEE_REVIEWS": "review",
        "VOC_LEONARDO": "review"
    }

    URL_TYPE_MAP_2 = {
        "overview": "VOE_LEONARDO_COMPANY_INFO",
        "job": "VOE_LEONARDO_JOB_LIST",
        "review": "VOE_LEONARDO_EMPLOYEE_REVIEWS",
        "review_stats": "VOE_LEONARDO_EMPLOYEE_REVIEWS",
    }

    URL_TYPE_MAP_3 = {
        "VOE_LEONARDO_COMPANY_INFO": "company_info",
        "VOE_LEONARDO_JOB_LIST": "job_list",
        "VOE_LEONARDO_EMPLOYEE_REVIEWS": "review",
        "VOC_LEONARDO": "review"
    }

    def company_data_source_crawl_progress_default(self):
        return {
            "job": {
                "progress": 0,
                "details": {
                    "crawl": 0,
                    "load": 0,
                    "translate": 0,
                    "preprocess": 0,
                },
                "status": None,
            },
            "review": {
                "progress": 0,
                "details": {
                    "crawl": 0,
                    "load": 0,
                    "translate": 0,
                    "preprocess": 0,
                },
                "status": None,
            },
            "review_stats": {
                "progress": 0,
                "details": {
                    "crawl": 0,
                    "load": 0,
                    "translate": 0,
                    "preprocess": 0,
                },
                "status": None,
            },
            "coresignal_employees": {
                "progress": 0,
                "details": {
                    "crawl": 0,
                    "load": 0,
                    "translate": 0,
                    "preprocess": 0,
                },
                "status": None,
            },
            "coresignal_stats": {
                "progress": 0,
                "details": {
                    "crawl": 0,
                    "load": 0,
                    "translate": 0,
                    "preprocess": 0,
                },
                "status": None,
            },
        }

    STATUS_FAILED = "FAILED"
    STATUS_IN_PROGRESS = "IN_PROGRESS"
    STATUS_COMPLETED = "COMPLETED"

    id = Column(Integer, primary_key=True)
    is_active = Column(Boolean)
    is_valid = Column(Boolean, default=True)
    is_data = Column(Boolean, default=True)
    status = Column(String(255))
    crawl_progress = Column(JSONB)
    url = Column(JSONB)
    statistic = Column(JSONB)
    validate_progress = Column(Float, nullable=True, default=None)
    data_source_id = Column(Integer, ForeignKey("leo_data_source.id"))
    company_id = Column(Integer, ForeignKey("leo_company.id"))


class DataSource(Base):
    __tablename__ = "leo_data_source"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String(255))
    nlp_type_id = Column(Integer, ForeignKey("leo_nlp_type.id"))


class HealthCheckConfig(Base):
    __tablename__ = "leo_health_check_config"

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("leo_company.id"))
    date_of_week = Column(Integer)
    number_of_weeks_to_run = Column(Integer)
    email_list = Column(JSONB)
    is_run_now = Column(Boolean)
    is_active = Column(Boolean)
    create_time = Column(DateTime, default=datetime.utcnow)
    is_running = Column(Boolean)
    term_data = Column(JSONB)

    def to_json(self):
        return {
            'id': self.id,
            'company_id': self.company_id,
            'date_of_week': self.date_of_week,
            'number_of_weeks_to_run': self.number_of_weeks_to_run,
            'email_list': self.email_list,
            'is_run_now': self.is_run_now,
            'is_active': self.is_active,
            'create_time': self.create_time.strftime("%d/%m/%Y"),
            'is_running': self.is_running,
            'term_data': self.term_data
        }


class HealthCheckReport(Base):
    __tablename__ = "leo_health_check_report"

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("leo_company.id"))
    number_of_company_data_source_total = Column(Integer)
    number_of_company_data_source_passed = Column(Integer)
    number_of_company_data_source_failed = Column(Integer)
    data = Column(JSONB)
    csv_data = Column(JSONB)
    create_time = Column(DateTime, default=datetime.utcnow)
    update_time = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    is_active = Column(Boolean, default=True)


class BackendCrawler(Base):
    __tablename__ = "leo_backend_crawler"

    id = Column(Integer, primary_key=True)
    source_name = Column(String)
    is_runing = is_active = Column(Boolean)
    is_run_now = is_active = Column(Boolean)
    temp_data = Column(JSONB)
    is_active = Column(Boolean, default=True)
    create_time = Column(DateTime, default=datetime.utcnow)
    update_time = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    def to_json(self):
        return {
            'id': self.id,
            'source_name': self.source_name,
            'is_runing': self.is_runing,
            'is_run_now': self.is_run_now,
            'temp_data': self.temp_data
        }



engine = create_engine(
    config.DATABASE_URI, echo=False, pool_size=5,
    pool_recycle=60, connect_args={"options": "-c timezone=utc"}
)
Session = scoped_session(sessionmaker(bind=engine))


def auto_session(func):
    def wrapper(*args, session=None, **kwargs):
        if session is None:
            session = Session()

        try:
            result = func(*args, **kwargs, session=session)
            return result
        except:
            session.rollback()
            raise
        finally:
            session.close()

    return wrapper
