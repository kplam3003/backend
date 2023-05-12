from enum import Enum


class StatusEnum(Enum):
    NEW = "NEW"
    RENEW = "RENEW"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"