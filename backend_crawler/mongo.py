import pymongo

from datetime import datetime
from cerberus import Validator
from config import (
    MONGODB_URI,
    MONGODB_CLIENT_KEY,
    MONGODB_ROOT_CA,
    MONGODB_DB_NAME,
    PITCHBOOK_CRAWL_COLLECTION,
    PITCHBOOK_CRAWL_SCHEMA,
    PITCHBOOK_SOURCE_NAME
)


# init long-live MongoDB connection, auth by X509 self-signed SSL
MONGO_CLIENT = pymongo.MongoClient(
    MONGODB_URI,
    authMechanism="MONGODB-X509",
    directConnection=True,
    tls=True,
    tlsCertificateKeyFile=MONGODB_CLIENT_KEY,
    tlsCAFile=MONGODB_ROOT_CA,
    tlsAllowInvalidHostnames=True
)

DB = MONGO_CLIENT[MONGODB_DB_NAME]

if PITCHBOOK_CRAWL_COLLECTION not in DB.list_collection_names():
    DB.create_collection(PITCHBOOK_CRAWL_COLLECTION)

PITCHBOOK_COLLECTION = DB.get_collection(PITCHBOOK_CRAWL_COLLECTION)

def add_mongo_metadata(schema=None, collection=None, source_name=None):
    def decorator(func):
        def wrapper(item):
            # Validate document using the schema and additional validator arguments
            validator = Validator(schema)
            if not validator.validate(item):
                raise ValueError(validator.errors)

            # Add additional fields
            item['create_date'] = datetime.now()
            item['update_date'] = datetime.now()
            item['is_active'] = True

            # Add data_version
            _info = DB[collection].find_one(
                filter={
                    f"{source_name}_id": item[f"{source_name}_id"],
                },
                sort=[("data_version", pymongo.DESCENDING)],
                projection={
                    "_id": 0,
                    f"{source_name}_id": 1,
                    "data_version": 1
                },
            )

            if _info:
                item["data_version"] = _info["data_version"] + 1
            else:
                item["data_version"] = 1

            # Call the wrapped function with the updated document
            return func(item)

        return wrapper
    return decorator

def get_lastes_data_version(pitchbook_id):
    cur = PITCHBOOK_COLLECTION.find(filter={"pitchbook_id": pitchbook_id}).sort([("data_version", -1)]).limit(1)
    return cur[0]

def get_all_unique_pitchbook_ids():
    return PITCHBOOK_COLLECTION.distinct('pitchbook_id')

def get_data():
    data = []
    ls_pitchbook_id = get_all_unique_pitchbook_ids()
    for pitchbook_id in ls_pitchbook_id:
        data.append(get_lastes_data_version(pitchbook_id))
    return data
@add_mongo_metadata(PITCHBOOK_CRAWL_SCHEMA, PITCHBOOK_CRAWL_COLLECTION, PITCHBOOK_SOURCE_NAME)
def add_data_to_pitchbook_crawl_collection(item):
    DB[PITCHBOOK_CRAWL_COLLECTION].insert_one(item)
