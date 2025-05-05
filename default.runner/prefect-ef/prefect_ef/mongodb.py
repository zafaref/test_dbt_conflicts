from pymongo import MongoClient
from prefect import task, get_run_logger


@task
def mongodb_execute(uri, database, collection, query) -> list:
    log = get_run_logger()

    # Connect to pymongo
    mongo_client = MongoClient(uri)
    database = mongo_client[database]
    collection = database[collection]
    log.info(f"Connected to mongo cluster: {mongo_client.host}")

    try:
        log.info(f"Running query: {query}")
        return list(collection.find(query))
    except Exception as e:
        raise e
    finally:
        mongo_client.close()
