import snowflake.connector

from prefect import task, get_run_logger
from prefect_ef.utilities import get_prefect_user_email
from prefect.blocks.system import String, Secret
from time import sleep
from typing import List
import os

ENV_PREFIX = "QA_" if String.load("environment").value == "dev" else ""


def _get_not_completed_queries(queries: List[dict]) -> List[dict]:
    return [q for q in queries if not q["is_completed"]]


@task
def snowflake_execute_async_batch(
    account: str = "eftours_us.us-east-1",
    database: str = f"{ENV_PREFIX}SRC_RAWSTORE",
    queries: List[str] = [],
    schema: str = "",
    warehouse: str = "",
    role: str = "",
    parallel_query_max: int = 8,
    raise_errors: bool = True,
) -> List[dict]:
    logger = get_run_logger()
    logger.info(f"Connecting to {account}.{database}...")

    IS_AWS_ENV = bool(os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"))
    # Remove key word arguments that are empty
    kwargs = {
        "account": account,
        "user": f"{ENV_PREFIX}PREFECT" if IS_AWS_ENV else get_prefect_user_email(),
        "password": Secret.load("snowflake-prefect-password").get() if IS_AWS_ENV else "",
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "role": role,
        "authenticator": "externalbrowser" if not IS_AWS_ENV else ""
    }
    kwargs = {k: v for k, v in kwargs.items() if v != ""}

    # Re-structure query list
    queries = [
        {
            "sql": q,
            "sfqid": None,
            "query_status": None,
            "is_completed": False,
        }
        for q in queries
    ]

    # Connect to Snowflake
    with snowflake.connector.connect(**kwargs) as conn:
        with conn.cursor() as cur:
            # Keep running until all queries are completed
            active_query_count = 0
            while len(_get_not_completed_queries(queries)) > 0:
                # Continuously loop through uncompleted commands
                for query in _get_not_completed_queries(queries):
                    if query["sfqid"] is not None:
                        # If we are assessing a query that has already been submitted,
                        #   get its current status
                        if raise_errors:
                            query[
                                "query_status"
                            ] = conn.get_query_status_throw_if_error(query["sfqid"])
                        else:
                            query["query_status"] = conn.get_query_status(
                                query["sfqid"]
                            )
                        query["is_completed"] = not conn.is_still_running(
                            query["query_status"]
                        )
                        if query["is_completed"]:
                            # If the query is now complete, decrement the counter
                            active_query_count -= 1
                            logger.info(
                                f"Query {query['sfqid']} status: {query['query_status'].name}"
                            )
                    elif active_query_count < parallel_query_max:
                        # Submit query for execution when we are under query count max
                        cur.execute_async(query["sql"])
                        query["sfqid"] = cur.sfqid
                        active_query_count += 1
                        logger.info(
                            f"Query {query['sfqid']} submitted: {query['sql'][:100]}..."
                        )
                    else:
                        # Found a query we need to be submit, but we're already at the
                        #   max query count
                        continue

                    # Short pause between status checks
                    sleep(0.5)

    logger.info("Snowflake connection closed.")

    return queries
