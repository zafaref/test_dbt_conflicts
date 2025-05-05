import os
from typing import Any, List, Optional

import pandas as pd
import sqlalchemy as sa
from prefect import task, get_run_logger
from prefect.blocks.system import Secret, String
from prefect_ef.utilities import get_prefect_user_email
from snowflake.connector.pandas_tools import make_pd_writer
from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy.custom_types import SnowflakeType

ENV_PREFIX = "QA_" if String.load("environment").value == "dev" else ""


def get_snowflake_engine(**kwargs) -> sa.engine:
    # running in local dev env
    if not os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"):
        kwargs["user"] = get_prefect_user_email()
        kwargs.pop("password") if kwargs.get("password") else ""
        kwargs["authenticator"] = "externalbrowser"
    # running in aws environment
    else:
        kwargs["password"] = Secret.load("snowflake-prefect-password").get()
        kwargs["user"] = f"{ENV_PREFIX}PREFECT"

    kwargs = {k: v for k, v in kwargs.items() if v != ""}
    return sa.create_engine(url=URL(**kwargs))


@task
def upload_dataframe_to_snowflake(
        df: pd.DataFrame,
        account: str = "eftours_us.us-east-1",
        database: str = f"{ENV_PREFIX}SRC_RAWSTORE",
        schema: str = "",
        table_name: str = "",
        role: str = "",
        if_exists: str = "replace",
        dtype: Optional[dict | SnowflakeType] = None,
):
    """Upload a dataframe to Snowflake.

    Keyword arguments:
    df -- the dataframe to upload
    account -- the account to upload the dataframe to
    database -- the database to upload the dataframe to
    schema -- the schema within the database to upload the dataframe to
    table_name -- the table name to upload the dataframe to
    role -- the role to upload the dataframe under
    if_exists -- the behavior to use if the table already exists. Options are 'fail', 'replace', 'append', 'truncate', 'drop'.
    dtype -- columns types
    Returns:
    The number of records written to the table.
    """

    logger = get_run_logger()
    logger.info(f"Connecting to {account}.{database}...")

    if df.empty:
        logger.info("The df is empty - not loading anything")
        return 0

    # Connect to Snowflake via sqlalchemy + pyodbc
    engine = get_snowflake_engine(
        account=account,
        database=database,
        schema=schema,
        role=role,
        table_name=table_name,
    )

    # Convert column names to uppercase to prevent quoting
    df.columns = df.columns.str.upper()

    # Transfer the dataframe to Snowflake
    df.to_sql(
        schema=schema,
        name=table_name,
        con=engine,
        index=False,
        method=make_pd_writer(quote_identifiers=False),
        if_exists=if_exists,
        chunksize=None,
        dtype=dtype,
    )
    record_count = df.shape[0]
    logger.info(
        f"Uploaded {record_count} records from dataframe to {schema}.{table_name}"
    )

    # Close connection and return records written
    engine.dispose()
    return record_count


@task
def snowflake_execute(
        account: str = "eftours_us.us-east-1",
        database: str = f"{ENV_PREFIX}SRC_RAWSTORE",
        query: str = "",
        schema: str = "",
        role: str = "",
        warehouse: str = "",
        return_column_names: bool = False,
) -> List[tuple[Any]] | List[dict[str, Any]]:
    """Execute a query in Snowflake and return the result.

    Note that SnowFlake will use the PREFECT user defaults for database and warehouse
    arguments when not provided.

    Keyword arguments:
    account -- the account to execute the query under
    database -- the database to execute the query against
    query   -- the query to execute
    schema -- Optional. the schema within the database to execute the query against.
    role -- Optional. the role to execute the query under. Defaults to `None`.
    warehouse -- Optional. the warehouse to execute the query under. Defaults to `None`.
    return_column_names -- Optional. whether to return the column names in the result. Defaults to `False`.

    Returns:
    A list of tuples representing the result of the query. If `return_column_names` is `True`,
    the result will be a list of dictionaries.
    """
    logger = get_run_logger()
    logger.info(f"Connecting to {account}.{database}...")

    # Connect to Snowflake via sqlalchemy + pyodbc
    engine = get_snowflake_engine(
        account=account,
        database=database,
        query=query,
        schema=schema,
        role=role,
        warehouse=warehouse,
        autocommit=True,
    )

    # Execute the query
    with engine.connect() as conn:
        with conn.connection.cursor() as cursor:
            logger.info(f"Executing query: {query[:100]}...")
            rows = cursor.execute(query).fetchall()
            column_names = [col[0] for col in cursor.description]
            logger.info(f"Received result: {rows}"[:100])
            logger.info(f"Received {len(rows)} rows")

    if return_column_names:
        return [dict(zip(column_names, row)) for row in rows]
    else:
        return rows


@task
def snowflake_execute_many(
        account: str = "eftours_us.us-east-1",
        database: str = f"{ENV_PREFIX}SRC_RAWSTORE",
        queries: str = [],
        schema: str = "",
        role: str = "",
        warehouse: str = "",
        raise_errors: bool = False,
) -> dict:
    """Execute a list of queries in Snowflake and return the results.

    Note that SnowFlake will use the PREFECT user defaults for database and warehouse
    arguments when not provided.

    Keyword arguments:
    account -- the account to execute the queries under
    database -- the database to execute the queries against
    queries -- the list of queries to execute
    schema -- Optional. the schema within the database to execute the queries against.
    role -- Optional. the role to execute the queries under. Defaults to `None`.
    warehouse -- Optional. the warehouse to execute the queries under. Defaults to `None`.
    raise_errors -- Optional. whether to raise an exception if an error occurs. Defaults to `False`.

    Returns:
    A list of dictionaries representing the results of the queries.
    """

    logger = get_run_logger()
    logger.info(f"Connecting to {account}.{database}...")
    results = []
    QUERY_LOGGING_THRESHOLD = 100

    # Connect to Snowflake via sqlalchemy + pyodbc
    engine = get_snowflake_engine(
        account=account,
        database=database,
        schema=schema,
        role=role,
        warehouse=warehouse,
    )

    count = len(queries)
    logger.info(f"Executing {count} queries...")

    if count > QUERY_LOGGING_THRESHOLD:
        logger.info(
            f"Note: Because the query count is above {QUERY_LOGGING_THRESHOLD}, "
            "executed sql statements will only be available in the DEBUG logs."
        )

    with engine.connect() as conn:
        for i, query in enumerate(queries, start=1):
            if count > QUERY_LOGGING_THRESHOLD:
                logger.debug(f"[{i}/{count}] Executing query: {query[:100]}...")
            else:
                logger.info(f"[{i}/{count}] Executing query: {query[:100]}...")
            try:
                conn.execute(query)
                results.append({"query": query, "result": "OK"})
            except Exception as e:
                logger.warning(f"Error executing query: {e}")
                results.append({"query": query, "result": e})
                if raise_errors:
                    raise e

    return results


@task
def snowflake_execute_to_csv(
        account: str,
        database: str,
        query: str,
        csv_path: str,
        schema: str = "",
        role: str = "",
        warehouse: str = "",
) -> None:
    logger = get_run_logger()
    logger.info(f"Connecting to {account}.{database}...")

    # Connect to Snowflake via sqlalchemy + pyodbc
    engine = get_snowflake_engine(
        account=account,
        database=database,
        schema=schema,
        role=role,
        warehouse=warehouse,
    )

    # Connect to Snowflake, execute the query, and save result to csv file
    engine.connect().connection.cursor().execute(query).fetch_pandas_all().to_csv(
        index=False, path_or_buf=csv_path
    )
    logger.info(
        f"Query '{query}' has been executed and result has been saved into '{csv_path}'."
    )

    return None
