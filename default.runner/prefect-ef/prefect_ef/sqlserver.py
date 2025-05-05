import urllib
import pandas as pd
import pymssql
import sqlalchemy as sa
from typing import Any, List, Dict, Union

from prefect import task, get_run_logger
from prefect.blocks.system import Secret


@task
def sqlserver_execute(
    server: str,
    database: str,
    query: str,
    user: str = "prefect",
    password: str = None,
) -> List[Dict[str, Any]]:
    """Execute a query in SQL Server and return the result.

    Keyword arguments:
    server -- the server to execute the query under
    database -- the database to execute the query against
    query   -- the query to execute
    user -- Optional. the user to execute the query under. Defaults to `prefect`.
    password -- Optional. the password to execute the query under. Defaults to `None`.

    Returns:
    A list of dictionaries representing the result of the query.
    """

    logger = get_run_logger()
    logger.info(f"Connecting to {server}.{database}...")
    with pymssql.connect(
        server=server,
        database=database,
        user=user,
        password=password or Secret.load("prefect-sqlserver-password").get(),
        autocommit=True,
    ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            logger.info(f"Executing query: {query[:100]}...")
            cursor.execute(query)
            try:
                rows = cursor.fetchall()
                logger.info(f"Received result: {rows}"[:100])
            except pymssql.OperationalError:
                rows = None

    return rows


@task
def upload_dataframe_to_sqlserver(
    df: pd.DataFrame,
    server: str,
    database: str,
    schema: str,
    table_name: str,
    if_exists: str = "replace",
    dtype: dict = None,
):
    logger = get_run_logger()
    logger.info(f"Connecting to {server}.{database}...")

    # Connect to SQLServer via sqlalchemy + pyodbc
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID=prefect;"
        f"PWD={Secret.load('prefect-sqlserver-password').get()};",
    )
    engine = sa.create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True
    )

    # Transfer the dataframe to SQL Server
    df.to_sql(
        schema=schema,
        name=table_name,
        con=engine,
        index=False,
        if_exists=if_exists,
        chunksize=1000,
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
def sqlserver_execute_many(server: str, database: str, queries: list) -> dict:
    logger = get_run_logger()
    logger.info(f"Connecting to {server}.{database}...")
    results = []
    QUERY_LOGGING_THRESHOLD = 100
    with pymssql.connect(
        server=server,
        database=database,
        user="prefect",
        password=Secret.load("prefect-sqlserver-password").get(),
        autocommit=True,
    ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            count = len(queries)
            logger.info(f"Executing {count} queries...")

            if count > QUERY_LOGGING_THRESHOLD:
                logger.info(
                    f"Note: Because the query count is above {QUERY_LOGGING_THRESHOLD}, "
                    "executed sql statements will only be available in the DEBUG logs."
                )

            for i, query in enumerate(queries, 1):
                if count > QUERY_LOGGING_THRESHOLD:
                    logger.debug(f"[{i}/{count}] Executing query: {query[:100]}...")
                else:
                    logger.info(f"[{i}/{count}] Executing query: {query[:100]}...")
                try:
                    cursor.execute(query)
                    results.append({"query": query, "result": "OK"})
                except Exception as e:
                    logger.warning(f"Error executing query {query}: {e}")
                    results.append({"query": query, "result": e})

    return results
