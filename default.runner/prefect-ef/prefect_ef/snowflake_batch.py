import os
import asyncio
from typing import List, Coroutine, Sequence, Dict

import sqlalchemy as sa
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret, String
from snowflake.sqlalchemy import URL

ENV_PREFIX = "QA_" if String.load("environment").value == "dev" else ""

async def _get_prefect_user_email():
    from prefect.client.cloud import get_cloud_client

    client = get_cloud_client()
    r = await client.get("/me")
    return r["email"]

async def get_snowflake_engine(**kwargs) -> sa.engine:
    # running in local dev env
    if not os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"):
        kwargs["user"] = await _get_prefect_user_email()
        kwargs.pop("password") if kwargs.get("password") else ""
        kwargs["authenticator"] = "externalbrowser"
    # running in aws environment
    else:
        secret = await Secret.load("snowflake-prefect-password")
        kwargs["password"] = secret.get()
        kwargs["user"] = f"{ENV_PREFIX}PREFECT"

    kwargs = {k: v for k, v in kwargs.items() if v != ""}
    return sa.create_engine(url=URL(**kwargs))

#@task - we can even put this here, but it might make it slower. We need to try
async def execute_query_async(engine: sa.engine.Engine, query: str, raise_errors : bool = True) -> Dict:
    
    loop = asyncio.get_running_loop()
    
    def run_sync():
        try:
            
            with engine.connect() as conn:
                with conn.connection.cursor() as cursor:
                    rows = cursor.execute(query).fetchall()
                    column_names = [col[0] for col in cursor.description]
            
                return {"query": query, "result": [dict(zip(column_names, row)) for row in rows], "error": None}
    
        except Exception as e:
            
            if raise_errors:
                raise e
            else:
                return {"query": query, "result": None, "error": str(e)}
            
            
    return await loop.run_in_executor(None, run_sync)

def _limit_concurrency(
    coroutines: Sequence[Coroutine], concurrency: int
) -> List[Coroutine]:
    """Decorate coroutines to limit concurrency.
    Enforces a limit on the number of coroutines that can run concurrently in higher
    level asyncio-compatible concurrency managers like asyncio.gather(coroutines) and
    asyncio.as_completed(coroutines).
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def with_concurrency_limit(coroutine: Coroutine) -> Coroutine:
        async with semaphore:
            return await coroutine

    return [with_concurrency_limit(coroutine) for coroutine in coroutines]

@task
async def snowflake_execute_concurrent_batch(
    account: str,
    database: str,
    queries: List[str],
    schema: str = "",
    warehouse: str = "",
    role: str = "",
    raise_errors: bool = True,
    parallel_query_max: int = 8,
) -> List[Dict]:
    
    logger = get_run_logger()
    logger.info(f"Connecting to {account}.{database}...")
    
    engine = await get_snowflake_engine(
        account=account, database=database, schema=schema, warehouse=warehouse, role=role
    )
    
    tasks = _limit_concurrency(
        [execute_query_async(engine, query, raise_errors) for query in queries], concurrency=parallel_query_max
    )
    
    results = await asyncio.gather(*tasks)
    logger.info("All queries executed.")
    return results
