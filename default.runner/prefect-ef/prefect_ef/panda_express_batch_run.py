from prefect import flow, task, get_run_logger
from prefect.blocks.system import String
from prefect.deployments import run_deployment
from prefect_ef.slack import notify_slack
from typing import Coroutine, List, Sequence

import asyncio
import json

environment = String.load("environment").value
deployment_name = "panda-express-transfer/panda_express_manual_run"


# Note: this async flow can be invoked directly from regular sync flows
@flow
async def panda_express_batch_run_flow(config_files: list, process_name: str = "pe"):

    logger = get_run_logger()

    # Initial config steps
    config_list = await get_pe_config_list(config_files)
    deployment_batch = await build_deployment_batch(config_list, process_name)

    # Submit deployment flows to run in parallel
    flow_run_results = await submit_deployment_batch_run(deployment_batch)

    # Repeat this process for any sub-flows that were not successful
    retries = 1
    while (
        len(flow_runs_to_retry := get_flow_runs_to_retry(flow_run_results)) > 0
        and retries <= 2
    ):
        logger.warning(
            "The following deployment runs failed and will be retried: "
            f"{', '.join([f['flow_run_name'] for f in flow_runs_to_retry])}"
        )

        flow_run_results = await submit_deployment_batch_run(flow_runs_to_retry)

        retries += 1

    # Notify slack if any panda-express runs are still not successful
    if len(flow_runs_to_retry := get_flow_runs_to_retry(flow_run_results)) > 0:
        flow_run_names = "\n".join([f["flow_run_name"] for f in flow_runs_to_retry])
        logger.error(
            f"Max retries reached. Some panda-express runs are still not successful: {flow_run_names}"
        )
        notify_slack(
            slack_webhook_block="data-prefect-alert",
            subject=":alert: Panda Express alert",
            message=f"Max retries reached. Some panda-express runs are still not successful.\n```{flow_run_names}```",
        )


@task(description="Further transform panda-express inputs into deployment format")
async def build_deployment_batch(config_list: list, process_name: str) -> list:
    return [
        {
            "name": deployment_name,
            "flow_run_name": f"{process_name}_{x}_{c['target_table']}",
            "parameters": normalize_config(c),
        }
        for x, c in enumerate(config_list, start=1)
    ]


@task(description="Get panda-express configuration as a dictionary")
async def get_pe_config_list(config_files: list) -> list:
    logger = get_run_logger()

    logger.info(f"{config_files=}")

    config_list = get_pe_config(config_files=config_files, environment=environment)
    logger.info(f"Found {len(config_list)} config items.")

    return config_list


# Normalize the config dictionary to match the expected format of the panda-express flow
def normalize_config(config: dict) -> dict:

    # keys to move to kwargs dictionary
    kwargs_keys = (
        "source_schema",
        "snowflake_role",
        "snowflake_warehouse",
        "mssql_username",
    )

    # create kwargs dictionary
    kwargs_dict = {k: v for k, v in config.items() if k in kwargs_keys}

    # remove kwargs keys from config dictionary
    for k in kwargs_keys:
        config.pop(k, None)

    # add kwargs to config
    config["kwargs"] = kwargs_dict

    return config


# get_pe_config() accepts a list of panda-express config files and an environment.
#   It returns a list of dictionaries that represent the panda-express input parameters.
def get_pe_config(
    config_files: list,
    config_dir: str = "panda_express_config",
    environment: str = None,
) -> list:

    config_list = list()

    # config_reldir = f"{os.path.dirname(__file__)}/{config_dir}"
    config_reldir = f"dp_data_platform/{config_dir}"

    #  Loop through the config files and load the json into a list
    for config_file in config_files:
        # Opening config file
        with open(f"{config_reldir}/{config_file}", "r") as file:
            config_list.append(file.read())

    # create valid JSON string from config files data
    config_str = f"[{','.join(config_list)}]"

    if environment is not None:
        env_prefix = "" if environment == "prod" else "qa_"
        config_str = config_str.replace("${env_prefix}", env_prefix)

    # parse JSON string as list of dictionaries
    return json.loads(config_str)


def get_flow_runs_to_retry(flow_run_results: List[Coroutine]) -> list:
    return [
        {
            "name": deployment_name,
            "flow_run_name": f.name,
            "parameters": f.parameters,
        }
        for f in flow_run_results
        if not f.state.is_completed()
    ]


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


async def submit_deployment_batch_run(
    deployment_batch: list, concurrency: int = 16
) -> List[Coroutine]:
    """Accepts a deployment batch (a list of deployment configuration dictionaries)
    and runs deployment flows in parallel according to the concurrency parameter.
    Uses asyncio to async initiate the flow, and await+gather to block until the
    deployment flows return with a final state.
    """
    return await asyncio.gather(
        *_limit_concurrency(
            [run_deployment(**deployment) for deployment in deployment_batch],
            concurrency=concurrency,
        )
    )
