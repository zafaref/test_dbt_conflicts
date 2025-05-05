import asyncio
import importlib
import os
import random
import time
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.blocks.system import String
from prefect.deployments import run_deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_github.repository import GitHubRepository

GITHUB_BLOCK_NAME = "de-prefect"


def deploy_flow(
        flow: flow,
        name: str,
        description: str = None,
        tags: list = [],
        parameters: dict = {},
        timezone: str = "America/New_York",
        dev_cron_schedule: str = None,
        prod_cron_schedule: str = None,
        work_pool_name: str = "virginia-worker-pool",
        work_queue_name: str = "default",
        infra_overrides: dict = {},
        entrypoint: str = None,
        alert_on_failure: bool = True,
        asynchronous: bool = False,
):
    # If run outside of github CI, just execute the flow
    if not os.environ.get("CI"):
        if asynchronous:
            asyncio.run(flow(**parameters))
        else:
            flow(**parameters)
        return

    # Set cron schedule according to environment
    environment = String.load("environment").value
    if environment == "dev":
        cron_schedule = dev_cron_schedule
    elif environment == "prod":
        cron_schedule = prod_cron_schedule

    # Cron schedules default to US east coast time
    if cron_schedule is not None:
        schedules = [{"schedule": CronSchedule(cron=cron_schedule, timezone=timezone)}]
    else:
        schedules = None

    # Add alert_on_failure tag if parameter is set
    if alert_on_failure and "alert_on_failure" not in tags:
        tags.append("alert_on_failure")

    if not entrypoint:
        ## first see if an entrypoint can be determined
        flow_file = getattr(flow, "__globals__", {}).get("__file__")
        mod_name = getattr(flow, "__module__", None)
        if not flow_file:
            if not mod_name:
                # todo, check if the file location was manually set already
                raise ValueError("Could not determine flow's file location.")
            module = importlib.import_module(mod_name)
            flow_file = getattr(module, "__file__", None)
            if not flow_file:
                raise ValueError("Could not determine flow's file location.")
        # set entrypoint
        entry_path = Path(flow_file).absolute().relative_to(Path(".").absolute())
        entrypoint = f"{entry_path}:{flow.fn.__name__}"

    print(f"Entrypoint = {entrypoint}")

    deployment_id = flow.from_source(
        source=GitHubRepository.load(GITHUB_BLOCK_NAME),
        entrypoint=entrypoint,
    ).deploy(
        name=name,
        description=description,
        tags=tags,
        schedules=schedules,
        parameters=parameters,
        work_pool_name=work_pool_name,
        work_queue_name=work_queue_name,
        job_variables=infra_overrides,
    )

    print(f"Deployment id = {deployment_id}")


@task
def run_flow_deployment(
        deployment_name: str,
        process_name: str = None,
        timeout: float = None,
        parameters: dict = None,
):
    logger = get_run_logger()
    logger.info(f"Starting deployment {deployment_name} ...")

    # Retry if the run crashes
    for i in range(3):
        result = run_deployment(
            name=deployment_name,
            flow_run_name=process_name,
            timeout=timeout,
            parameters=parameters,
        )
        if result.state.is_crashed():
            logger.warning(f"Retrying crashed deployment {result}")
            time.sleep(random.randint(1, 10))
        else:
            break

    if result.state.is_failed() or result.state.is_crashed():
        raise Exception(f"Subflow run failed: {result}")
    else:
        logger.info(f"{deployment_name} run result: {result}")

    return result
