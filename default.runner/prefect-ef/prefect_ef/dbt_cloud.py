import json
import time
import requests
from prefect import task, get_run_logger
from prefect.blocks.system import Secret, String
from prefect.runtime import flow_run
from typing import Union

DBT_ACCOUNT_ID = String.load("dbt-cloud-account").value
BASE_URL = f"https://emea.dbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}"
BASE_URL_V3 = f"https://emea.dbt.com/api/v3/accounts/{DBT_ACCOUNT_ID}"

environment = String.load("environment").value


def _get_authorization_header() -> dict:
    api_token = Secret.load("dbt-cloud-api-token").get()
    header = {"Authorization": f"Token {api_token}"}
    return header


def _get_request_body(
    git_branch: str = None,
    target_name_override: str = None,
    steps_override: list[str] = None,
) -> dict:
    body = {"cause": f"Triggered from Prefect {environment}: {flow_run.ui_url}"}
    if git_branch:
        body["git_branch"] = git_branch

    if target_name_override:
        body["target_name_override"] = target_name_override

    if steps_override:
        body["steps_override"] = steps_override

    return body


def _get_job_run_metadata(response) -> dict:
    return {
        "run_id": response["data"]["id"],
        "job_definition_id": response["data"]["job_definition_id"],
        "project_id": response["data"]["project_id"],
        "environment_id": response["data"]["environment_id"],
        "job_name": response["data"]["job"]["name"],
        "execute_steps": response["data"]["job"]["execute_steps"],
        "settings": response["data"]["job"]["settings"],
        "href": response["data"]["href"],
        "trigger": response["data"]["trigger"],
    }


def _get_run_results(run_id: int) -> dict:
    headers = _get_authorization_header()

    response = requests.get(
        BASE_URL + f"/runs/{run_id}/artifacts/run_results.json", headers=headers
    )
    run_results = response.json()

    dbt_run_args = {
        "elapsed_time": run_results.get("elapsed_time"),
        "target": run_results.get("args").get("target"),
        "invocation_command": run_results.get("args").get("invocation_command"),
    }

    output = []

    for result in run_results.get("results"):
        # print(result)
        status = result.get("status")
        relation_name = (
            result.get("relation_name")
            if result.get("relation_name")
            else result.get("unique_id")
        )
        execution_time = result.get("execution_time")
        message = result.get("message") if result.get("message") else status.upper()

        timing = [t for t in result.get("timing") if t.get("name") != "compile"][0]
        completed_at = timing.get("completed_at")

        output.append(
            {
                "status": status,
                "relation_name": relation_name,
                "execution_time": execution_time,
                "message": message,
                "completed_at": completed_at,
            }
        )

    # sort results by completed_at
    output.sort(key=lambda x: x["completed_at"])

    return dbt_run_args, output


def _log_run_results(run_id: int, logger) -> None:
    try:
        dbt_run_args, run_results = _get_run_results(run_id)
        logger.info(f"DBT run args: {dbt_run_args}")
        total = len(run_results)
        for i, r in enumerate(run_results, 1):
            if r["status"] in ("success", "pass"):
                logger.info(
                    f"{r['completed_at'][11:22]} {i} of {total} {r['status'].upper()} {r['relation_name']} ... [{r['message']} in {round(r['execution_time'], 2)}s]"
                )
            else:
                logger.error(
                    f"{r['completed_at'][11:22]} {i} of {total} {r['status'].upper()} {r['relation_name']} ... [{r['status'].upper()} in {round(r['execution_time'], 2)}s]\n{r['message']}"
                )

        # Count entries for each status
        if total > 0:
            status_count = [
                f"{status.upper()}={len([r for r in run_results if r['status'] == status])}"
                for status in ["success", "pass", "error", "fail", "warn", "skipped"]
            ]
            logger.info(" ".join(status_count) + f" TOTAL={total}")

    except Exception as e:
        logger.warning(f"Error getting DBT run results: {e}")


def _get_project_id(project_name: str) -> int:
    logger = get_run_logger()
    headers = _get_authorization_header()
    body = {"name__icontains": project_name}

    response = requests.get(BASE_URL_V3 + f"/projects/", headers=headers, params=body)
    response.raise_for_status()
    response = response.json()

    logger.debug("{response=}")

    # find project id in response path data -> id, raise exception if not found
    try:
        project_id = [t for t in response["data"] if t["name"] == project_name][0]["id"]
        logger.info(f"Found project_id {project_id} for project '{project_name}'")
    except Exception as e:
        logger.error(e)
        raise Exception(f"Project '{project_name}' not found")

    return project_id


def _get_environment_id(project_id: int, environment_name: str) -> int:
    logger = get_run_logger()
    headers = _get_authorization_header()
    params = {"project_id": project_id, "name__icontains": environment_name}

    response = requests.get(
        BASE_URL_V3 + f"/environments/", headers=headers, params=params
    )
    response.raise_for_status()
    response = response.json()

    logger.debug("{response=}")

    # find environment id in response path data -> id, raise exception if not found
    try:
        environment_id = [t for t in response["data"] if t["name"] == environment_name][
            0
        ]["id"]
        logger.info(
            f"Found environment_id {environment_id} for environment '{environment_name}'"
        )
    except Exception as e:
        logger.error(e)
        raise Exception(f"Environment '{environment_name}' not found")

    return environment_id


def _get_job_id(project_name: str, environment_name: str, job_name: str) -> int:
    logger = get_run_logger()
    project_id = _get_project_id(project_name)
    environment_id = _get_environment_id(project_id, environment_name)

    headers = _get_authorization_header()
    params = {
        "project_id": project_id,
        "environment_id": environment_id,
        "name__icontains": job_name,
    }
    response = requests.get(BASE_URL + f"/jobs/", headers=headers, params=params)
    response.raise_for_status()
    response = response.json()

    try:
        job_id = [t for t in response["data"] if t["name"] == job_name][0]["id"]
        logger.info(
            f"Found job_id {job_id} for job '{job_name}' in project '{project_name}' and environment '{environment_name}'"
        )
    except Exception as e:
        logger.error(e)
        raise Exception(
            f"Job '{job_name}' not found in project '{project_name}' and environment '{environment_name}'"
        )

    return job_id


@task
def dbt_cloud_run_job(
    job_id: int = None,
    project_name: str = None,
    environment_name: str = None,
    job_name: str = None,
    git_branch: str = None,
    target_name_override: str = None,
    steps_override: list[str] = None,
    wait_for_completion: bool = True,
    poll_frequency_seconds: int = 10,
    raise_exception_on_failure: bool = False,
) -> int:
    """
    Run a DBT Cloud job.

    Args:
        job_id (int, optional): The DBT Cloud job ID.
        project_name (str, optional): The project name. Defaults to None.
        environment_name (str, optional): The environment name. Defaults to None.
        job_name (str, optional): The job name. Defaults to None.
        git_branch (str, optional): The git branch to checkout before running. Defaults to None.
        target_name_override (str, optional): Overrides the `target.name` context variable. Defaults to None.
        steps_override (list[str], optional): Overrides the list of dbt commands. Defaults to None.
        wait_for_completion (bool, optional): Whether to wait for the job to complete. Defaults to True.
        poll_frequency_seconds (int, optional): The frequency to poll the job status. Defaults to 10.
        raise_exception_on_failure (bool, optional): Defines if an exception will be raised when a run fails. Defaults to False

    Returns:
        int: The job run ID.

    Raises:
        Exception: If the job run fails.
    """
    logger = get_run_logger()
    logger.info(
        f"Task parameters: {job_id=}, {project_name=}, {environment_name=}, {job_name=}, {git_branch=}, {target_name_override=}, {steps_override=}, {wait_for_completion=}, {poll_frequency_seconds=}"
    )

    if job_id:
        _job_id = job_id
    else:
        _job_id = _get_job_id(project_name, environment_name, job_name)

    job = DBTCloudJob(
        job_id=_job_id,
        git_branch=git_branch,
        target_name_override=target_name_override,
        steps_override=steps_override,
        wait_for_completion=wait_for_completion,
    )

    return job.run(
        logger=logger,
        poll_frequency_seconds=poll_frequency_seconds,
        log_run_detailed=True,
        raise_exception_on_failure=raise_exception_on_failure,
    )


class DBTCloudJob:

    headers = _get_authorization_header()

    def __init__(
        self,
        job_id: int = None,
        project_name: str = None,
        environment_name: str = None,
        job_name: str = None,
        git_branch: str = None,
        target_name_override: str = None,
        steps_override: list[str] = None,
        wait_for_completion: bool = True,
    ):

        if job_id:
            self.job_id = job_id
        else:
            self.job_id = _get_job_id(project_name, environment_name, job_name)

        self.git_branch = git_branch
        self.target_name_override = target_name_override
        self.steps_override = steps_override
        self.wait_for_completion = wait_for_completion

        self.run_id = None

        self.is_running = False
        self.is_complete = False
        self.is_success = False
        self.run_duration = None
        self.href = None

        self.job_data = None

    @classmethod
    def create_from_command(
        cls,
        project_name: str,
        dbt_commands: str | list[str],
        environment_name: str = None,
        git_branch: str = None,
        target_name_override: str = None,
        wait_for_completion: bool = True,
    ):
        """Creates a dbt Cloud job with a command, but DOES NOT EXECUTE IT. You need to call `dbt_cloud_batch_run_job` or `job_object.run(logger)` later in your code.
        If you want your code to be executed immediately, use the `dbt_cloud_run_command()` function

        This allows you to execute multiple jobs in parallel in different projects, if needed.

        Returns:
            DBTCloudJob: an object containing the job details. Use `dbt_cloud_batch_run_job(job_object)` or `job_object.run(logger)` to execute it
        """

        if environment_name is None:
            environment_name = "PROD" if environment == "prod" else "QA"

        default_job_name = f"[{environment_name}] [PREFECT] Custom Command"

        target_name = "prod" if environment == "prod" else "qa"

        if target_name_override is not None:
            target_name = target_name_override

        if isinstance(dbt_commands, str):
            dbt_commands = [dbt_commands]

        return cls(
            job_id=None,
            project_name=project_name,
            environment_name=environment_name,
            job_name=default_job_name,
            git_branch=git_branch,
            target_name_override=target_name,
            steps_override=dbt_commands,
            wait_for_completion=wait_for_completion,
        )

    def start_run(self, logger) -> int:
        """
        Start running job (in case of parallel runs - internal use only)
        """
        if self.run_id is not None:
            raise Exception(f"Job {self.job_id} is already running")

        headers = DBTCloudJob.headers
        body = _get_request_body(
            self.git_branch, self.target_name_override, self.steps_override
        )

        response = requests.post(
            BASE_URL + f"/jobs/{str(self.job_id)}/run/", headers=headers, json=body
        )
        logger.debug(f"Response: {response.json()}")
        response.raise_for_status()

        job_run_metadata = _get_job_run_metadata(response.json())
        logger.info(f"Job run triggered: {job_run_metadata}")

        self.run_id = job_run_metadata["run_id"]

        return self.run_id

    def get_status(self, logger):

        headers = DBTCloudJob.headers

        if self.run_id is None:
            raise Exception("Tried to get status of a job that has not started yet")

        status = requests.get(BASE_URL + f"/runs/{self.run_id}/", headers=headers)
        status.raise_for_status()
        
        logger.debug(f"Status response: {status.json()}")

        self.job_data = status.json()["data"]

        self.is_running = self.job_data["is_running"]
        self.is_complete = self.job_data["is_complete"]
        self.is_success = self.job_data["is_success"]
        self.run_duration = self.job_data["run_duration"]
        self.href = self.job_data["href"]
        logger.info(
            f"{self.run_duration=}, {self.run_id=}, {self.is_running=} {self.is_complete=}"
        )

        return status

    def run(
        self,
        logger,
        poll_frequency_seconds: int = 10,
        log_run_detailed: bool = False,
        raise_exception_on_failure: bool = False,
    ):
        """
        Starts running a single instance of a DBTCloudJob.

        Args:
            job (DBTCloudJob): dbt Cloud job to run. Must be a DBTCloudJob object.
            poll_frequency_seconds (int, optional): Number of seconds between each status update. Defaults to 10.
            log_run_detailed (bool, optional): If true, will forward all output lines to the logger. Defaults to True. - Notice that you can see the output in dbt cloud directly.
            raise_exception_on_failure (bool, optional): If true, will raise an exception if the job fails. Defaults to False.
        Returns:
            int: returns the run_id for the executed job.
        """

        run_id = self.start_run(logger)

        if self.wait_for_completion is False:
            return run_id

        errors = 0
        while self.is_complete is False:

            # Wait and then poll status
            time.sleep(poll_frequency_seconds)

            # Get job status
            try:
                status = self.get_status(logger)
                errors = 0
            except Exception as e:
                logger.error(f"Error getting job status: {e}")
                errors += 1
                if errors >= 3:
                    logger.error("Too many errors getting job status, aborting")
                    raise e
            
            # Report response and break if no longer in progress
            if not self.is_complete:
                continue
            elif self.is_success:
                if log_run_detailed:
                    _log_run_results(run_id, logger)
                logger.info(f"Job run {run_id} succeeded: {self.href}")
            else:
                if log_run_detailed:
                    _log_run_results(run_id, logger)
                logger.error(f"job run {run_id} failed: {self.href}")
                if raise_exception_on_failure:
                    raise Exception(
                        f"job run {run_id} failed: {self.href}",
                        f"API Response: {json.dumps(self.job_data)}",
                    )

        return run_id


@task
def dbt_cloud_batch_run_job(
    jobs: list[Union[int, DBTCloudJob]],
    poll_frequency_seconds: int = 10,
    log_run_detailed: bool = False,
    raise_exception_on_failure: bool = False,
) -> list[int]:
    """
    Triggers multiple dbt Cloud jobs in parallel, possibly monitoring them as configured.

    Args:
        jobs (list[Union[int, DBTCloudJob]]): List of dbt Cloud jobs to run. Can be an integer (job id) or a DBTCloudJob object, which allow customizing options.
        poll_frequency_seconds (int, optional): Number of seconds between each status update. Defaults to 10.
        log_run_detailed (bool, optional): If true, will forward all output lines to the logger. Defaults to False. - Notice that you can see the output in dbt cloud directly.

    Returns:
        list[int]: returns the run_id for each job executed.
    """

    logger = get_run_logger()

    jobs = [j if isinstance(j, DBTCloudJob) else DBTCloudJob(j) for j in jobs]

    monitored_jobs = []
    run_ids = []

    successes = []
    failures = []
    not_monitored = []

    for job in jobs:

        run_id = job.start_run(logger)
        run_ids.append(run_id)

        if job.wait_for_completion is True:
            monitored_jobs.append(job)
        else:
            not_monitored.append(str(job.job_id))

    while len(monitored_jobs) > 0:

        # Wait and then poll status
        time.sleep(poll_frequency_seconds)

        for job in monitored_jobs:

            run_id = job.run_id
            status = job.get_status(logger)

            # Report response and break if no longer in progress
            if not job.is_complete:
                continue
            elif job.is_success:
                if log_run_detailed:
                    _log_run_results(run_id, logger)
                logger.info(f"Job run {run_id} succeeded: {job.href}")
                successes.append(str(job.job_id))
            else:
                if log_run_detailed:
                    _log_run_results(run_id, logger)
                logger.error(f"job run {run_id} failed: {job.href}")
                failures.append(str(job.job_id))

        monitored_jobs = [j for j in monitored_jobs if j.is_complete is False]

    if len(not_monitored) > 0:
        not_monitored_str = ", ".join(not_monitored)
        logger.warning(f"Not monitored: [{not_monitored_str}]")

    if len(successes) > 0:
        successes_str = ", ".join(successes)
        logger.info(f"Successful jobs: [{successes_str}]")

    if len(failures) > 0:
        failures_str = ", ".join(failures)
        logger.error(f"Failed jobs: [{failures_str}]")

        if raise_exception_on_failure:
            num_failures = len(failures)
            raise Exception(f"{num_failures} job(s) failed: [{failures_str}]")

    return run_ids


@task
def dbt_cloud_run_command(
    project_name: str,
    dbt_commands: str | list[str],
    environment_name: str = None,
    git_branch: str = None,
    target_name_override: str = None,
    wait_for_completion: bool = True,
    poll_frequency_seconds: int = 10,
    log_run_detailed: bool = False,
    raise_exception_on_failure: bool = False,
) -> int:
    """
    Executes a command or a sequence of commands in dbt Cloud

    Returns:
        int: run id
    """

    logger = get_run_logger()

    job_object = DBTCloudJob.create_from_command(
        project_name=project_name,
        dbt_commands=dbt_commands,
        environment_name=environment_name,
        git_branch=git_branch,
        target_name_override=target_name_override,
        wait_for_completion=wait_for_completion,
    )

    return job_object.run(
        logger,
        poll_frequency_seconds=poll_frequency_seconds,
        log_run_detailed=log_run_detailed,
        raise_exception_on_failure=raise_exception_on_failure,
    )
