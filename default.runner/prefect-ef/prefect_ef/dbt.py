import logging
import os
import shutil
import stat

from prefect import flow, get_run_logger
from prefect.blocks.system import Secret, String
from prefect.runtime import flow_run
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_ef.slack import notify_slack
from prefect_github.repository import GitHubRepository


"""
Note: this task requires:
    * Pre-existing ssh authentication with github
    * pip install dbt-snowflake
"""
landops_path = "./analytics-land-travel/land/"


def on_rm_error(func, path, exc_info):
    try:
        os.chmod(path, stat.S_IWRITE)
        os.unlink(path)
    except Exception as e:
        logger = get_run_logger()
        logger.error(
            f"On removing {path} with {func} got {exc_info}, on its workaround got {e}"
        )


def delete_repo_folder(repo_name: str):
    repo_folder = os.path.abspath(f"./{repo_name}")
    if os.path.exists(repo_folder):
        shutil.rmtree(repo_folder, onerror=on_rm_error)


def get_slack_channel(dbt_project_dir):
    if dbt_project_dir.startswith(landops_path):
        return "land-ops-analytics-cicd"
    else:
        return "data-snowflake-alert"


def send_slack_message(log_level, msg, dbt_project_dir, command_with_args):
    dbt_command = command_with_args.replace(" --no-use-colors", "")
    # INFO
    if log_level == logging.INFO and dbt_project_dir.startswith(landops_path):
        tail_of_log = "\n".join(msg[-20:])
        message = (
            f"dbt project: ```{dbt_project_dir}```\ndbt command: ```{dbt_command}```\n"
            + f"Tail of the log:\n```{tail_of_log}```"
        )
        notify_slack(
            get_slack_channel(dbt_project_dir), ":check: DBT summary log", message
        )
    # ERROR
    elif log_level == logging.ERROR:
        markdown_error = msg.replace("\n", "`\n`")
        message = (
            f"dbt project: ```{dbt_project_dir}```\ndbt command: ```{dbt_command}```\n"
            + f"Error message:\n`{markdown_error}`"
        )
        notify_slack(
            get_slack_channel(dbt_project_dir), ":alert: DBT execution failed", message
        )


@flow()
def dbt_cli_command(
    command: str,
    project_path: str,
    repo_name: str = "de-dbt",
    git_branch: str = "master",
    command_args: str = "",
    profiles_dir: str = "profiles",
    warehouse: str = "",
) -> str:
    """
    Run dbt command in the specified dbt project.

    Args:
        command (str): dbt command to run.
        project_path (str): path to the dbt project.
        repo_name (str): name of the dbt repository.
        git_branch (str): git branch to clone.
        command_args (str): additional arguments for the dbt command.
        profiles_dir (str): path to the dbt profiles directory.
        warehouse (str): snowflake warehouse to use.

    Returns:
        str: dbt command output.

    Raises:
        Exception: if dbt command fails.
    """
    logger = get_run_logger()

    # Determine execution environment
    if os.environ.get("AWS_EXECUTION_ENV") == "AWS_ECS_FARGATE":
        execution_environment = "ECS Fargate"
    else:
        execution_environment = "local"

    logger.info(f"Running dbt from {execution_environment} execution environment")

    # Retrieve blocks
    environment = String.load("environment").value
    target = "qa" if environment == "dev" else "prod"
    dbt_password = Secret.load("snowflake-dbt-password").get()

    gh = GitHubRepository(
        repository_url=f"git@{repo_name}.github.com:eftours/{repo_name}.git",
        reference=git_branch,
    )

    # When running locally, revert to a regular ssh repo url (not using ssh profiles)
    #   and clear out the repo folder from prior runs
    if execution_environment == "local":
        gh.repository_url = f"git@github.com:eftours/{repo_name}.git"
        delete_repo_folder(repo_name)
        logger.info("Removed pre-existing repo folder.")

    # Clone dbt repo
    gh.get_directory(local_path=repo_name)
    logger.info(f"Cloned {repo_name}.")

    # Prepare dbt command inputs
    dbt_project_dir = f"./{repo_name}/{project_path}"
    profiles_dir = f"{dbt_project_dir}/{profiles_dir}"

    command_with_args = f"{command} --target {target} {command_args}".strip()

    dbt_env_vars = {
        "QA_SNOWFLAKE_USER": "QA_DBT",
        "QA_SNOWFLAKE_PASSWORD": dbt_password if target == "qa" else "",
        "QA_SNOWFLAKE_WAREHOUSE": f"QA_{warehouse}",
        "PROD_SNOWFLAKE_USER": "DBT",
        "PROD_SNOWFLAKE_PASSWORD": dbt_password if target == "prod" else "",
        "PROD_SNOWFLAKE_WAREHOUSE": warehouse,
        "DEV_SNOWFLAKE_USER": "DEV_DBT",
        "DEV_SNOWFLAKE_PASSWORD": dbt_password if target == "dev" else "",
        "DEV_SNOWFLAKE_WAREHOUSE": warehouse,
        "ORCHESTRATOR": "prefect",
        "JOB_RUN_ID": flow_run.id,
        "JOB_RUN_URL": flow_run.ui_url,
    }

    # Run dbt deps
    trigger_dbt_cli_command.with_options(name="dbt deps")(
        command="dbt --no-use-colors deps",
        profiles_dir=profiles_dir,
        project_dir=dbt_project_dir,
        env=dbt_env_vars,
    )

    # Run dbt
    try:
        shell_output = trigger_dbt_cli_command.with_options(name=command)(
            command=command_with_args,
            profiles_dir=profiles_dir,
            project_dir=dbt_project_dir,
            env=dbt_env_vars,
            return_all=True,
        )

        send_slack_message(
            logging.INFO, shell_output, dbt_project_dir, command_with_args
        )

    except Exception as e:
        send_slack_message(logging.ERROR, str(e), dbt_project_dir, command_with_args)
        raise e

    # Clean up on local runs
    if execution_environment == "local":
        delete_repo_folder(repo_name)
        logger.info("Removed repo folder.")

    return shell_output
