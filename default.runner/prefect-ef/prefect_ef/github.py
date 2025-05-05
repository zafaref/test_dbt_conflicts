import os
import shutil
import stat
from prefect import task, get_run_logger
from prefect_github.repository import GitHubRepository


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


@task
def clone_github_repo(
    repo_name: str, git_branch: str = "master", local_path: str | None = None
) -> None:
    logger = get_run_logger()

    # Determine execution environment
    if os.environ.get("AWS_EXECUTION_ENV") == "AWS_ECS_FARGATE":
        execution_environment = "ECS Fargate"
    else:
        execution_environment = "local"

    logger.info(f"Running dbt from {execution_environment} execution environment")

    gh = GitHubRepository(
        repository_url=f"git@{repo_name}.github.com:eftours/{repo_name}.git",
        reference=git_branch,
    )

    # When running locally, revert to a regular ssh repo url (not using ssh profiles)
    #   and clear out the repo folder from prior runs
    if execution_environment == "local":
        gh.repository_url = f"git@github.com:eftours/{repo_name}.git"
        delete_repo_folder(local_path if local_path else repo_name)
        logger.info("Removed pre-existing repo folder.")

    # Clone dbt repo
    gh.get_directory(local_path=local_path if local_path else repo_name)
    logger.info(f"Cloned {repo_name}.")
