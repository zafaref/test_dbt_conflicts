from prefect import flow, get_run_logger
from prefect_ef import dbt_cli_command


# Entry point for the flow
@flow(description="dbt run flow with additional arguments")
def dbt_batch_run_flow(
    dbt_projects: list,
    process_name: str = "dbt-run",
    command: str = "dbt run",
) -> list:
    logger = get_run_logger()
    logger.info(f"{dbt_projects=}")

    output = []

    for dbt_project in dbt_projects:
        logger.info(f"Starting {command}: {dbt_project}")
        try:
            dbt_output = dbt_cli_command.with_options(
                name=f"{process_name}--{dbt_project['project_name'].replace('/','-')}",
                # retries=1,
            )(
                command=command[: command.find(" ")]
                + " --no-use-colors"
                + command[command.find(" ") :],
                project_path=dbt_project["project_name"],
                repo_name=dbt_project.get("repo_name", "de-dbt"),
                git_branch=dbt_project.get("git_branch", "master"),
                command_args=dbt_project.get("dbt_run_arguments", ""),
            )

        # continue to run other dbt projects even if anyone fails
        except Exception as e:
            logger.warning(
                f"dbt execution failed. Command: {command}. Parameters: {dbt_project}.\nError: {e}"
            )
            dbt_output = str(e)

        output.append(
            {
                "dbt_project": dbt_project,
                "dbt_command": command,
                "dbt_output": dbt_output,
            }
        )

    return output
