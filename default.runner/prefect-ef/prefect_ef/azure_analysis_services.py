import json
import requests
import time

from prefect import task, get_run_logger

from .blocks import AzureOAuthHeaders

# OAuth Parameters
TENANT_ID = "f0d1c6fd-dff0-486a-8e91-cfefefc7d98d"
OAUTH_BLOCK_NAME = "azure-analysis-services-oauth-headers"

# Note: We will default to northeurope unless the server is listed here
server_region_exception = {"ssasproduction": "eastus"}


# Determine the resource_url based on the input AzureAS server
def get_resource_url(server: str) -> str:
    azure_region = server_region_exception.get(server, "northeurope")
    return f"https://{azure_region}.asazure.windows.net"


# Use credentials to retrieve an OAuth token from Microsoft
def get_access_token(resource_url: str) -> str:
    oauth_headers = AzureOAuthHeaders.load(OAUTH_BLOCK_NAME).json()
    oauth_headers["resource"] = resource_url

    oauth_endpoint = f"https://login.windows.net/{TENANT_ID}/oauth2/token"
    oauth_response = requests.post(url=oauth_endpoint, data=oauth_headers)
    oauth_response.raise_for_status()

    return oauth_response.json()["access_token"]


@task
def azure_analysis_services_model_refresh(
    server: str,
    model: str,
    type: str = "Full",
    retry_count: int = 2,
    objects: list = [],
    wait_for_completion: bool = False,
) -> dict:
    logger = get_run_logger()
    logger.info(
        f"{server=}, {model=}, {type=}, {retry_count=}, {objects=}, {wait_for_completion=}"
    )

    # Retrieve an OAuth2 token
    resource_url = get_resource_url(server)
    access_token = get_access_token(resource_url)

    # Send model refresh request
    refresh_url = f"{resource_url}/servers/{server}/models/{model}/refreshes"
    data = {"type": type, "RetryCount": retry_count, "Objects": objects}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    refresh_response = requests.post(
        url=refresh_url, headers=headers, data=json.dumps(data)
    )
    refresh_response.raise_for_status()

    # Extract the refresh id and log it
    refresh_id = refresh_response.json()["operationId"]
    logger.info(f"Refresh_id {refresh_id} created for {server}/{model}")

    # If configured to, poll the refresh status until completion/failure
    if wait_for_completion:
        status_url = f"{refresh_url}/{refresh_id}"
        while True:
            # Wait and then poll status
            time.sleep(5)
            status_response = requests.get(url=status_url, headers=headers)
            status_response.raise_for_status()

            # Report response and break if no longer in progress
            status = status_response.json()["status"]
            logger.info(f"Refresh_id {refresh_id} status: {status}")

            if status in ["notStarted", "inProgress"]:
                continue
            elif status in ["succeeded"]:
                break
            else:
                raise Exception(
                    f"Received refresh status of '{status}' from {status_url}",
                    f"Response payload: {json.dumps(status_response.json())}",
                )

    return refresh_id


# azure_analysis_services_model_refresh(
#     server="ssasproduction1", model="OpsEmergencyResponse", wait_for_completion=True
# )
