import pyodbc
import requests
import uuid

from prefect import task, get_run_logger
from prefect.blocks.system import Secret

from .blocks import AzureOAuthHeaders

OAUTH_BLOCK_NAME = "powerbi-oauth-headers"


def get_azure_ids(workspace_name: str, dataset_name: str) -> dict:
    sql = f"EXEC pbi.usp_PowerBIRefreshIds_Get @WORKSPACE_NAME = '{workspace_name}', @DATASET_NAME = '{dataset_name}'"

    with pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER=toursbi.eftsd.com;"
        f"DATABASE=SRC_ExternalSources;"
        f"UID=prefect;"
        f"PWD={Secret.load('prefect-sqlserver-password').get()};",
        autocommit=True,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = dict(
                zip([column[0] for column in cursor.description], cursor.fetchone())
            )

    return row["WorkspaceId"], row["DatasetId"]


# Use credentials to retrieve an OAuth token from Microsoft
def get_access_token(oauth_block_name: str) -> str:
    oauth_headers = AzureOAuthHeaders.load(oauth_block_name).json()

    oauth_endpoint = "https://login.microsoftonline.com/common/oauth2/token"
    oauth_response = requests.post(url=oauth_endpoint, data=oauth_headers)
    oauth_response.raise_for_status()

    return oauth_response.json()["access_token"]


# Use the OAuth token to retrieve PowerBI headers
def get_powerbi_headers(oauth_block_name: str) -> dict:
    # These headers will be used for all API calls
    access_token = get_access_token(oauth_block_name)
    powerbi_headers = {
        "Authorization": "Bearer " + access_token,
        "User-Agent": "dataengineering-powerbi",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "client-request-id": str(uuid.uuid4()),
    }

    return powerbi_headers


@task(retries=3, retry_delay_seconds=5)
def powerbi_dataset_refresh(workspace_name: str, dataset_name: str) -> int:
    logger = get_run_logger()

    # Retrieve azure guid's for the workspace and dataset
    logger.info(
        f"Attempting to resolve workspace '{workspace_name}' and dataset '{dataset_name}'..."
    )
    group_id, dataset_id = get_azure_ids(workspace_name, dataset_name)
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes"
    logger.info(
        f"Resolved inputs to workspace_id {group_id} and dataset_id {dataset_id}"
    )

    # Authenticate with PowerBI
    headers = get_powerbi_headers(OAUTH_BLOCK_NAME)

    # Send dataset refresh request
    response = requests.post(
        url=url,
        headers=headers,
        # Let's let Prefect handle retries and notifications
        # json={"notifyOption": "MailOnFailure", "retryCount": 2},
    )
    logger.info(f"Received dataset refresh api response: {response.status_code}")
    response.raise_for_status()
    return response.status_code
