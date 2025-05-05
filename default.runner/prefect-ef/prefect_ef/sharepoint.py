import io
from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from shareplum.office365 import Office365
from shareplum.site import Version, Site


@task
def connect_to_sharepoint(
    base_site_url: str = "https://efcom.sharepoint.com",
    site_name: str = "site",
    user: str = "svc.tg.pwrbi.de@ef.com",
    password: str = None,
) -> Site:
    logger = get_run_logger()

    if not password:
        password = Secret.load("sharepoint-password").get()

    site_url = f"{base_site_url}/sites/{site_name}"
    logger.info(f"Connecting to {site_url}...")

    authcookie = Office365(base_site_url, username=user, password=password).GetCookies()
    site = Site(site_url, authcookie=authcookie, version=Version.v365)

    return site


@task
def get_sharepoint_folder(site: Site, folder_path: str):
    logger = get_run_logger()
    logger.info(f"Getting folder {folder_path}")
    folder = site.Folder(folder_path)

    return folder


@task
def list_sharepoint_folder(site: Site, folder_path: str) -> list:
    logger = get_run_logger()
    logger.info(f"Getting folder {folder_path}")
    folder = site.Folder(folder_path)
    files = folder.files
    logger.info(f"Found {len(files)} files in {folder_path}")
    logger.info(f"First 10 files: {files[:10]}")

    return folder.files


@task
def get_file_from_sharepoint(folder, file_name: str) -> bytes:
    logger = get_run_logger()
    logger.info(f"Getting file {file_name}")
    file_data = folder.get_file(file_name)

    return io.BytesIO(file_data)


@task
def get_file_from_sharepoint_v2(site: Site, server_relative_file_path: str) -> bytes:
    logger = get_run_logger()
    logger.info(f"Getting file {server_relative_file_path} from {site.site_url}...")
    response = site._session.get(
        site.site_url
        + f"/_api/web/GetFileByServerRelativeUrl('{server_relative_file_path}')/$value"
    )
    response.raise_for_status()

    return response.content


@task
def get_file_metadata_from_sharepoint(site: Site, server_relative_file_path: str) -> bytes:
    logger = get_run_logger()
    logger.info(f"Getting file {server_relative_file_path} from {site.site_url}...")
    response = site._session.get(
        site.site_url
        + f"/_api/web/GetFileByServerRelativeUrl('{server_relative_file_path}')/ListItemAllFields"
    )
    response.raise_for_status()

    return response.content


@task
def get_editor_info_from_sharepoint(site: Site, file_editor_id: str) -> bytes:
    logger = get_run_logger()
    logger.info(f"Getting editor info {file_editor_id} from {site.site_url}...")
    response = site._session.get(
        site.site_url
        + f"/_api/web/GetUserById({file_editor_id})"
    )
    response.raise_for_status()

    return response.content

 