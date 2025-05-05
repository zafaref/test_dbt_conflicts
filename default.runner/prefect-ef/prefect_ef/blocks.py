from typing import Optional
from prefect.blocks.core import Block
from pydantic import SecretStr


class AzureOAuthHeaders(Block):
    grant_type: Optional[str] = None
    resource: Optional[str] = None
    scope: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[SecretStr] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None

    def json(self) -> dict:
        d = {
            "grant_type": self.grant_type,
            "resource": self.resource,
            "scope": self.scope,
            "client_id": self.client_id,
            "client_secret": None
            if self.client_secret is None
            else self.client_secret.get_secret_value(),
            "username": self.username,
            "password": None
            if self.password is None
            else self.password.get_secret_value(),
        }
        # Return only k:v values where the value is defined
        return {k: v for k, v in d.items() if v is not None}


class MsSqlConnection(Block):
    server: str
    username: str
    password: SecretStr
    database: Optional[str] = None
    db_schema: Optional[str] = None

    def json(self) -> dict:
        d = {
            "server": self.server,
            "username": self.username,
            "password": self.password.get_secret_value(),
            "database": self.database,
            "db_schema": self.db_schema,
        }
        # Return only k:v values where the value is defined
        return {k: v for k, v in d.items() if v is not None}


class PandaExpress(Block):
    snowflake_account: str
    snowflake_database: str
    snowflake_warehouse: str
    snowflake_schema: str
    snowflake_role: str
    snowflake_autocommit: bool
    snowflake_user: str
    snowflake_password: SecretStr
    mssql_server: str
    mssql_driver: str
    mssql_database: str
    mssql_schema: str
    mssql_username: str
    mssql_password: SecretStr

    _block_type_name = "Panda Express"
    _logo_url = "https://www.pandarg.com/sites/default/files/px-logo-new.png"
    _description = "Connection configuration for Panda Express data transfer operations"

    def json(self) -> dict:
        d = {
            "snowflake_account": self.snowflake_account,
            "snowflake_database": self.snowflake_database,
            "snowflake_warehouse": self.snowflake_warehouse,
            "snowflake_schema": self.snowflake_schema,
            "snowflake_role": self.snowflake_role,
            "snowflake_autocommit": self.snowflake_autocommit,
            "snowflake_user": self.snowflake_user,
            "snowflake_password": self.snowflake_password.get_secret_value(),
            "mssql_server": self.mssql_server,
            "mssql_driver": self.mssql_driver,
            "mssql_database": self.mssql_database,
            "mssql_schema": self.mssql_schema,
            "mssql_username": self.mssql_username,
            "mssql_password": self.mssql_password.get_secret_value(),
        }
        # Return only k:v values where the value is defined
        return {k: v for k, v in d.items() if v is not None}


class SalesforceCredentials(Block):
    consumer_key: str
    consumer_secret: SecretStr
    username: str
    password: SecretStr
    security_token: SecretStr
    api_version: str

    def json(self) -> dict:
        d = {
            "consumer_key": self.consumer_key,
            "consumer_secret": self.consumer_secret.get_secret_value(),
            "username": self.username,
            "password": self.password.get_secret_value(),
            "security_token": self.security_token.get_secret_value(),
            "api_version": self.api_version,
        }
        # Return only k:v values where the value is defined
        return {k: v for k, v in d.items() if v is not None}
