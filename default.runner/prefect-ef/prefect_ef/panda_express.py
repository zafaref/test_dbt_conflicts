import pandas as pd
import sqlalchemy as sa
import urllib
import psutil

from datetime import datetime, timezone
from prefect import flow, get_run_logger
from prefect.blocks.system import JSON, Secret
from sqlalchemy import text
from snowflake.connector.pandas_tools import make_pd_writer
from snowflake.sqlalchemy import URL
from typing import Literal, Union

from .blocks import PandaExpress

# Input validation
AllowedServerTypes = Literal["mssql", "snowflake"]
AllowedLoadTypes = Literal["append_only", "drop_and_recreate", "truncate_and_insert"]


@flow
def panda_express_transfer(
    source_type: AllowedServerTypes,
    source_server: str,
    source_database: str,
    source_sql: str,
    target_type: AllowedServerTypes,
    target_server: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    target_prehook_sql: str = None,
    target_posthook_sql: str = None,
    load_type: AllowedLoadTypes = "drop_and_recreate",
    batch_size: int = 500000,
    insert_metadata: bool = True,
    prefect_secret_id: str = None,
    parse_dates: Union[list, dict] = None,
    **kwargs,
):
    DataTransfer(
        connection_config=PandaExpress.load("data-engineering").json(),
        transfer_config=dict(
            source_type=source_type,
            source_server=source_server,
            source_database=source_database,
            source_sql=source_sql,
            target_type=target_type,
            target_server=target_server,
            target_database=target_database,
            target_schema=target_schema,
            target_table=target_table,
            target_prehook_sql=target_prehook_sql,
            target_posthook_sql=target_posthook_sql,
            load_type=load_type,
            batch_size=batch_size,
            insert_metadata=insert_metadata,
            prefect_secret_id=prefect_secret_id,
            parse_dates=parse_dates,
            **kwargs,
        ),
    ).execute()


class DataTransfer:
    def __init__(self, connection_config, transfer_config):
        # Default config is written in first; passed config overwrites
        self.__dict__.update(connection_config)
        self.__dict__.update(transfer_config)
        self.snowflake_reserved_keywords = JSON.load(
            "snowflake-reserved-keywords"
        ).value
        self.log = get_run_logger()

        self.start_loading_time = pd.Timestamp.utcnow()

        # Parameter feedback
        if "load_type" not in transfer_config:
            self.log.info(
                "[TARGET] No load_type parameter provided.  Using 'drop_and_recreate' by default."
            )
            self.load_type = "drop_and_recreate"

        if (
            "prefect_secret_id" in transfer_config
            and self.prefect_secret_id is not None
        ):
            self.log.info("[MSSQL] Overriding MSSQL password with given secret")
            password = None
            try:
                password = Secret.load(self.prefect_secret_id).get()
            except ValueError:
                self.log.warning(
                    "[PREFECT] The parameter prefect_secret_id was provided, but the identifier does not exist in prefect"
                )
                password = None

            if password is not None:
                self.mssql_password = password

        if "batch_size" not in transfer_config:
            self.batch_size = 500000

        # Set target object casing and long name
        self.adjust_target_object_casing()
        self.full_table_name = (
            f"{self.target_database}.{self.target_schema}.{self.target_table}"
        )

    def execute(self):
        # Open up connections
        self.initialize_connections()

        # Run pre-hook sql if needed
        self.execute_hook_sql(hook_type="pre-hook", hook_sql=self.target_prehook_sql)

        # Open transaction at target
        self.update_transaction("BEGIN")

        # Set session attributes
        if self.target_type == "snowflake":
            self.target_conn.execute(
                text("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_TZ';")
            )
            self.target_conn.execute(text("ALTER SESSION SET TIMEZONE = 'UTC';"))

        # Download from source
        self.log.info(f'[SOURCE] Executing sql: "{self.source_sql}"')

        # Loop through query results in chunks
        total_row_count = 0
        for batch_number, batch_df in enumerate(
            pd.read_sql_query(
                sql=self.source_sql,
                con=self.source_engine,
                chunksize=self.batch_size,
                parse_dates=self.parse_dates,
            )
        ):
            self.log.debug(f"[SOURCE] Dataframe dtypes: {batch_df.dtypes.to_dict()}")

            total_row_count += batch_df.shape[0]
            self.log.info(
                f"[SOURCE] Dataframe batch load complete: {batch_df.shape[0]} records"
            )
            memory_usage = batch_df.memory_usage(deep=True).sum() / 1024**2
            self.log.info(f"[SOURCE] Dataframe memory usage: {memory_usage:.3f} MB")

            # System memory usage
            sys_memory_usage = psutil.virtual_memory().used / 1024**2
            self.log.info(
                f"[PESYSTEM] Panda-express container memory usage: {sys_memory_usage:.3f} MB"
            )

            # Add metadata
            batch_df["_pandaexpress_updated"] = pd.Timestamp.utcnow()

            # Adjust dataframe for target compatibility
            batch_df = self.fix_datetime_columns(df=batch_df, tz="US/Eastern")
            batch_df = self.purge_unsupported_datatypes(df=batch_df)
            batch_df = self.adjust_dataframe_column_names(df=batch_df)

            try:
                self.log.info(
                    f"[TARGET] {self.full_table_name}: Loading record batch..."
                )
                if self.load_type == "truncate_and_insert" and batch_number == 0:
                    self.truncate_target_table()
                batch_df.to_sql(
                    schema=self.get_target_object_casing(self.target_schema),
                    name=self.get_target_object_casing(self.target_table),
                    con=self.target_conn,
                    index=False,
                    method=self.get_target_insert_method(),
                    if_exists=self.get_if_exists_value(batch_number),
                    chunksize=self.get_target_insert_chunksize(),
                )
                self.log.info(
                    f"[TARGET] {self.full_table_name}: Inserted {batch_df.shape[0]} records in batch; {total_row_count} in total"
                )
            except Exception as e:
                self.update_transaction("ROLLBACK")
                self.log.error(
                    f"Error encountered while trying to insert records to {self.target_type}: {e}"
                )
                raise e

        # Run post-hook sql if needed
        self.execute_hook_sql(hook_type="post-hook", hook_sql=self.target_posthook_sql)

        # Insert metadata record to audit table
        if self.insert_metadata:
            self.insert_metadata_record(total_row_count)

        # Commit transaction at target
        self.update_transaction("COMMIT")
        self.log.info(f"[TARGET] {self.full_table_name}: Load complete")

        # Close out
        self.close_connections()
        self.log.info("All connections closed.")

        return total_row_count

    def initialize_connections(self):
        # Connect to target
        self.target_engine = self.get_generic_engine(self.target_type, "target")
        self.target_conn = self.get_generic_connection("target")
        self.log.info(
            f"[TARGET] Connected to {self.target_type}: {self.target_server} {self.target_database}"
        )

        # Connect to source
        self.source_engine = self.get_generic_engine(self.source_type, "source")
        self.source_conn = self.get_generic_connection("source")
        self.log.info(
            f"[SOURCE] Connected to {self.source_type}: {self.source_server} {self.source_database}"
        )

    # Handle pre and post hook sql execution
    def execute_hook_sql(self, hook_type: str, hook_sql: str):
        if hook_sql:
            # Split sql statement using "||" separator if needed to run in separate sessions
            sql_statements = [x.strip() for x in hook_sql.split("||")]
            for sql_statement in sql_statements:
                self.log.info(f'[TARGET] Executing {hook_type} sql: "{sql_statement}"')
                results = self.target_conn.execute(text(sql_statement))
                try:
                    self.log.info(
                        f"[TARGET] Result of {hook_type} sql: {results.mappings().all()}"
                    )
                except:
                    pass

    def close_connections(self):
        self.source_conn.close()
        self.source_engine.dispose()
        self.target_conn.close()
        self.target_engine.dispose()

    def get_generic_connection(self, alignment):
        if alignment == "source":
            return self.source_engine.connect().execution_options(autocommit=False)
        else:
            if self.target_type == "mssql":
                return self.target_engine.connect()
            else:
                return self.target_engine.connect().execution_options(autocommit=False)

    def get_generic_engine(self, engine_type, alignment):
        if engine_type == "mssql":
            url = self.get_mssql_engine_url(alignment)
            return sa.create_engine(url, fast_executemany=True)
        elif engine_type == "snowflake":
            url = self.get_snowflake_engine_url(alignment)
            return sa.create_engine(url)
        else:
            raise Exception(f"Unknown {alignment} type: {self.engine_type}")

    def get_mssql_engine_url(self, alignment):
        params = urllib.parse.quote_plus(
            f"DRIVER={self.mssql_driver};"
            f"SERVER={self.source_server if alignment == 'source' else self.target_server};"
            f"DATABASE={self.source_database if alignment == 'source' else self.target_database};"
            f"UID={self.mssql_username};"
            f"PWD={self.mssql_password};"
        )
        return f"mssql+pyodbc:///?odbc_connect={params}"

    def get_snowflake_engine_url(self, alignment):
        return URL(
            account=self.source_server if alignment == "source" else self.target_server,
            user=self.snowflake_user,
            password=self.snowflake_password,
            database=(
                self.source_database if alignment == "source" else self.target_database
            ),
            schema=self.source_schema if alignment == "source" else self.target_schema,
            warehouse=self.snowflake_warehouse,
            role=self.snowflake_role,
        )

    def get_if_exists_value(self, batch_number):
        if self.load_type == "drop_and_recreate" and batch_number == 0:
            return "replace"
        elif self.load_type == "drop_and_recreate" and batch_number > 0:
            return "append"
        elif self.load_type in ["append_only", "truncate_and_insert"]:
            return "append"
        else:
            return "replace"

    # clean_snowflake_column_name() takes a column name input and modifies the name so that it
    #   will be acceptable to Snowflake.
    def clean_snowflake_column_name(self, column):
        # Convert to string and lowercase
        column = str(column).lower()

        # Remove certain special characters
        replace_set = '!"#$%&()*+,-./:;<=>?@[\]^`{|}~ '
        for char in replace_set:
            column = column.replace(char, "")

        # Prefix reserved keywords with an underscore
        if column.upper() in self.snowflake_reserved_keywords:
            column = f"_{column}"

        return column

    def adjust_target_object_casing(self):
        # Coerce all snowflake object inputs to uppercase
        if self.source_type == "mssql" and self.target_type == "snowflake":
            self.target_database = self.target_database.upper()
            self.target_schema = self.target_schema.upper()
            self.target_table = self.target_table.upper()

    def adjust_dataframe_column_names(self, df):
        if self.source_type == "mssql" and self.target_type == "snowflake":
            df.columns = [self.clean_snowflake_column_name(x) for x in df.columns]

        return df

    def fix_datetime_columns(self, df, tz="UTC"):
        if self.target_type == "snowflake":
            # Convert timezone-naive datetimes to be timezone-specific
            for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
                df[col] = df[col].dt.tz_localize(
                    tz, ambiguous=True, nonexistent="shift_forward"
                )

            # Some source tz-aware datatypes are loaded into pandas as an "object";
            #   which requires UTC alignment to prevent date corruption
            for col in df.select_dtypes(include=["object"]).columns:
                first_notnull_index = df[col].first_valid_index()
                if first_notnull_index is not None:
                    first_notnull_value = df[col].iloc[first_notnull_index]
                    if isinstance(first_notnull_value, datetime):
                        df[col] = df[col].apply(
                            lambda x: (
                                x.replace(tzinfo=timezone.utc) if x is not None else x
                            )
                        )

        if self.target_type == "mssql":
            # In MSSQL datetime columns are loaded as timestamps which leads to troubles,
            # since there can be only one timestamp column in a table.
            # The straightforward workaround is to convert datetime columns into strings
            for col in df.select_dtypes(
                include=["datetime64[ns, UTC]", "datetime64[ns]"]
            ).columns:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

        return df

    def purge_unsupported_datatypes(self, df):
        if self.target_type == "snowflake":
            for col in df.select_dtypes(include=["object"]).columns:
                first_notnull_index = df[col].first_valid_index()
                if first_notnull_index is not None:
                    first_notnull_value = df[col].iloc[first_notnull_index]
                    if isinstance(first_notnull_value, bytes):
                        self.log.warning(
                            f"[SOURCE] Dropped column '{col}' because of",
                            f"unsupported datatype {type(first_notnull_value).__name__}",
                        )
                        df.drop(columns=[col], inplace=True)

        return df

    def update_transaction(self, command):
        if self.target_type == "mssql":
            sql = f"{command} TRAN"
        elif self.target_type == "snowflake":
            sql = command
        else:
            sql = None

        self.target_conn.execute(text(sql))

    def truncate_target_table(self):
        if self.target_type == "mssql":
            sql = "IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            sql += f"WHERE TABLE_SCHEMA = '{self.target_schema}' AND TABLE_NAME = '{self.target_table}')"
            sql += f"\n\tTRUNCATE TABLE {self.target_schema}.{self.target_table};"
        elif self.target_type == "snowflake":
            sql = f"TRUNCATE TABLE IF EXISTS {self.target_schema}.{self.target_table};"
        else:
            sql = None

        self.target_conn.execute(text(sql))

    def get_target_object_casing(self, input):
        if self.target_type == "snowflake":
            return input.lower()
        else:
            return input

    def get_target_insert_method(self):
        if self.target_type == "snowflake":
            return make_pd_writer(quote_identifiers=False)
        else:
            return None

    def get_target_insert_chunksize(self):
        if self.target_type == "mssql":
            return 1000
        else:
            return None

    def insert_metadata_record(self, total_row_count):
        df = self.fix_datetime_columns(
            pd.DataFrame(
                {
                    "source_type": [self.source_type],
                    "source_server": [self.source_server],
                    "source_database": [self.source_database],
                    "source_sql": [self.source_sql],
                    "target_type": [self.target_type],
                    "target_server": [self.target_server],
                    "target_database": [self.target_database],
                    "target_schema": [self.target_schema],
                    "target_table": [self.target_table],
                    "target_prehook_sql": [self.target_prehook_sql],
                    "target_posthook_sql": [self.target_posthook_sql],
                    "load_type": [self.load_type],
                    "batch_size": [self.batch_size],
                    "total_row_count": [total_row_count],
                    "start_loading_time": [self.start_loading_time],
                    "end_loading_time": [pd.Timestamp.utcnow()],
                }
            )
        )

        df.to_sql(
            schema=self.target_schema.lower(),
            name="pandaexpress_audit",
            con=self.target_conn,
            index=False,
            method=self.get_target_insert_method(),
            chunksize=1,
            if_exists="append",
        )

        self.log.info(
            f"[TARGET] Inserted metadata record to {self.target_schema}.pandaexpress_audit."
        )
