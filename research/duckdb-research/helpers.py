import pandas as pd
from typing import Dict, Optional
import duckdb
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Add the DuckDB to BigQuery mapper class
class DuckDBToBigQueryMapper:
    """
    Maps DuckDB DataFrame data types to BigQuery external table schema format.
    """

    def __init__(self):
        # Mapping from DuckDB/pandas dtypes to BigQuery types
        self.type_mapping = {
            # Integer types
            "int8": "INTEGER",
            "int16": "INTEGER",
            "int32": "INTEGER",
            "int64": "INTEGER",
            "Int8": "INTEGER",
            "Int16": "INTEGER",
            "Int32": "INTEGER",
            "Int64": "INTEGER",
            # Float types
            "float32": "FLOAT",
            "float64": "FLOAT",
            "Float32": "FLOAT",
            "Float64": "FLOAT",
            # String types
            "object": "STRING",
            "string": "STRING",
            "category": "STRING",
            # Boolean types
            "bool": "BOOLEAN",
            "boolean": "BOOLEAN",
            # Date/time types
            "datetime64[ns]": "TIMESTAMP",
            "datetime64[ns, UTC]": "TIMESTAMP",
            "datetime64": "TIMESTAMP",
            "date": "DATE",
            "time": "TIME",
            "timedelta64[ns]": "TIME",
            # DuckDB specific types
            "VARCHAR": "STRING",
            "TEXT": "STRING",
            "BIGINT": "INTEGER",
            "INTEGER": "INTEGER",
            "SMALLINT": "INTEGER",
            "TINYINT": "INTEGER",
            "DOUBLE": "FLOAT",
            "REAL": "FLOAT",
            "DECIMAL": "NUMERIC",
            "NUMERIC": "NUMERIC",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIME": "TIME",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMPTZ": "TIMESTAMP",
            "UUID": "STRING",
            "BLOB": "BYTES",
            "JSON": "JSON",
        }

    def get_bigquery_type(self, dtype: str) -> str:
        """Convert a DuckDB/pandas dtype to BigQuery type."""
        dtype_str = str(dtype).upper()

        # Handle complex types
        if "DECIMAL" in dtype_str or "NUMERIC" in dtype_str:
            return "NUMERIC"
        elif "TIMESTAMP" in dtype_str:
            return "TIMESTAMP"
        elif "DATETIME" in dtype_str:
            return "TIMESTAMP"
        elif "DATE" in dtype_str:
            return "DATE"
        elif "TIME" in dtype_str:
            return "TIME"
        elif "VARCHAR" in dtype_str or "TEXT" in dtype_str:
            return "STRING"
        elif "INT" in dtype_str or "BIGINT" in dtype_str:
            return "INTEGER"
        elif "FLOAT" in dtype_str or "DOUBLE" in dtype_str or "REAL" in dtype_str:
            return "FLOAT"
        elif "BOOL" in dtype_str:
            return "BOOLEAN"

        return self.type_mapping.get(str(dtype), "STRING")

    def duckdb_describe_to_bq_schema(
        self, describe_df: pd.DataFrame, mode: str = "NULLABLE"
    ) -> list:
        """Convert DuckDB DESCRIBE result to BigQuery schema format."""
        schema = []

        for _, row in describe_df.iterrows():
            field = {
                "name": row["column_name"],
                "type": self.get_bigquery_type(row["column_type"]),
                "mode": mode,
            }
            schema.append(field)

        return schema

    def generate_external_table_ddl(
        self,
        schema: list,
        table_name: str,
        source_uris: list,
        format_type: str = "PARQUET",
        skip_leading_rows: int = 0,
    ) -> str:
        """Generate BigQuery CREATE EXTERNAL TABLE DDL statement."""
        # Build column definitions
        columns = []
        for field in schema:
            column_def = f"  {field['name']} {field['type']}"
            if field["mode"] == "REQUIRED":
                column_def += " NOT NULL"
            columns.append(column_def)

        columns_sql = ",\n".join(columns)

        # Build source URIs
        uris = [f"'{uri}'" for uri in source_uris]
        uris_sql = ",\n    ".join(uris)

        # Build DDL
        ddl = f"""CREATE OR REPLACE EXTERNAL TABLE `{table_name}` (
                    {columns_sql}
                    )
                    OPTIONS (
                      format = '{format_type}',
                      uris = [
                        {uris_sql}
      ]"""

        if format_type.upper() == "CSV" and skip_leading_rows > 0:
            ddl += f",\n  skip_leading_rows = {skip_leading_rows}"

        ddl += "\n);"

        return ddl


def duckdb_init_psql(
    duck_conn: duckdb.DuckDBPyConnection,
    psql_conn: str | None,
    gcs_hmac_access_key: str | None,
    gcs_hmac_access_key_secret: str | None,
) -> None:
    try:
        ### connect to postgres
        install_psql_ext = f"""
                INSTALL postgres;
                LOAD postgres;
                ATTACH '{psql_conn}'
                AS pg (TYPE POSTGRES, READ_ONLY);
            """

        ### install httpfs extension for file transfer
        install_httpfs_ext = """
                INSTALL httpfs;
                LOAD httpfs;
        """

        ### create secret to upload parquet files to GCS
        create_gcs_secret = f"""
                CREATE SECRET (
                    TYPE gcs,
                    KEY_ID '{gcs_hmac_access_key}',
                    SECRET '{gcs_hmac_access_key_secret}',
                    URL_STYLE path
                );
            """

        ### settings for performance
        performance_setting = """
            SET memory_limit = '2GB';
            SET threads TO 2;
            SET enable_progress_bar = true;
            SET preserve_insertion_order = false;
        """

        logging.info("installing psql extension...")
        duck_conn.sql(install_psql_ext)

        logging.info("installing httpfs extension...")
        duck_conn.sql(install_httpfs_ext)

        logging.info("creating gcs secret...")
        duck_conn.sql(create_gcs_secret)

        logging.info("setting the performance...")
        duck_conn.sql(performance_setting)

    except Exception as e:
        logger.error(e, exc_info=True)
        raise


def duckdb_read_query(
    duck_conn: duckdb.DuckDBPyConnection,
    file: str,
    psql_schema: Optional[str] | None,
    psql_table: Optional[str] | None,
    params: Optional[Dict] = None,
) -> None:
    try:
        if os.path.exists(file):
            with open(file) as query_file:
                query = query_file.read().format(
                    psql_schema=psql_schema, psql_table=psql_table
                )

                result = duck_conn.sql(query=query, params=params)
                return result
        else:
            logger.error(".sql file not found.", exc_info=True)

    except Exception as e:
        logger.error(e, exc_info=True)
        raise


def duckdb_upload_parquet_to_bucket(
    duck_conn: duckdb.DuckDBPyConnection,
    query: str,
    path: str,
    format: str,
    file: str = "upload_file_query.sql",
) -> None:
    try:
        if os.path.exists(file):
            with open(file) as query_file:
                upload_command = query_file.read().format(
                    query=query, path=path, format=format
                )

                return duck_conn.sql(query=upload_command)
        else:
            logger.error(".sql file not found.", exc_info=True)

    except Exception as e:
        logger.error(e, exc_info=True)
        raise


# def duckdb_getting_index_id(
#     duck_conn: duckdb.DuckDBPyConnection,
#     file: str,
#     psql_schema: str | None,
#     psql_table: str | None,
# ) -> pd.DataFrame:
#     try:
#         if os.path.exists(file):
#             with open(file) as query:
#                 query_index = query.read().format(
#                     psql_schema=psql_schema, psql_table=psql_table
#                 )
#
#                 logging.info("getting max_id & min_id for indexing...")
#                 indexes_df = duck_conn.sql(query=query_index).df()
#                 return indexes_df
#         else:
#             logger.error(".sql file not found.", exc_info=True)
#             raise
#
#     except Exception as e:
#         logger.error(e, exc_info=True)
#         raise
#
#
# def duckdb_describe_query(
#     duck_conn: duckdb.DuckDBPyConnection,
#     query: str,
#     psql_dstart: str,
#     psql_dend: str,
#     indexes_df: pd.DataFrame,
# ) -> pd.DataFrame:
#     ### show result of DESCRIBE from duckdb
#     print("=== DuckDB Schema Description ===")
#     describe_df = duck_conn.sql(
#         query=("DESCRIBE " + query),
#         params={
#             "min_id": indexes_df["min_id"].iloc[0],
#             "max_id": indexes_df["max_id"].iloc[0],
#             "psql_dstart": psql_dstart,
#             "psql_dend": psql_dend,
#         },
#     ).df()
#     return describe_df


# ### generate schema from DESCRIBE result
# logging.info("generating BigQuery schema...")
# bq_mapper = DuckDBToBigQueryMapper()
# bq_schema_from_describe = bq_mapper.duckdb_describe_to_bq_schema(describe_df)
#
# print("\n=== BigQuery Schema (from DESCRIBE) ===")
# print(json.dumps(bq_schema_from_describe, indent=2))
#
# gcs_bucket = os.getenv("DEV_GCS_BUCKET")
#
# gcs_bucket_path = f"gs://{gcs_bucket}/{psql_table}/dt={etl_date}/duckdb.parquet"
# query_parquet_gcs = f"""
#     COPY (
#     {query_data}
#     )
#     TO '{gcs_bucket_path}' (
#         FORMAT parquet,
#         COMPRESSION zstd
#     );
# """
