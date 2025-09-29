import logging
from typing import List
import os
from datetime import timedelta
import json
import duckdb
import pendulum
from dotenv import load_dotenv

from helpers import (
    duckdb_init_psql,
    duckdb_read_query,
    duckdb_upload_parquet_to_bucket,
    DuckDBToBigQueryMapper,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv("../../.env.shared")

### date format
# date = pendulum.now("Asia/Jakarta")
date = pendulum.datetime(2025, 4, 14, tz="Asia/Jakarta")
psql_dstart = (date - timedelta(days=2)).strftime("%Y-%m-%d 17:00:00")
psql_dend = (date - timedelta(days=1)).strftime("%Y-%m-%d 17:00:00")
etl_date = date.strftime("%Y-%m-%d")
file_timestamp = date.strftime("%H%M%S")

### table config
psql_schema = "public"
psql_table = "b2x_checkout_transaction"

try:
    duck_conn = duckdb.connect()

    logging.info("setting up duckdb...")

    duckdb_setting = duckdb_init_psql(
        duck_conn=duck_conn,
        psql_conn=os.getenv("PSQL_CONN"),
        gcs_hmac_access_key=os.getenv("GOOGLE_HMAC_ACCESS_KEY"),
        gcs_hmac_access_key_secret=os.getenv("GOOGLE_HMAC_ACCESS_KEY_SECRET"),
    )

    logging.info("setup done.")

    logging.info("getting min_id and max_id for indexing...")
    indexes_df = duckdb_read_query(
        file="index_query.sql",
        duck_conn=duck_conn,
        psql_schema=psql_schema,
        psql_table=psql_table,
    ).df()
    logging.info("index retrieved.")

    logging.info("executing main query...")
    main_data = duckdb_read_query(
        file="main_query.sql",
        duck_conn=duck_conn,
        psql_schema=psql_schema,
        psql_table=psql_table,
        params={
            "min_id": indexes_df["min_id"].iloc[0],
            "max_id": indexes_df["max_id"].iloc[0],
            "psql_dstart": psql_dstart,
            "psql_dend": psql_dend,
        },
    )

    main_data_df = main_data.df()
    logging.info("query executed.")

    logging.info(
        f"Retrieved {len(main_data_df)} rows with {len(main_data_df.columns)} columns."
    )
    logging.info(
        f"DataFrame memory usage: {main_data_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
    )

    # ### NOTE: enable for debugging
    # print(query_data)

    logging.info("data preview:")
    print(main_data_df.head())

    logging.info("uploading file...")
    gcs_bucket = os.getenv("GCS_BUCKET")
    gcs_bucket_path = (
        f"gs://{gcs_bucket}/{psql_table}/dt={etl_date}/duckdb-{file_timestamp}.parquet"
    )
    upload_job = duckdb_upload_parquet_to_bucket(
        duck_conn=duck_conn, query=query, path=gcs_bucket_path, format="parquet"
    )
    logging.info("file uploaded.")

except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
    raise

#     def main_job() -> None:
#         logging.info("executing main query...")
#         main_df = db_conn.sql(
#             query=query_data,
#             params={
#                 "min_id": indexes_df["min_id"].iloc[0],
#                 "max_id": indexes_df["max_id"].iloc[0],
#                 "psql_dstart": psql_dstart,
#                 "psql_dend": psql_dend,
#             },
#         ).df()
#         logging.info("query executed.")
#
#         logging.info(
#             f"Retrieved {len(main_df)} rows with {len(main_df.columns)} columns."
#         )
#         logging.info(
#             f"DataFrame memory usage: {main_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
#         )
#
#         # print(query_data)
#
#         logging.info("data preview:")
#         print(main_df.head())
#         return main_df
#
#     def generate_schema_job() -> List:
#         logging.info("running DESCRIBE to query...")
#         describe_df = duckdb_describe_query(
#             duck_conn=db_conn,
#             query=query_data,
#             psql_dstart=psql_dstart,
#             psql_dend=psql_dend,
#             indexes_df=indexes_df,
#         )
#         print(describe_df)
#
#         logging.info("generating BQ external table schema...")
#         bq_schema_mapper = DuckDBToBigQueryMapper()
#         bq_schema = bq_schema_mapper.duckdb_describe_to_bq_schema(describe_df)
#
#         print(json.dumps(bq_schema, indent=2))
#         return bq_schema
#
#     def upload_parquet() -> None:
#         gcs_bucket = os.getenv("GCS_BUCKET")
#         duckdb_upload_parquet_to_bucket(duck_conn=db_conn, query=query_data)
#         gcs_bucket_path = f"gs://{gcs_bucket}/{psql_table}/dt={etl_date}/duckdb.parquet"
#
# # logging.info("executing main query:")
# # logging.info(f"{query_data}")
# # logging.info("uploading data to main query...")
#
# # duck_conn.sql(query=query_parquet_gcs)
# # logging.info("data uploaded.")
