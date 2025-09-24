import os
from dotenv import load_dotenv
import logging
from datetime import timedelta
import duckdb
import pendulum

from helpers import DuckDBToBigQueryMapper, duckdb_getting_ids, duckdb_init_psql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv("../../.env.shared")

### date format
# date = pendulum.now("Asia/Jakarta")
date = pendulum.datetime(2025, 4, 14, tz="Asia/Jakarta")
psql_dstart = (date - timedelta(days=2)).strftime("%Y-%m-%d 17:00:00")
psql_dend = (date - timedelta(days=1)).strftime("%Y-%m-%d 17:00:00")
etl_date = date.strftime("%Y-%m-%d")

### table config
psql_schema = "public"
psql_table = "b2x_checkout_transaction"

### begin ingestion
with open("main_query.sql") as main_query:
    duck_conn = duckdb.connect()

    logging.info("setting up duckdb...")

    duckdb_setting = duckdb_init_psql(
        duck_conn=duck_conn,
        psql_conn=os.getenv("PSQL_CONN"),
        gcs_hmac_access_key=os.getenv("GOOGLE_HMAC_ACCESS_KEY"),
        gcs_hmac_access_key_secret=os.getenv("GOOGLE_HMAC_ACCESS_KEY_SECRET"),
    )

    logging.info("setup done.")

    indexes_df = duckdb_getting_ids(
        duck_conn=duck_conn, psql_schema=psql_schema, psql_table=psql_table
    )

    query_data = main_query.read().format(
        psql_schema=psql_schema, psql_table=psql_table
    )

    main_df = duck_conn.sql(
        query=query_data,
        params={
            "min_id": indexes_df["min_id"].iloc[0],
            "max_id": indexes_df["max_id"].iloc[0],
            "psql_dstart": psql_dstart,
            "psql_dend": psql_dend,
        },
    ).df()

    logging.info("executing main query:")
    print(query_data)

    logging.info("preview:")
    print(main_df.head())

    # logging.info("executing main query:")

# logging.info("executing main query:")
# logging.info(f"{query_data}")
# logging.info("uploading data to main query...")

# duck_conn.sql(query=query_parquet_gcs)
# logging.info("data uploaded.")
