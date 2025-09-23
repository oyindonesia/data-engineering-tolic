import json
import logging
import os
from datetime import timedelta

import duckdb
import pendulum
from dotenv import load_dotenv

from helpers import DuckDBToBigQueryMapper

logging.basicConfig(level=logging.INFO)

load_dotenv(".env.shared")

duck_conn = duckdb.connect()

### connect to postgres
install_psql_ext = f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH '{os.getenv("DEV_PSQL_CONN")}'
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
            KEY_ID '{os.getenv("GOOGLE_HMAC_ACCESS_KEY")}',
            SECRET '{os.getenv("GOOGLE_HMAC_ACCESS_KEY_SECRET")}',
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

### table config
psql_schema = "public"
psql_table = "b2x_checkout_transaction"

# date = pendulum.now("Asia/Jakarta")
date = pendulum.datetime(2025, 4, 14, tz="Asia/Jakarta")
psql_dstart = (date - timedelta(days=2)).strftime("%Y-%m-%d 17:00:00")
psql_dend = (date - timedelta(days=1)).strftime("%Y-%m-%d 17:00:00")
etl_date = date.strftime("%Y-%m-%d")

### get min & max id for indexing
query_index = f"""
    SELECT
        min(id) as min_id,
        max(id) as max_id
    from
        pg.{psql_schema}.{psql_table}
"""

logging.info("getting max_id & min_id for indexing...")
indexes_df = duck_conn.sql(query=query_index).df()
print(indexes_df)

### main query to get data
query_data = f"""
    SELECT
        *
    from
        pg.{psql_schema}.{psql_table}
    where
        true
        and id >= $min_id
        and id <= $max_id
        and created >= $psql_dstart
        and created < $psql_dend
"""

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
logging.info(f"{query_data}")

logging.info(f"Retrieved {len(main_df)} rows with {len(main_df.columns)} columns")
logging.info(
    f"DataFrame memory usage: {main_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
)

### show result of DESCRIBE from duckdb
print("=== DuckDB Schema Description ===")
describe_df = duck_conn.sql(
    query=("DESCRIBE " + query_data),
    params={
        "min_id": indexes_df["min_id"].iloc[0],
        "max_id": indexes_df["max_id"].iloc[0],
        "psql_dstart": psql_dstart,
        "psql_dend": psql_dend,
    },
).df()
print(describe_df)

### generate schema from DESCRIBE result
logging.info("generating BigQuery schema...")
bq_mapper = DuckDBToBigQueryMapper()
bq_schema_from_describe = bq_mapper.duckdb_describe_to_bq_schema(describe_df)

print("\n=== BigQuery Schema (from DESCRIBE) ===")
print(json.dumps(bq_schema_from_describe, indent=2))

gcs_bucket = f"gs://dev-duckdb-sink/{psql_table}/dt={etl_date}/duckdb.parquet"
query_parquet_gcs = f"""
    COPY (
    {query_data}
    )
    TO '{gcs_bucket}' (
        FORMAT parquet,
        COMPRESSION zstd
    );
"""

# logging.info("executing main query:")
# logging.info(f"{query_data}")
# logging.info("uploading data to main query...")

# duck_conn.sql(query=query_parquet_gcs)
# logging.info("data uploaded.")
