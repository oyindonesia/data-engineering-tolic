import logging
import json
import os
from datetime import timedelta, datetime
import duckdb
import pendulum
from dotenv import load_dotenv

from helpers import DuckDBToBigQueryMapper, compare_bigquery_schemas_dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv("../../.env.shared")

### date format
# date = pendulum.now("Asia/Jakarta")
date = pendulum.datetime(2025, 10, 8, tz="Asia/Jakarta")
psql_dstart = (date - timedelta(days=2)).strftime("%Y-%m-%d 17:00:00")
psql_dend = (date - timedelta(days=1)).strftime("%Y-%m-%d 17:00:00")
etl_date = date.strftime("%Y-%m-%d")
file_timestamp = datetime.now().strftime("%H%M%S")

### table config
psql_schema = "public"
psql_table = "b2x_checkout_transaction"
bq_project = os.getenv("GCP_PROJECT")
bq_dataset = "raw"
bq_table_name = f"{bq_project}.{bq_dataset}.{psql_table}"
duckdb_tbl = f"duckdb_sink_{psql_schema}_{psql_table}"

### gcs config
gcs_bucket_name = os.getenv("GCS_BUCKET")
gcs_bucket_prefix = f"gs://{gcs_bucket_name}"

gcs_data_path = f"{psql_table}/dt={etl_date}"
parquet_filename = f"{file_timestamp}.parquet"
parquet_gcs_uri = f"{gcs_bucket_prefix}/{gcs_data_path}/{parquet_filename}"

with duckdb.connect() as duck_conn:
    logging.info("setting up duckdb...")

    ### connect to postgres
    install_psql_ext = f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH '{os.getenv("PSQL_CONN")}'
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
            KEY_ID '{os.getenv("GCS_HMAC_ACCESS_KEY")}',
            SECRET '{os.getenv("GCS_HMAC_ACCESS_KEY_SECRET")}'
        );
    """

    ### settings for performance
    performance_setting = """
        SET enable_progress_bar = true;
        SET memory_limit = '8GB';
        SET threads TO 2;
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

    logging.info("setup done.")

    main_query = f"""
        SELECT * FROM {duckdb_tbl}
    """
    logging.info("getting min_id and max_id for indexing...")

    index_query = f"""
    SELECT
        min(id) as min_id,
        max(id) as max_id
    from
        pg.{psql_schema}.{psql_table}
    """

    indexes_df = duck_conn.sql(query=index_query).df()
    logging.info("index retrieved.")

    query_params = {
        "min_id": indexes_df["min_id"].iloc[0],
        "max_id": indexes_df["max_id"].iloc[0],
        "psql_dstart": psql_dstart,
        "psql_dend": psql_dend,
    }

    data_query = f"""
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

        union all

        SELECT
            *
        from
            pg.{psql_schema}.{psql_table}
        where
            true
            and id >= $min_id
            and id <= $max_id
            and last_updated >= $psql_dstart
            and last_updated < $psql_dend;
    """

    logging.info(f"creating table duckdb_sink_{psql_schema}_{psql_table} for upload...")
    data = duck_conn.sql(query=data_query, params=query_params)
    print(data.sql_query())
    # data.create(duckdb_tbl)
    # logging.info("table created.")

    # logging.info("peeking at table:\n")
    # print(duck_conn.table(duckdb_tbl).limit(1))

    # main_data = duck_conn.sql(query=main_query)
    # logging.info("generating dataframe...")
    # main_data_df = main_data.df()
    # logging.info("dataframe generated.")

    # logging.info(
    #     f"dataframe memory usage: {main_data_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
    # )
    # logging.info(
    #     f"retrieved {len(main_data_df)} rows with {len(main_data_df.columns)} columns."
    # )
    # logging.info("columns:\n")
    # print(main_data_df.columns)

    # logger.info(
    #     f"uploading parquet file: {parquet_filename} to {parquet_gcs_uri}..."
    # )

    # upload_parquet_to_gcs_query = f"""
    #     COPY {duckdb_tbl}
    #     TO '{parquet_gcs_uri}' (
    #         FORMAT PARQUET,
    #         COMPRESSION zstd
    #     );
    # """
    # # Create a copy query from the relation
    # print(upload_parquet_to_gcs_query)

    # duck_conn.sql(
    #     query=upload_parquet_to_gcs_query,
    # )
    # logger.info("✅ parquet file uploaded successfully.")
