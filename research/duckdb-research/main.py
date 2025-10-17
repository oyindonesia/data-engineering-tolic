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

gcs_schema_path = f"{psql_table}/schema"
schema_json_file = "bq_schema.json"

def main_ingestion():
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
            SET memory_limit = '2GB';
            SET threads TO 1;
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

        def get_main_data_df():
            ### data query
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

            logging.info(
                f"creating table duckdb_sink_{psql_schema}_{psql_table} for upload..."
            )
            data = duck_conn.sql(query=data_query, params=query_params)
            data.create(duckdb_tbl)
            logging.info("table created.")

            logging.info("peeking at table:\n")
            print(duck_conn.table(duckdb_tbl).limit(1))

            main_data = duck_conn.sql(query=main_query)
            logging.info("generating dataframe...")
            main_data_df = main_data.df()
            logging.info("dataframe generated.")

            logging.info(
                f"dataframe memory usage: {main_data_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
            )
            logging.info(
                f"retrieved {len(main_data_df)} rows with {len(main_data_df.columns)} columns."
            )
            logging.info("columns:\n")
            print(main_data_df.columns)
            return main_data_df

        # def query_existing_data():
        #     print("=" * 80)
        #     print("query existing data")
        #     print("=" * 80)
        #     gcs_query = f"""
        #         SELECT
        #             * EXCLUDE (dt)
        #         FROM
        #             read_parquet('{gcs_bucket_prefix}/{psql_table}/*/*.parquet')
        #     """
        #     logging.info("peeking at existing data:\n")
        #     existing_data = duck_conn.sql(query=gcs_query)
        #     print(existing_data.limit(1))
        #     return existing_data.df()

        # def get_bq_ext_tbl_schema():
        #     from google.cloud import bigquery
        #     import io

        #     client = bigquery.Client()

        #     logging.info("get existing BQ schema...")
        #     table_ref = client.dataset(bq_dataset, project=bq_project).table(psql_table)
        #     table = client.get_table(table_ref)

        #     f = io.StringIO("")
        #     client.schema_to_json(table.schema, f)
        #     bq_ext_tbl_schema_str = f.getvalue()
        #     bq_ext_tbl_schema = json.loads(bq_ext_tbl_schema_str)
        #     logging.info(f"existing BQ table has {len(bq_ext_tbl_schema)} columns.")
        #     return bq_ext_tbl_schema

        # def get_duckdb_tbl_schema():
        #     from helpers import DuckDBToBigQueryMapper

        #     bq_mapper = DuckDBToBigQueryMapper()

        #     logging.info("analyzing current query schema...")
        #     describe_df = duck_conn.sql(query="DESCRIBE " + main_query).df()
        #     duckdb_tbl_schema = bq_mapper.duckdb_describe_to_bq_schema(describe_df)
        #     logging.info(f"current query has {len(duckdb_tbl_schema)} columns.")
        #     return duckdb_tbl_schema

        # def checking_schema():
        #     from google.cloud import storage
        #     from helpers import union_bigquery_schemas

        #     duckdb_tbl_schema = get_duckdb_tbl_schema()
        #     bq_ext_tbl_schema = get_bq_ext_tbl_schema()

            # client = bigquery.Client()
            # bq_mapper = DuckDBToBigQueryMapper()

            # # Get current schema from DuckDB query
            # logging.info("analyzing current query schema...")
            # describe_df = duck_conn.sql(query="DESCRIBE " + main_query).df()
            # current_query_schema = bq_mapper.duckdb_describe_to_bq_schema(describe_df)
            # logging.info(f"current query has {len(current_query_schema)} columns.")

            # schema_changed = False
            # current_schema = None

            # get existing schema from current BQ external table
            # try:
            #     # logging.info("get existing BQ schema...")
            #     # table_ref = client.dataset(bq_dataset, project=bq_project).table(psql_table)
            #     # table = client.get_table(table_ref)

            #     # f = io.StringIO("")
            #     # client.schema_to_json(table.schema, f)
            #     # existing_schema_str = f.getvalue()
            #     # old_schema = json.loads(existing_schema_str)
            #     # logging.info(f"existing BQ table has {len(old_schema)} columns.")

            #     # Create union schema (preserves all historical columns)
            #     logging.info("creating union schema (preserving all historical columns)...")
            #     union_result = union_bigquery_schemas(
            #         bq_ext_tbl_schema, duckdb_tbl_schema
            #     )
            #     current_schema = union_result["union_schema"]

            #     logging.info(f"union schema has {len(current_schema)} columns.")

            #     # Check for added columns
            #     if union_result["added_columns"]:
            #         schema_changed = True
            #         logger.info(
            #             f"🟢 NEW columns detected: {len(union_result['added_columns'])}"
            #         )
            #         for col_name, col_info in union_result["added_columns"].items():
            #             logger.info(f"   + {col_name} ({col_info['type']}, {col_info['mode']})")

            #     # Check for removed columns (automatically preserved in union)
            #     if union_result["removed_from_source"]:
            #         logger.info(
            #             f"🟡 Columns preserved (not in current query): {len(union_result['removed_from_source'])}"
            #         )
            #         for col_name, col_info in union_result["removed_from_source"].items():
            #             logger.info(
            #                 f"   - {col_name} ({col_info['type']}) - kept for historical data"
            #             )

            #     # Check for type conflicts
            #     if union_result["type_conflicts"]:
            #         schema_changed = True
            #         logger.error(
            #             f"🔴 TYPE CONFLICTS detected: {len(union_result['type_conflicts'])}"
            #         )
            #         for col_name, conflict in union_result["type_conflicts"].items():
            #             logger.error(
            #                 f"   ! {col_name}: {conflict['old_type']} → {conflict['new_type']}"
            #             )
            #             logger.error(
            #                 f"     → Keeping old type ({conflict['old_type']}) for compatibility"
            #             )

            #     if not schema_changed:
            #         logger.info("✅ No schema changes detected.")

            # except Exception as e:
            #     # Table doesn't exist yet - use current query schema
            #     logger.info(f"BigQuery table doesn't exist yet or error occurred: {e}")
            #     logger.info("Using current query schema as initial schema...")
            #     current_schema = duckdb_tbl_schema
            #     schema_changed = True

            # Save union schema to GCS
            # storage_client = storage.Client(project=os.getenv("GCP_PROJECT"))
            # gcs_bucket = storage_client.bucket(gcs_bucket_name)
            # schema_blob_path = f"{gcs_schema_path}/{schema_json_file}"
            # schema_blob = gcs_bucket.blob(schema_blob_path)

            # logger.info(f"saving union schema ({len(current_schema)} columns) to GCS...")
            # schema_json_str = json.dumps(current_schema, indent=2)
            # schema_blob.upload_from_string(schema_json_str, content_type="application/json")
            # logger.info(f"✅ schema saved to gs://{gcs_bucket_name}/{schema_blob_path}")

            # return schema_changed, current_schema

        def ingesting_parquet_to_gcs():
            ### Upload parquet to GCS using DuckDB
            logger.info(
                f"uploading parquet file: {parquet_filename} to {parquet_gcs_uri}..."
            )

            upload_parquet_to_gcs_query = f"""
                COPY {duckdb_tbl}
                TO '{parquet_gcs_uri}' (
                    FORMAT PARQUET,
                    COMPRESSION zstd
                );
            """
            # Create a copy query from the relation
            print(upload_parquet_to_gcs_query)

            duck_conn.sql(
                query=upload_parquet_to_gcs_query,
            )
            logger.info("✅ parquet file uploaded successfully.")

        get_main_data_df()
        # manage_schema()
        # upload()
