import duckdb
from dotenv import load_dotenv
import os

env_vars = load_dotenv("../../.env.shared")

with duckdb.connect() as dconn:
    install_extensions = """
        INSTALL ducklake;
        INSTALL httpfs;
        INSTALL postgres;
    """

    create_gcs_secret = f"""
        CREATE SECRET (
            TYPE gcs,
            KEY_ID '{os.getenv("GCS_HMAC_ACCESS_KEY")}',
            SECRET '{os.getenv("GCS_HMAC_ACCESS_KEY_SECRET")}',
            URL_STYLE path
        );
    """

    load_extensions = """
        LOAD postgres;
        LOAD httpfs;
    """

    attach_postgres = f"""
        ATTACH 
        'ducklake:postgres:dbname={os.getenv("PSQL_NAME")} \
        host={os.getenv("PSQL_HOST")} \
        user={os.getenv("PSQL_USERNAME")} \
        password={os.getenv("PSQL_PASSWORD")}'
        AS psql_ducklake (DATA_PATH 'gs://dev-duckdb-sink/ducklake_metadata/');

        USE psql_ducklake;
    """

    dconn.sql(create_gcs_secret)
    dconn.sql(install_extensions)
    dconn.sql(load_extensions)
    dconn.sql(attach_postgres)
    dconn.sql("SELECT * FROM duckdb_tables ;").show()
