import os

import duckdb
from dotenv import load_dotenv

load_dotenv(".env.shared")

duck_conn = duckdb.connect()

install_psql_ext = f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH
        '{os.getenv("PSQL_CONN")}'
        AS pg (TYPE POSTGRES, READ_ONLY);
    """

install_httpfs_ext = """
        INSTALL httpfs;
        LOAD httpfs;
"""

create_gcs_secret = """
        CREATE SECRET (
            TYPE gcs,
            KEY_ID 'insert_key_id',
            SECRET 'insert_secret',
            URL_STYLE path
        );
    """

duck_conn.sql(install_psql_ext)
duck_conn.sql(install_httpfs_ext)

query_index = """
    SELECT 
        max(id) as max_id,
        min(id) as min_id
    from 
        pg.public.tx_bank_transfer
    where
        true
        and created >= '2025-08-24 17:00:00'
        and created <= '2025-08-26 17:00:00'
"""
print("data preview:")
print(duck_conn.sql(query=query_index).show())

indexes_df = duck_conn.sql(query=query_index).df()

query_data = """
    SELECT 
        *
    from 
        pg.public.tx_bank_transfer
    where
        id >= $min_id
        and id <= $max_id
        and created >= '2025-08-25 17:00:00'
        and created <= '2025-08-26 17:00:00'
"""

psql_query = duck_conn.sql(
    query=query_data,
    params={
        "min_id": indexes_df["min_id"].iloc[0],
        "max_id": indexes_df["max_id"].iloc[0],
    },
)
print("data preview:")
print(psql_query.show())

query_parquet_gcs = f"""
    COPY (
    {query_data}
    )
    TO 'gs://dev-duckdb-sink/tx_bank_transfer' (
        FORMAT parquet,
        COMPRESSION zstd,
        PARTITION_BY (last_updated)
    );
"""
print("Copying data to parquet...")
print("executing query:")
print(
    duck_conn.sql(
        query_parquet_gcs,
        params={
            "min_id": indexes_df["min_id"].iloc[0],
            "max_id": indexes_df["max_id"].iloc[0],
        },
    ).show()
)

duck_conn.sql(
    query=query_parquet_gcs,
    params={
        "min_id": indexes_df["min_id"].iloc[0],
        "max_id": indexes_df["max_id"].iloc[0],
    },
)
