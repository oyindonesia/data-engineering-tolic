import duckdb

with duckdb.connect() as duck_conn:
    query_params = {
        "gcs_hmac_access_key": "a",
        "gcs_hmac_access_key_secret": "123",
    }

    secrets_query = """
        CREATE SECRET (
            TYPE gcs,
            KEY_ID $gcs_hmac_access_key,
            SECRET $gcs_hmac_access_key_secret
        );
    """

    create_secrets = duck_conn.sql(
        query=secrets_query,
        params=query_params,
    )

    create_secrets.show()
