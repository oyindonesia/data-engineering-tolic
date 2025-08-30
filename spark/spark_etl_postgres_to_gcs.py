import logging
from datetime import datetime

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Putting everything (i.e. configs, env vars, functions) to this script
# Easier to debug, everything in one place (no need to jump to other files)

psql_schema = "public"
# psql_table = "b2x_inquiry_api_tx"
# psql_table = "b2x_payment_routing_disburse_trx"
# psql_table = "b2x_user_profiles"
# psql_table = "b2x_hold_balance_history"
# psql_table = "b2x_users"
# psql_table = "b2x_user_status_history"
# psql_table = "b2x_balance_adjustment_requests"
# psql_table = "b2x_va_tx_history"
# psql_table = "acceptance_bank_transfer_transaction"
# psql_table = "tx_bank_transfer"
# psql_table = "b2x_payment_routing_trx"
# psql_table = "qris_transaction"
# psql_table = "b2x_admin_fee_detail"
psql_table = "b2x_checkout_transaction"

# gcs_bucket = "dev-oy-bi-raw-hub"
gcs_bucket = "oy-bi-raw-hub"
# gcs_bucket = f"oy-bi-raw-hub/_backup/{psql_table}"

POSTGRES_CONFIG = {
    "user": "monitoring",
    "password": "S&Fewc92vSSA#7GFXCb9mv&qI",
    "host": "paywallet-postgres.main.danarapay.internal",
    # "host": "10.135.4.86",
    "port": "5432",
    "database": "postgres",
}

# Create SparkSession
logger.info("Creating Spark session...")
spark = (
    SparkSession.builder.appName("Postgres Data Loader")
    .config(
        "spark.jars",
        "jars/postgresql-42.7.5.jar,"
        "jars/gcsio-3.0.7.jar,"
        "jars/gcs-connector-hadoop3-latest.jar,",
        # "jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar",
    )
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.cores", "2")
    .config("spark.tasks.cpus", "1")
    .config("spark.default.parallelism", "2")
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    .getOrCreate()
)

spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set(
    "google.cloud.auth.service.account.json.keyfile",
    "/home/giovannor/Documents/creds/sakey-de.json",
)

jdbc_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

logger.info(f"JDBC URL: {jdbc_url}")


# Define the query
# start_date = (pendulum.now("Asia/Jakarta") - timedelta(days=2)).strftime(
#     "%Y-%m-%d 17:00:00"
# )
# end_date = (pendulum.now("Asia/Jakarta") - timedelta(days=1)).strftime(
#     "%Y-%m-%d 17:00:00"
# )

start_date = datetime(2025, 3, 20).strftime("%Y-%m-%d 10:01:02")
end_date = datetime(2025, 6, 13).strftime("%Y-%m-%d 04:10:12")

# etl_date = pendulum.now("Asia/Jakarta").strftime("%Y-%m-%d")
etl_date = datetime(2025, 6, 14).strftime("%Y-%m-%d")


# Load data from Postgres
try:
    query_indexes = f"""
    select 
        min(id) as min_id
        ,max(id) as max_id
    FROM
        {psql_schema}.{psql_table}
    """

    final_query_indexes = f"({query_indexes}) as q"

    logger.info(f"Executing query to get indexes: {query_indexes}")

    df_index = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", jdbc_url)
        .option("user", POSTGRES_CONFIG["user"])
        .option("password", POSTGRES_CONFIG["password"])
        .option("dbtable", final_query_indexes)
        .option("fetchsize", 2)
        .option("isolationLevel", "READ_UNCOMMITTED")
        .load()
    )

    min_id = df_index.select("min_id").head()[0]
    max_id = df_index.select("max_id").head()[0]

    # and created < '{end_date}'
    query = f"""
    SELECT distinct *
    FROM {psql_schema}.{psql_table}
    WHERE
    true
    and id >= '{min_id}' and id <= '{max_id}'
    and created >= '{start_date}' and created < '{end_date}'

    union all

    SELECT distinct *
    FROM {psql_schema}.{psql_table}
    WHERE
    true
    and id >= '{min_id}' and id <= '{max_id}'
    and last_updated >= '{start_date}' and created < '{end_date}'
    """
    # and last_updated < '{end_date}'

    # query = f"""
    # SELECT *
    # FROM {psql_schema}.{psql_table}
    # WHERE
    # true
    # and id >= '{min_id}' and id <= '{max_id}'
    # """

    final_query = f"({query}) as q"

    logger.info(f"Executing query to get data: {query}")

    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", jdbc_url)
        .option("user", POSTGRES_CONFIG["user"])
        .option("password", POSTGRES_CONFIG["password"])
        .option("dbtable", final_query)
        .option("fetchsize", 10000)
        # .option("numPartitions", "10")
        # .option("partitionColumn", "id")
        # .option("lowerBound", f"{min_id}")
        # .option("upperBound", f"{max_id}")
        .option("isolationLevel", "READ_UNCOMMITTED")
        .load()
    )

    # df_filtered = df.filter(
    #     ((df.created >= start_date) & (df.created < end_date))
    #     | ((df.last_updated >= start_date) & (df.created < end_date))
    # )

    # df_filtered = df.filter((df.created < end_date) | (df.last_updated < end_date))

    # df_filtered.explain()

    # df_filtered.show()

    # print(f"Result count: {df_filtered.count()}")
    print(f"Result count: {df.count()}")

    # df_filtered.write.mode("append").parquet(
    #     f"gs://{gcs_bucket}/{psql_table}/dt={etl_date}"
    # )

    df.write.option("compression", "zstd").mode("append").parquet(
        f"gs://{gcs_bucket}/{psql_table}/dt={etl_date}"
    )

    print(
        f"Dataframe converted to parquet and uploaded to gcs gs://{gcs_bucket}/{psql_table}/dt={etl_date}"
    )

except Exception as e:
    logger.error(f"Error: {str(e)}")

finally:
    logger.info("Stopping Spark session...")
    spark.stop()
