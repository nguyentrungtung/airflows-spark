from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

# --- CONFIGURATION ---
POSTGRES_CONN_ID = 'postgres_chat_client'
MINIO_CONN_ID = 'minio_conn'
SPARK_CONN_ID = 'spark_connect_prod'

SOURCE_TABLE = 'public.spring_ai_chat_memory'
ICEBERG_TABLE = 'iceberg.default.stg_chat.spring_ai_chat_memory'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def etl_direct_postgres_to_iceberg(**kwargs):
    """
    Reads directly from Postgres using Spark JDBC and writes to Iceberg.
    No intermediate JSON files.
    """
    from pyspark.sql import SparkSession
    
    # 1. Get Connections
    # Spark & MinIO (for Iceberg storage)
    try:
        from airflow.sdk import Connection as SDKConnection
        spark_conn = SDKConnection.get(SPARK_CONN_ID)
        minio_conn = SDKConnection.get(MINIO_CONN_ID)
    except (ImportError, AttributeError):
        spark_conn = BaseHook.get_connection(SPARK_CONN_ID)
        minio_conn = BaseHook.get_connection(MINIO_CONN_ID)

    # Postgres Credentials
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_conn_params = pg_hook.get_connection(POSTGRES_CONN_ID)
    
    # Resolve Spark Host
    minio_endpoint = minio_conn.extra_dejson.get('endpoint_url')
    host = spark_conn.host
    if not host.startswith("sc://"):
        host = f"sc://{host}"
    if spark_conn.port and ":" not in host.replace("sc://", ""):
        host = f"{host}:{spark_conn.port}"

    logging.info(f"Connecting to Spark: {host}")
    
    # 2. Build Spark Session
    # Note: We add the Postgres JDBC driver package so Spark can talk to DB
    spark = SparkSession.builder.remote(host) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_conn.login) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_conn.password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://analytics/warehouse") \
        .getOrCreate()

    # 3. Read from Postgres (Direct JDBC)
    # Important: 'host' for Spark (inside Docker) is the service name 'postgres'
    # Check if the Airflow connection host is 'postgres' or 'localhost' or 'host.docker.internal'.
    # We will prefer the service name 'postgres' if running in the same compose stack.
    # Otherwise, fallback to the connection's host.
    
    db_host = pg_conn_params.host
    # Heuristic: If connection says localhost, but we are in docker, we might need 'postgres'
    # But often users configure the connection as 'postgres' in Airflow directly.
    # We will trust the Airflow Connection first, but if it fails, one might need to adjust the Connection in UI.
    
    jdbc_url = f"jdbc:postgresql://{db_host}:{pg_conn_params.port}/{pg_conn_params.schema}"
    
    logging.info(f"Reading from JDBC URL: {jdbc_url}")
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", SOURCE_TABLE) \
        .option("user", pg_conn_params.login) \
        .option("password", pg_conn_params.password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # 4. Write to Iceberg (Overwrite)
    logging.info(f"Overwriting Iceberg table: {ICEBERG_TABLE}")
    
    # Ensure Namespace exists
    ns = ".".join(ICEBERG_TABLE.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")

    df.createOrReplaceTempView("source_data")
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {ICEBERG_TABLE} 
        USING iceberg 
        TBLPROPERTIES (
            'format-version'='2',
            'write.metadata.delete-after-commit.enabled'='true',
            'write.metadata.previous-versions-max'='2'
        )
        AS SELECT * FROM source_data
    """)
    
    logging.info("Load complete.")

    # 5. Maintenance
    try:
        logging.info("Running maintenance...")
        spark.sql(f"CALL iceberg.system.expire_snapshots(table => '{ICEBERG_TABLE}', retain_last => 2, older_than => TIMESTAMP '{datetime.now()}')")
        spark.sql(f"CALL iceberg.system.remove_orphan_files(table => '{ICEBERG_TABLE}')")
        logging.info("Maintenance complete.")
    except Exception as e:
        logging.warning(f"Maintenance warning: {e}")

with DAG(
    'chat_memory_etl_iceberg',
    default_args=default_args,
    schedule='15 1 * * *',
    catchup=False,
    tags=['etl', 'chat', 'spark', 'iceberg', 'direct-jdbc'],
    description='Unified pipeline: Direct JDBC Extract (Postgres) -> Spark Iceberg Load -> Maintenance'
) as dag:
    
    t1 = PythonOperator(
        task_id='etl_direct_postgres_to_iceberg',
        python_callable=etl_direct_postgres_to_iceberg,
    )
    
    t1