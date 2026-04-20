from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging

# --- CONFIGURATION ---
MINIO_CONN_ID = 'minio_conn'
SPARK_CONN_ID = 'spark_connect_prod'

# [USER CONFIGURATION REQUIRED]
# Replace 'your_database_name' with the value of 'isocert.mysql.database' from your properties file.
# Based on MinIO path: analytics/warehouse/default/isocert_visits/sql_isocert_org_/tbl_isocert_visits
ISOCERT_DB_NAME = 'sql_isocert_org_' 

# Construct the full table name based on the Java code logic:
# namespace() -> "default.isocert_visits"
# targetNs -> "iceberg.default.isocert_visits.{ISOCERT_DB_NAME}"
# targetTable -> "{targetNs}.tbl_isocert_visits"
TARGET_TABLE = f"iceberg.default.isocert_visits.{ISOCERT_DB_NAME}.tbl_isocert_visits"

TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128MB

def perform_iceberg_maintenance(**kwargs):
    from pyspark.sql import SparkSession
    
    # 1. Setup Spark Connection
    spark_conn = BaseHook.get_connection(SPARK_CONN_ID)
    minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
    minio_endpoint = minio_conn.extra_dejson.get('endpoint_url')
    
    host = spark_conn.host
    if not host.startswith("sc://"):
        host = f"sc://{host}"
    
    if spark_conn.port and ":" not in host.replace("sc://", ""):
        host = f"{host}:{spark_conn.port}"

    logging.info(f"Connecting to Spark: {host}")
    spark = SparkSession.builder.remote(host) \
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

    # 2. Maintain Table
    table_name = TARGET_TABLE
    logging.info(f"Starting maintenance for table: {table_name}")
    
    try:
        # 0. Check if table exists
        if not spark.catalog.tableExists(table_name):
            logging.warning(f"Table {table_name} does not exist. Skipping maintenance.")
            return

        # A. Set Table Properties (To enforce metadata limits on future writes)
        logging.info(f"  - Setting table properties for {table_name} (retain 2 versions)...")
        spark.sql(f"""
            ALTER TABLE {table_name} SET TBLPROPERTIES (
                'write.metadata.delete-after-commit.enabled'='true',
                'write.metadata.previous-versions-max'='2'
            )
        """)

        # B. Rewrite Data Files (Compaction/Slimming)
        # Run this FIRST to compact small files. This creates a new snapshot.
        logging.info(f"  - Rewriting data files for {table_name} (Target: 128MB)...")
        spark.sql(f"""
            CALL iceberg.system.rewrite_data_files(
                table => '{table_name}', 
                options => map('target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}')
            )
        """)

        # C. Rewrite Manifests (Optimize Metadata/Avro)
        # Run this SECOND to compact manifest files. This creates another new snapshot.
        # The old fragmented avro files are now part of the 'old' history.
        logging.info(f"  - Rewriting manifests for {table_name}...")
        spark.sql(f"CALL iceberg.system.rewrite_manifests(table => '{table_name}')")

        # D. Expire Snapshots (Clean Metadata & Old Avro Files)
        # CRITICAL: Run this AFTER rewrites. 
        # It will expire the snapshots that pointed to the old (pre-rewrite) data/manifest files,
        # triggering the physical deletion of those 'garbage' avro files.
        logging.info(f"  - Expiring snapshots for {table_name} (Retain 2)...")
        spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table => '{table_name}', 
                retain_last => 2, 
                older_than => TIMESTAMP '{datetime.now()}'
            )
        """)
        
        # E. Remove Orphan Files (Clean Unreferenced Files)
        logging.info(f"  - Removing orphan files for {table_name}...")
        spark.sql(f"CALL iceberg.system.remove_orphan_files(table => '{table_name}')")
        
        logging.info(f"Successfully maintained {table_name}.")
        
    except Exception as e:
        logging.error(f"Error maintaining {table_name}: {e}")
        raise e

# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'maintenance_isocert_iceberg',
    default_args=default_args,
    description='Maintenance for Isocert Visits Iceberg table (Retain 2 snapshots, clean orphans).',
    schedule='0 2 * * *',  # Run daily at 02:00 AM (after the daily ETL usually)
    catchup=False,
    tags=['maintenance', 'iceberg', 'isocert']
) as dag:
    
    maintenance_task = PythonOperator(
        task_id='perform_maintenance',
        python_callable=perform_iceberg_maintenance
    )
