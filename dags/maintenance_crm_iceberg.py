from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import traceback

# --- CONFIGURATION ---
MINIO_CONN_ID = 'minio_conn'
SPARK_CONN_ID = 'spark_connect_prod'
BASE_CATALOG = 'iceberg'
# CHANGED: Target the new V2 Unpartitioned namespace
ROOT_NAMESPACE = 'default.stg_crm_v2' 
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128MB

def get_spark_session():
    import os
    from pyspark.sql import SparkSession
    spark_conn = BaseHook.get_connection(SPARK_CONN_ID)
    minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
    
    # 1. MinIO Endpoint Extraction
    # Standard Airflow AWS Hook uses extra -> endpoint_url
    minio_endpoint = minio_conn.extra_dejson.get('endpoint_url')
    # Fallback: check if user put it in the 'Host' field (e.g., Generic connection type)
    if not minio_endpoint and minio_conn.host:
        minio_endpoint = minio_conn.host
        # Ensure minio_endpoint is a string before checking startswith
        if not isinstance(minio_endpoint, str):
            minio_endpoint = str(minio_endpoint)

        # If host doesn't start with http, assume it needs it (common mistake)
        if not minio_endpoint.startswith("http"):
             minio_endpoint = f"http://{minio_endpoint}"
             
    if not minio_endpoint:
        raise ValueError(f"MinIO Connection '{MINIO_CONN_ID}' is missing an Endpoint URL. "
                         f"Please set it in the 'Extra' JSON as 'endpoint_url' or in the 'Host' field.")

    # 2. Spark Host Extraction
    host = spark_conn.host
    if not host:
        # Fallback: check 'host' in extra (uncommon but possible)
        host = spark_conn.extra_dejson.get('host')

    if not host:
         raise ValueError(f"Spark Connection '{SPARK_CONN_ID}' is missing a Host. "
                          f"Please set the Spark Connect URI (e.g., sc://...) in the 'Host' field.")

    if not host.startswith("sc://"):
        host = f"sc://{host}"
    
    if spark_conn.port and ":" not in host.replace("sc://", ""):
        host = f"{host}:{spark_conn.port}"

    return SparkSession.builder.remote(host) \
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

def discover_and_maintain_crm_tables(**kwargs):
    spark = get_spark_session()
    
    # 1. Discover Sub-Namespaces (Services)
    logging.info(f"Scanning root namespace: {BASE_CATALOG}.{ROOT_NAMESPACE}")
    
    try:
        # SHOW NAMESPACES IN iceberg.default.stg_crm_v2
        namespaces_df = spark.sql(f"SHOW NAMESPACES IN {BASE_CATALOG}.{ROOT_NAMESPACE}")
        namespaces = [row[0] for row in namespaces_df.collect()]
    except Exception as e:
        logging.warning(f"Could not list namespaces in {ROOT_NAMESPACE}. It might not exist yet. Error: {e}")
        return

    total_tables_maintained = 0

    for ns in namespaces:
        # Handle potential fully qualified namespace return
        if ns.startswith(ROOT_NAMESPACE):
            full_ns = ns
        else:
            full_ns = f"{ROOT_NAMESPACE}.{ns}"
            
        logging.info(f"Checking namespace: {full_ns}")
        
        try:
            # 2. List Tables in Sub-Namespace
            tables_df = spark.sql(f"SHOW TABLES IN {BASE_CATALOG}.{full_ns}")
            tables = [row['tableName'] for row in tables_df.collect()]
            
            for table_name in tables:
                full_table_ref = f"{BASE_CATALOG}.{full_ns}.{table_name}"
                maintain_single_table(spark, full_table_ref)
                total_tables_maintained += 1
                
        except Exception as e:
            logging.error(f"Error processing namespace {full_ns}: {e}")

    logging.info(f"Maintenance complete. Total tables processed: {total_tables_maintained}")

import subprocess
import os

# ... (Previous imports and config)

def remove_orphans_via_local_spark(table_ref):
    """
    Invokes a separate Python script to run local Spark (JVM) operations 
    for orphan file removal, isolating it from the main Spark Connect session.
    """
    minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
    minio_endpoint = minio_conn.extra_dejson.get('endpoint_url')
    if not minio_endpoint and minio_conn.host:
        minio_endpoint = minio_conn.host
        # Ensure minio_endpoint is a string
        if not isinstance(minio_endpoint, str):
            minio_endpoint = str(minio_endpoint)
            
        if not minio_endpoint.startswith("http"):
             minio_endpoint = f"http://{minio_endpoint}"
             
    if not minio_endpoint:
        raise ValueError(f"MinIO Connection '{MINIO_CONN_ID}' is missing an Endpoint URL.")

    script_path = "/opt/airflow/dags/scripts/iceberg_orphan_cleaner.py"
    
    cmd = [
        "python", script_path,
        "--table", table_ref,
        "--endpoint", minio_endpoint,
        "--access-key", minio_conn.login,
        "--secret-key", minio_conn.password
    ]
    
    logging.info(f"    -> Running external script: {' '.join(cmd)}")
    
    # Prepare environment: Remove Spark Connect variables to force Classic mode
    env_copy = os.environ.copy()
    env_copy.pop("SPARK_CONNECT_URI", None)
    env_copy.pop("SPARK_REMOTE", None)
    env_copy.pop("SPARK_CONNECT_MODE_ENABLED", None)
    
    # Run the script in a subprocess. 
    # 'check=True' raises CalledProcessError if return code is non-zero.
    # capture_output=True allows us to log stdout/stderr.
    try:
        result = subprocess.run(
            cmd, 
            check=True, 
            capture_output=True, 
            text=True,
            env=env_copy
        )
        logging.info(f"    -> External Script Output:\n{result.stdout}")
        logging.info(f"    -> Orphan removal success.")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"    -> External Script FAILED with return code {e.returncode}")
        logging.error(f"    -> STDOUT:\n{e.stdout}")
        logging.error(f"    -> STDERR:\n{e.stderr}")
        raise e

def maintain_single_table(spark, table_ref):
    logging.info(f"--- Maintaining {table_ref} ---")
    try:
        # 0. Set Table Properties for aggressive metadata cleanup
        logging.info("  [0/5] Setting cleanup properties (delete-after-commit=true)...")
        spark.sql(f"""
            ALTER TABLE {table_ref} SET TBLPROPERTIES (
                'write.metadata.delete-after-commit.enabled'='true',
                'write.metadata.previous-versions-max'='2'
            )
        """)

        # 1. Rewrite Data Files (Compaction) - FORCE REWRITE for UNPARTITIONED
        # Moved to First Step so subsequent expire_snapshots cleans up the old small files
        logging.info("  [1/5] Rewriting data files (Compacting)...")
        # Added 'rewrite-all': 'true' to force compaction of all small files into 128MB chunks
        rewrite_df = spark.sql(f"""
            CALL {BASE_CATALOG}.system.rewrite_data_files(
                table => '{table_ref}', 
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                    'rewrite-all', 'true'  
                )
            )
        """)
        rewrite_rows = rewrite_df.collect()
        if rewrite_rows:
            res = rewrite_rows[0]
            logging.info(f"    -> Rewritten (Input) Files: {res['rewritten_data_files_count']}")
            logging.info(f"    -> Added (Compacted) Files: {res['added_data_files_count']}")

        # 2. Rewrite Manifests
        logging.info("  [2/5] Rewriting manifests (Compacting metadata)...")
        manifest_df = spark.sql(f"CALL {BASE_CATALOG}.system.rewrite_manifests(table => '{table_ref}')")
        manifest_rows = manifest_df.collect()
        if manifest_rows:
            res = manifest_rows[0]
            logging.info(f"    -> Rewritten Manifests: {res['rewritten_manifests_count']}")
            logging.info(f"    -> Added Manifests: {res['added_manifests_count']}")

        # 3. Expire Snapshots
        # Moved after compaction to ensure pre-compaction history is removed
        logging.info("  [3/5] Expiring snapshots...")
        expire_df = spark.sql(f"""
            CALL {BASE_CATALOG}.system.expire_snapshots(
                table => '{table_ref}', 
                retain_last => 1, 
                older_than => TIMESTAMP '{datetime.now()}'
            )
        """)
        expire_rows = expire_df.collect()
        if expire_rows:
            res = expire_rows[0]
            logging.info(f"    -> Deleted Data Files: {res['deleted_data_files_count']}")
            logging.info(f"    -> Deleted Manifest Files: {res['deleted_manifest_files_count']}")
            logging.info(f"    -> Deleted Manifest Lists: {res['deleted_manifest_lists_count']}")

        # 4. Remove Orphan Files (Hybrid: Use Local Spark)
        logging.info("  [4/5] Removing orphan files (Hybrid Mode)...")
        try:
            remove_orphans_via_local_spark(table_ref)
        except Exception as local_e:
            logging.error(f"    -> Local orphan removal failed: {local_e}")
            logging.warning("    -> Falling back to standard SQL (24h safety check)...")
            # Fallback to standard SQL if local fails
            orphan_df = spark.sql(f"""
                CALL {BASE_CATALOG}.system.remove_orphan_files(
                    table => '{table_ref}',
                    older_than => TIMESTAMP '{datetime.now() - timedelta(days=1)}'
                )
            """)
            logging.info(f"    -> Fallback SQL completed.")

        logging.info(f"SUCCESS: {table_ref} maintenance finished.")
        
    except Exception as e:
        logging.error(f"FAILED: {table_ref} - {e}")

        logging.info(f"SUCCESS: {table_ref} maintenance finished.")
        
    except Exception as e:
        logging.error(f"FAILED: {table_ref} - {e}")

# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0, 
}

with DAG(
    'maintenance_crm_iceberg_unpartitioned',
    default_args=default_args,
    description='Dynamic maintenance for Unpartitioned CRM tables (v2) - Force compaction to 128MB',
    schedule='0 3 * * *',  # Run daily at 03:00 AM (After CrmToStgUnpartitioned runs at 00:15)
    catchup=False,
    tags=['maintenance', 'iceberg', 'crm', 'unpartitioned']
) as dag:
    
    maintenance_task = PythonOperator(
        task_id='discover_and_maintain',
        python_callable=discover_and_maintain_crm_tables
    )