from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import re
import logging
import json

# --- CONFIGURATION ---
MINIO_CONN_ID = 'minio_conn'
SPARK_CONN_ID = 'spark_connect_prod'
BUCKET_NAME = 'analytics'
BASE_PATH = 'warehouse/default/crm/'
DEFAULT_PRIMARY_KEY = 'id'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def list_and_group_files(**kwargs):
    """
    Scans MinIO for files matching the execution date (yesterday by default)
    OR all files if full_scan=True, and groups them by target table.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    
    # Get Params
    params = kwargs.get('params', {})
    full_scan = params.get('full_scan', False)
    target_date = kwargs['ds'] # YYYY-MM-DD
    
    if full_scan:
        logging.info(f"FULL SCAN MODE ENABLED. Listing all keys in {BUCKET_NAME}/{BASE_PATH}...")
    else:
        logging.info(f"Scanning {BUCKET_NAME}/{BASE_PATH} for date: {target_date}")
    
    all_keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=BASE_PATH)
    
    if not all_keys:
        logging.info("No files found in base path.")
        return {}

    grouped_files = {}
    
    # Regex to find date pattern YYYY-MM-DD in path
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')

    for key in all_keys:
        if not key.endswith('.json'):
            continue
            
        # Filter logic
        if not full_scan and f"/{target_date}/" not in key:
            continue

        parts = key.split('/')
        
        # Robust Parsing Strategy: Find the date component using regex
        date_match = None
        date_idx = -1
        
        # Iterate backwards to find the date directory
        for i in range(len(parts)-1, -1, -1):
            if date_pattern.match(parts[i]):
                date_match = parts[i]
                date_idx = i
                break
        
        if date_idx >= 2: # valid structure: namespace/table/date
            try:
                table_raw = parts[date_idx - 1]     
                namespace_raw = parts[date_idx - 2] 
                
                # Normalize Namespace
                ns_norm = namespace_raw.replace('-', '_')
                
                # Normalize Table
                table_norm = table_raw.replace('-', '_')
                if not table_norm.lower().startswith('tbl_'):
                    table_norm = f"tbl_{table_norm}"
                
                # Construct Iceberg Namespace & Table Name
                iceberg_namespace = f"iceberg.default.stg_crm_v2.stg_{ns_norm}"
                full_table_name = f"{iceberg_namespace}.{table_norm}"
                
                if full_table_name not in grouped_files:
                    grouped_files[full_table_name] = []
                
                grouped_files[full_table_name].append(f"s3a://{BUCKET_NAME}/{key}")
            except Exception as e:
                logging.warning(f"Error parsing path {key}: {e}")
        else:
            logging.debug(f"Skipping key {key}: Could not identify namespace/table structure relative to date.")

    logging.info(f"Found {len(grouped_files)} tables to process.")
    for tbl, files in grouped_files.items():
        logging.info(f"  - {tbl}: {len(files)} files")

    return grouped_files

def process_crm_batches(**kwargs):
    from pyspark.sql import SparkSession
    from airflow.hooks.base import BaseHook
    
    grouped_files = kwargs['ti'].xcom_pull(task_ids='discover_files')
    if not grouped_files:
        logging.info("No files to process.")
        return

    # --- Spark Connection Setup ---
    # Try using Airflow 3.x Task SDK if available, otherwise fallback to BaseHook
    try:
        from airflow.sdk import Connection as SDKConnection
        spark_conn = SDKConnection.get(SPARK_CONN_ID)
        minio_conn = SDKConnection.get(MINIO_CONN_ID)
    except (ImportError, AttributeError):
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
        .getOrCreate()
        
    for table_name, file_paths in grouped_files.items():
        logging.info(f"Processing table: {table_name} ({len(file_paths)} files)")
        
        try:
            # 1. Read JSON
            df = spark.read.option("multiLine", "true").json(file_paths)
            
            if "_corrupt_record" in df.columns:
                df = df.drop("_corrupt_record")
            
            # --- DEDUPLICATION STEP ---
            # Fix for MERGE_CARDINALITY_VIOLATION: Ensure source has unique keys
            # Strategy: Keep the LATEST record based on timestamp if available
            merge_key = DEFAULT_PRIMARY_KEY
            if merge_key in df.columns:
                # 1. Identify Timestamp Column for ordering
                timestamp_col = None
                candidates = ['updated_at', 'modified_at', 'created_at', 'timestamp', 'time']
                for col_name in df.columns:
                    if col_name.lower() in candidates:
                        timestamp_col = col_name
                        break
                
                if timestamp_col:
                    logging.info(f"Deduplicating {table_name} using key '{merge_key}' and ordering by '{timestamp_col}'")
                    from pyspark.sql.functions import row_number, col
                    from pyspark.sql.window import Window
                    
                    # Window spec: Partition by ID, Order by Timestamp DESC
                    windowSpec = Window.partitionBy(merge_key).orderBy(col(timestamp_col).desc())
                    
                    df = df.withColumn("row_num", row_number().over(windowSpec)) \
                           .filter(col("row_num") == 1) \
                           .drop("row_num")
                else:
                    logging.warning(f"No timestamp column found in {table_name}. Deduplicating arbitrarily on '{merge_key}'.")
                    df = df.dropDuplicates([merge_key])

            # Temp View
            staging_view = f"staging_{table_name.replace('.', '_')}"
            df.createOrReplaceTempView(staging_view)
            
            # 2. Ensure Target Table Exists
            try:
                spark.sql(f"DESCRIBE {table_name}")
                table_exists = True
            except Exception:
                table_exists = False
                
            if not table_exists:
                logging.info(f"Table {table_name} does not exist. Creating...")
                ns = ".".join(table_name.split(".")[:-1])
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
                
                spark.sql(f"""
                    CREATE TABLE {table_name}
                    USING iceberg
                    TBLPROPERTIES (
                        'format-version'='2',
                        'write.metadata.delete-after-commit.enabled'='true',
                        'write.metadata.previous-versions-max'='2'
                    )
                    AS SELECT * FROM {staging_view} WHERE 1=0
                """ )
            
            # 3. MERGE Logic
            merge_key = DEFAULT_PRIMARY_KEY
            if merge_key not in df.columns:
                logging.warning(f"Column '{merge_key}' not found in {table_name}. Falling back to APPEND mode.")
                df.writeTo(table_name).append()
            else:
                logging.info(f"Performing MERGE on {table_name} using key: {merge_key}")
                spark.sql(f"""
                    MERGE INTO {table_name} t
                    USING {staging_view} s
                    ON t.{merge_key} = s.{merge_key}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """ )
                
            # 4. Maintenance (Optional - Kept here as this runs per TABLE, not per file, so it's safer)
            try:
                spark.sql(f"CALL iceberg.system.expire_snapshots(table => '{table_name}', retain_last => 2)")
            except Exception as e:
                logging.warning(f"Maintenance warning for {table_name}: {e}")
                
        except Exception as e:
            logging.error(f"Failed to process table {table_name}: {e}")
            raise e

    logging.info("All tables processed.")

with DAG(
    'crm_to_iceberg_spark',
    default_args=default_args,
    schedule='15 0 * * *',
    catchup=False,
    tags=['etl', 'crm', 'spark', 'iceberg'],
    params={
        "full_scan": Param(False, type="boolean", description="If True, scan ALL files in history instead of just today's.")
    }
) as dag:
    
    t1 = PythonOperator(
        task_id='discover_files',
        python_callable=list_and_group_files,
    )
    
    t2 = PythonOperator(
        task_id='process_crm_tables',
        python_callable=process_crm_batches,
    )
    
    t1 >> t2