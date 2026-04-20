from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging

# --- CONFIGURATION ---
SPARK_CONN_ID = 'spark_connect_prod'
MINIO_CONN_ID = 'minio_conn'
# Root namespace where all CRM services reside
ROOT_NAMESPACE = 'iceberg.default.stg_crm_v2'
# Target file size: 128MB (in bytes)
TARGET_FILE_SIZE = 128 * 1024 * 1024 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_spark_session():
    from pyspark.sql import SparkSession
    
    spark_conn = BaseHook.get_connection(SPARK_CONN_ID)
    minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
    minio_endpoint = minio_conn.extra_dejson.get('endpoint_url')
    
    host = spark_conn.host
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

def discover_all_tables(**kwargs):
    """
    Connects to Spark, lists all sub-namespaces under the root,
    and finds all tables within them.
    Returns a list of full table names.
    """
    spark = get_spark_session()
    all_tables = []
    
    try:
        # 1. List namespaces under iceberg.default.stg_crm_v2
        # Expected output rows: [namespace] -> e.g., 'stg_iss_365_accountant'
        # Note: 'SHOW NAMESPACES' usually returns a generic Row
        logging.info(f"Listing namespaces in {ROOT_NAMESPACE}")
        namespaces_df = spark.sql(f"SHOW NAMESPACES IN {ROOT_NAMESPACE}")
        namespaces = [row[0] for row in namespaces_df.collect()]
        
        for ns in namespaces:
            full_ns = f"{ROOT_NAMESPACE}.{ns}"
            logging.info(f"Scanning namespace: {full_ns}")
            
            # 2. List tables in each namespace
            try:
                tables_df = spark.sql(f"SHOW TABLES IN {full_ns}")
                # Expected columns: namespace, tableName, isTemporary
                rows = tables_df.collect()
                for row in rows:
                    # Depending on Spark version/catalog, column names might vary.
                    # Usually row.tableName is safe.
                    tbl_name = row['tableName']
                    full_table_name = f"{full_ns}.{tbl_name}"
                    all_tables.append(full_table_name)
            except Exception as e:
                logging.warning(f"Could not list tables in {full_ns}: {e}")

    except Exception as e:
        logging.error(f"Failed to discover tables: {e}")
        raise e
    
    logging.info(f"Found {len(all_tables)} tables to compact.")
    return all_tables

def compact_table(table_name):
    """
    Runs rewrite_data_files and remove_orphan_files for a single table.
    """
    spark = get_spark_session()
    logging.info(f"Starting compaction for: {table_name}")
    
    try:
        # 1. Rewrite Data Files (Compaction)
        # Bins small files into target-file-size-bytes (128MB)
        result = spark.sql(f"""
            CALL iceberg.system.rewrite_data_files(
                table => '{table_name}', 
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE}',
                    'min-input-files', '2' 
                )
            )
        """).collect()
        
        # Log results (number of files rewritten)
        # Result schema usually: rewritten_data_files_count, added_data_files_count
        logging.info(f"Rewrite result for {table_name}: {result}")
        
        # 2. Remove Orphan Files (Deep Cleanup)
        # This removes files on MinIO that are no longer referenced by any valid snapshot.
        # Should be run periodically to free up actual disk space.
        logging.info(f"Removing orphan files for: {table_name}")
        spark.sql(f"CALL iceberg.system.remove_orphan_files(table => '{table_name}')")
        
        logging.info(f"Maintenance complete for {table_name}")
        
    except Exception as e:
        logging.error(f"Maintenance failed for {table_name}: {e}")
        # We raise exception to mark task as failed in Airflow
        raise e

with DAG(
    'maintenance_crm_compaction',
    default_args=default_args,
    # Run weekly: Every Sunday at 03:00 AM
    schedule='0 3 * * 0',
    catchup=False,
    tags=['maintenance', 'iceberg', 'compaction']
) as dag:
    
    # Task 1: Find all tables
    discover_task = PythonOperator(
        task_id='discover_all_crm_tables',
        python_callable=discover_all_tables,
    )
    
    # Task 2: Map compaction task over each table
    compact_task = PythonOperator.partial(
        task_id='compact_table',
        python_callable=compact_table,
        map_index_template="{{ task.op_args[0] }}"
    ).expand(op_args=discover_task.output)
