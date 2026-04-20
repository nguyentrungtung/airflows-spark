from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import logging
import json

def get_spark_session_from_connection(spark_conn_id, minio_conn_id):
    from pyspark.sql import SparkSession
    
    # 1. Lấy thông tin Spark Connect từ Airflow Connection
    spark_conn = BaseHook.get_connection(spark_conn_id)
    
    # Xử lý URI (đảm bảo format sc://host:port)
    host = spark_conn.host if spark_conn.host.startswith("sc://") else f"sc://{spark_conn.host}"
    remote_uri = f"{host}:{spark_conn.port}"
    
    # Lấy các config phụ trong ô Extra
    spark_extras = spark_conn.extra_dejson
    
    # 2. Lấy thông tin MinIO từ Airflow Connection
    minio_conn = BaseHook.get_connection(minio_conn_id)
    minio_endpoint = minio_conn.extra_dejson.get('endpoint_url', 'http://minio:9000')
    
    logging.info(f"Building Spark Session -> Remote: {remote_uri}")
    
    # 3. Build Session
    builder = SparkSession.builder.remote(remote_uri)
    
    # Apply config từ Spark Connection (Extra)
    for key, value in spark_extras.items():
        builder = builder.config(key, value)
        
    # Apply config MinIO/Iceberg (Lấy user/pass từ MinIO Connection)
    # Lưu ý: Nếu Server Spark đã config sẵn thì phần này có thể lược bỏ
    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_conn.login) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_conn.password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://analytics/warehouse")

    return builder.getOrCreate()

def run_job():
    spark = get_spark_session_from_connection("spark_connect_prod", "minio_conn")
    
    logging.info("Session created successfully via Airflow Connections!")
    
    # Test query
    try:
        df = spark.sql("SHOW NAMESPACES IN iceberg")
        df.show()
    except Exception as e:
        logging.error(f"Query failed: {e}")
        # Không raise lỗi để demo session creation thành công

default_args = {'owner': 'airflow', 'start_date': datetime(2023, 1, 1)}

with DAG('spark_connect_from_connection_ui', default_args=default_args, schedule=None, catchup=False) as dag:
    PythonOperator(task_id='test_spark_connection', python_callable=run_job)
