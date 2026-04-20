
# Airflow + Spark + Iceberg (concise)

Minimal, fast reference for running and developing the project.

## Stack
- Airflow 3.x (CeleryExecutor)
- Spark (via Spark Connect)
- Apache Iceberg on MinIO (s3a://analytics/warehouse)
- PostgreSQL (Airflow metadata / source)
- Redis (Celery broker)

## Quick commands

PowerShell / Windows:

```powershell
# Start full stack
docker-compose up -d

# Start with Flower
docker-compose --profile flower up -d

# Run Airflow CLI inside container
docker-compose --profile debug run airflow-cli <airflow-command>

# Rebuild after requirements/Dockerfile changes
docker-compose build && docker-compose up -d

# Stop
docker-compose down
```

Service URLs:
- Airflow UI: http://localhost:8080 (user: `tungnt`, password in `.env`)
- Flower: http://localhost:5555

## Required Airflow connections (set in UI: Admin → Connections)
- `spark_connect_prod` — Spark Connect (host:port, Generic)
- `minio_conn` — MinIO (AWS type)
- `postgres_chat_client` — source PostgreSQL

## DAGs (short)
- `crm_to_iceberg_spark.py` — nightly CRM → Iceberg (MERGE/dedup)
- `chat_memory_etl_iceberg.py` — Postgres → Iceberg via Spark
- `maintenance_crm_iceberg.py` — daily Iceberg maintenance
- `maintenance_crm_compaction.py` — weekly heavy compaction
- `maintenance_isocert_iceberg.py` — maintenance for isocert tables
- `spark_connect_example.py` — manual demo
- `hello_world_dag.py` — simple example

## Iceberg maintenance pattern
1. Set table properties (retention / compaction targets)
2. CALL iceberg.system.rewrite_data_files(...) (compact data files)
3. CALL iceberg.system.rewrite_manifests(...)
4. CALL iceberg.system.expire_snapshots(...)
5. Remove orphan files

## Adding dependencies
1. Edit `requirements.txt`
2. Rebuild the containers: `docker-compose build` then `docker-compose up -d`

Notes:
- Use Python 3.12 for parity with compiled bytecode present in repo.
- Check `docker-compose.yaml` and `config/airflow.cfg` for service ports and mounts.

## Environment variables (key)
- `AIRFLOW_UID`, `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`, `AIRFLOW_PROJ_DIR`

Questions or want this even shorter? I can trim further or add a one-page `CONTRIBUTING.md` and `LICENSE`.
