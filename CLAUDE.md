# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Stack Overview

Local data engineering platform:
- **Airflow 3.1.5** (CeleryExecutor) — orchestration
- **Apache Spark** via Spark Connect (remote) — data processing
- **Apache Iceberg** — table format on MinIO
- **MinIO** — S3-compatible data lake (`s3a://analytics/warehouse`)
- **PostgreSQL 16** — Airflow metadata store + data source
- **Redis** — Celery broker

## Commands

```bash
# Start full stack
docker-compose up -d

# Start with Flower (Celery monitor at localhost:5555)
docker-compose --profile flower up -d

# Run Airflow CLI commands
docker-compose --profile debug run airflow-cli <airflow-command>

# Rebuild after changing requirements.txt or Dockerfile
docker-compose build && docker-compose up -d

# Stop
docker-compose down
```

**Service URLs:**
- Airflow UI: http://localhost:8080 (user: `tungnt`, pass in `.env`)
- Flower: http://localhost:5555
- MinIO Console: check `docker-compose.yaml` for exposed port

## Architecture

All DAGs use `PythonOperator` and interact with Spark via **Spark Connect** (not embedded Spark). Spark sessions are built using credentials from Airflow connections (`BaseHook.get_connection()`).

### Airflow Connections required (configure via UI: Admin > Connections)
- `spark_connect_prod` — Spark Connect server (Generic type, port 15002)
- `minio_conn` — MinIO (AWS type, endpoint `http://minio:9000`)
- `postgres_chat_client` — source PostgreSQL database

### DAG patterns

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `crm_to_iceberg_spark.py` | `15 0 * * *` | MinIO JSON → Iceberg via Spark, with MERGE + dedup by window |
| `chat_memory_etl_iceberg.py` | `15 1 * * *` | Postgres JDBC → Iceberg via Spark, direct MERGE |
| `maintenance_crm_iceberg.py` | `0 3 * * *` | Daily Iceberg compaction for `stg_crm_v2` namespace |
| `maintenance_isocert_iceberg.py` | `0 2 * * *` | Daily maintenance for `isocert_visits` table |
| `maintenance_crm_compaction.py` | `0 3 * * 0` | Weekly heavy compaction (Sunday), dynamic task mapping |
| `spark_connect_example.py` | None | Manual demo of Spark Connect session |
| `hello_world_dag.py` | Daily | Simple example with pandas |

### Iceberg maintenance pattern (used across maintenance DAGs)
1. Set table properties (`delete-after-commit`, snapshot retention)
2. `CALL iceberg.system.rewrite_data_files(...)` — compaction to 128MB target
3. `CALL iceberg.system.rewrite_manifests(...)`
4. `CALL iceberg.system.expire_snapshots(...)`
5. Remove orphan files (some DAGs call an external script via subprocess: `dags/scripts/iceberg_orphan_cleaner.py`)

## Adding Dependencies

Edit `requirements.txt`, then rebuild:
```bash
docker-compose build airflow-worker airflow-scheduler  # or all services
docker-compose up -d
```

`dbt-core` and `dbt-spark[PyHive]` must stay on the same minor version (`1.10.x`). dbt connects via Spark Thrift Server on port 10000.

## Key Environment Variables (`.env`)

- `AIRFLOW_UID=50000`
- `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` — web admin credentials
- `AIRFLOW_PROJ_DIR=.`
