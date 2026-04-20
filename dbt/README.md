# dbt Analytics Project

Tầng **Transform** (chữ T trong ELT) — nhận data đã được Airflow + Spark ingest vào Iceberg,
biến đổi thành các bảng sạch phục vụ báo cáo và dashboard.

---

## Kiến trúc tổng thể

```
[Nguồn]          [Airflow + Spark]     [dbt]                      [BI]
MySQL ──────────►                      staging/ ──► intermediate/
Postgres ───────► Iceberg Bronze  ────►                            ──► Metabase
MongoDB ─────────  (raw tables)        marts/office/                   Superset
Excel ──────────►                      marts/crm/
                                       marts/reporting/  ──────────► Dashboard
```

---

## Cấu trúc thư mục

```
dbt/
├── profiles.yml            Kết nối Spark Thrift Server (KHÔNG commit nếu repo public)
├── dbt_project.yml         Config project: tên schema output, materialization mặc định
├── README.md               File này
│
├── macros/                 Jinja macros dùng chung — tránh lặp SQL
│   └── cn_sap_xep_cols.sql
│
├── models/
│   │
│   ├── staging/            LAYER 1 — View, 1:1 với source table
│   │   │                   Chỉ đổi tên cột, ép kiểu cơ bản, KHÔNG join, KHÔNG filter
│   │   ├── office/         Source: Iceberg upload_* (chứng nhận, sắp xếp...)
│   │   │   ├── _sources.yml
│   │   │   └── stg_cn_sap_xep__coerce.sql
│   │   ├── crm/            Source: Iceberg stg_crm_v2 (khách hàng, hợp đồng...)
│   │   │   └── _sources.yml
│   │   ├── mysql/          Source: Iceberg stg_mysql_erp (ERP từ MySQL)
│   │   │   └── _sources.yml
│   │   └── postgres/       Source: Iceberg stg_postgres_* (chat memory, isocert...)
│   │
│   ├── intermediate/       LAYER 2 — View, business logic phức tạp
│   │   │                   Join giữa staging, regexp, window functions
│   │   │                   Đổi sang table nếu query > 30 giây
│   │   ├── office/
│   │   │   ├── int_cn_sap_xep__raw_extract.sql
│   │   │   └── int_cn_sap_xep__fmt_dates.sql
│   │   └── crm/
│   │
│   └── marts/              LAYER 3 — Iceberg TABLE thật, output cuối
│       │                   BI tool đọc trực tiếp từ đây
│       ├── office/
│       │   ├── _schema.yml         dbt tests (not_null, unique, accepted_values)
│       │   └── fct_cn_sap_xep.sql  Fact table chứng nhận đã format
│       ├── crm/
│       └── reporting/      Báo cáo tổng hợp, KPI, dashboard
│           ├── daily/      Báo cáo theo ngày — chạy mỗi sáng
│           └── monthly/    Báo cáo tháng — chạy đầu tháng
│
└── tests/                  Custom SQL tests (ngoài tests trong _schema.yml)
    └── assert_no_sai_dates.sql
```

---

## Quy tắc đặt tên file

| Pattern | Layer | Ví dụ |
|---|---|---|
| `stg_{source}__{entity}.sql` | staging | `stg_crm__customers.sql` |
| `int_{domain}__{logic}.sql` | intermediate | `int_office__cn_joined.sql` |
| `fct_{entity}.sql` | mart — fact (số liệu) | `fct_cn_sap_xep.sql` |
| `dim_{entity}.sql` | mart — dimension (danh mục) | `dim_companies.sql` |
| `rpt_{report_name}.sql` | reporting | `rpt_daily_cert_summary.sql` |

Dấu `__` (double underscore) phân tách **source/domain** với **entity** — quy ước chuẩn của cộng đồng dbt.

---

## Cài đặt

```bash
# Trong container Airflow (đã có dbt-spark cài sẵn)
docker exec -it <airflow-worker-container> bash

# Hoặc trên máy local nếu có Python
pip install dbt-spark[PyHive]==1.10.1

# Kiểm tra kết nối
cd /opt/airflow/dbt
dbt debug
```

---

## Chạy dbt

### Chạy toàn bộ pipeline
```bash
dbt run
```

### Chạy 1 model cụ thể
```bash
dbt run --select fct_cn_sap_xep
```

### Chạy theo tag
```bash
dbt run --select tag:staging          # chỉ staging layer
dbt run --select tag:mart             # chỉ mart layer
dbt run --select tag:reporting        # chỉ báo cáo
```

### Chạy 1 model + toàn bộ dependencies phía trước
```bash
dbt run --select +fct_cn_sap_xep      # dấu + = chạy upstream trước
```

### Chạy 1 model + toàn bộ downstream
```bash
dbt run --select fct_cn_sap_xep+      # dấu + sau = chạy downstream sau
```

---

## Test data quality

```bash
# Chạy tất cả tests khai báo trong _schema.yml
dbt test

# Test 1 model cụ thể
dbt test --select fct_cn_sap_xep

# Chạy run + test cùng lúc
dbt build --select fct_cn_sap_xep
```

---

## Xem lineage (data flow graph)

```bash
dbt docs generate
dbt docs serve --port 8081
# Mở trình duyệt: http://localhost:8081
```

---

## Thêm domain / SQL mới

### 1. Thêm source mới (ví dụ: MongoDB)
```
models/staging/mongodb/_sources.yml    ← khai báo tables
models/staging/mongodb/stg_mongo__orders.sql
```

### 2. Thêm báo cáo mới
```
models/marts/reporting/daily/rpt_daily_new_certs.sql
```
Đặt config materialization trong file:
```sql
{{ config(materialized='table', tags=['reporting','daily']) }}
SELECT ...
```

### 3. Thay đổi ngày upload (khi có file mới)
Sửa trong `dbt_project.yml`, không cần sửa SQL:
```yaml
vars:
  source_upload_date: "upload_20260501"   # ← đổi tại đây
```

---

## Tích hợp với Airflow

DAG Airflow gọi dbt bằng BashOperator:

```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id='dbt_run_office',
    bash_command='cd /opt/airflow/dbt && dbt run --select tag:office',
)

dbt_test = BashOperator(
    task_id='dbt_test_office',
    bash_command='cd /opt/airflow/dbt && dbt test --select tag:office',
)

dbt_run >> dbt_test
```

---

## Thứ tự chạy (Airflow orchestrate)

```
[00:15] Airflow: ingest CRM JSON → Iceberg Bronze
[01:15] Airflow: ingest Postgres  → Iceberg Bronze
        ↓
[02:00] dbt run --select tag:staging       (build views)
[02:05] dbt run --select tag:intermediate  (build views)
[02:10] dbt run --select tag:mart          (build Iceberg tables)
[02:30] dbt test                           (data quality check)
        ↓
[03:00] Dashboard / BI tool đọc từ mart_*
```
