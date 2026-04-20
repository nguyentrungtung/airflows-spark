{{
    config(
        materialized = 'view',
        tags          = ['intermediate', 'cn_sap_xep']
    )
}}

/*
  LAYER 2 — RAW EXTRACT
  Mục đích: regexp_extract toàn bộ cột ngày thành chuỗi dd/MM/yyyy thuần túy.
  Kế thừa toàn bộ cột từ staging, chỉ thêm cột *_r.
*/

WITH base AS (
    SELECT * FROM {{ ref('stg_cn_sap_xep__coerce') }}
),

date_pattern AS (
    SELECT '((0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])\\/(\\d{2,4}))' AS pat
)

SELECT
    base.*,
    regexp_extract(base.ngay_cap,            (SELECT pat FROM date_pattern), 1) AS ngay_cap_raw,
    regexp_extract(base.gs1_raw_col,         (SELECT pat FROM date_pattern), 1) AS gs1_r,
    regexp_extract(base.gs2_raw_col,         (SELECT pat FROM date_pattern), 1) AS gs2_r,
    regexp_extract(base.tcn1_raw_col,        (SELECT pat FROM date_pattern), 1) AS tcn1_r,
    regexp_extract(base.gs1_tcn1_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs1_tcn1_r,
    regexp_extract(base.gs2_tcn1_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs2_tcn1_r,
    regexp_extract(base.tcn2_raw_col,        (SELECT pat FROM date_pattern), 1) AS tcn2_r,
    regexp_extract(base.gs1_tcn2_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs1_tcn2_r,
    regexp_extract(base.gs2_tcn2_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs2_tcn2_r,
    regexp_extract(base.cnl1_raw_col,        (SELECT pat FROM date_pattern), 1) AS cnl1_r,
    regexp_extract(base.gs1_cnl1_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs1_cnl1_r,
    regexp_extract(base.gs2_cnl1_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs2_cnl1_r,
    regexp_extract(base.tcn1_cnl1_raw_col,   (SELECT pat FROM date_pattern), 1) AS tcn1_cnl1_r,
    regexp_extract(base.gs1_tcn1_cnl1_raw_col,(SELECT pat FROM date_pattern),1) AS gs1_tcn1_cnl1_r,
    regexp_extract(base.gs2_tcn1_cnl1_raw_col,(SELECT pat FROM date_pattern),1) AS gs2_tcn1_cnl1_r,
    regexp_extract(base.tcn2_cnl1_raw_col,   (SELECT pat FROM date_pattern), 1) AS tcn2_cnl1_r,
    regexp_extract(base.gs1_tcn2_cnl1_raw_col,(SELECT pat FROM date_pattern),1) AS gs1_tcn2_cnl1_r,
    regexp_extract(base.gs2_tcn2_cnl1_raw_col,(SELECT pat FROM date_pattern),1) AS gs2_tcn2_cnl1_r,
    regexp_extract(base.cnl2_raw_col,        (SELECT pat FROM date_pattern), 1) AS cnl2_r,
    regexp_extract(base.gs1_cnl2_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs1_cnl2_r,
    regexp_extract(base.gs2_cnl2_raw_col,    (SELECT pat FROM date_pattern), 1) AS gs2_cnl2_r,
    regexp_extract(base.tcn1_cnl2_raw_col,   (SELECT pat FROM date_pattern), 1) AS tcn1_cnl2_r,
    regexp_extract(base.gs1_tcn1_cnl2_raw_col,(SELECT pat FROM date_pattern),1) AS gs1_tcn1_cnl2_r,
    regexp_extract(base.gs2_tcn1_cnl2_raw_col,(SELECT pat FROM date_pattern),1) AS gs2_tcn1_cnl2_r,
    regexp_extract(base.tcn2_cnl2_raw_col,   (SELECT pat FROM date_pattern), 1) AS tcn2_cnl2_r,
    regexp_extract(base.gs1_tcn2_cnl2_raw_col,(SELECT pat FROM date_pattern),1) AS gs1_tcn2_cnl2_r,
    regexp_extract(base.gs2_tcn2_cnl2_raw_col,(SELECT pat FROM date_pattern),1) AS gs2_tcn2_cnl2_r

FROM base
