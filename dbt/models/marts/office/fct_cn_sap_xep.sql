{{
    config(
        materialized  = 'table',
        file_format   = 'iceberg',
        format_version = 2,
        tags          = ['mart', 'office', 'cn_sap_xep'],
        -- Incremental nếu muốn: thêm unique_key='id' và strategy='merge'
    )
}}

/*
  LAYER 4 — MART (fct)
  Mục đích: tính vị trí ĐÌNH CHỈ đầu/cuối, output ra bảng Iceberg cuối cùng.
  Chỉ SELECT các cột business cần thiết — bỏ toàn bộ cột *_raw_col và *_r trung gian.
*/

WITH fmt AS (
    SELECT * FROM {{ ref('int_cn_sap_xep__fmt_dates') }}
),

with_pos AS (
    SELECT
        id,
        cong_ty,
        loai_chung_nhan,
        format_ngay_cap                AS ngay_cap,

        format_gs1                     AS gs1,
        format_gs2                     AS gs2,
        format_tcn1                    AS tcn1,
        format_gs1_tcn1                AS gs1_tcn1,
        format_gs2_tcn1                AS gs2_tcn1,
        format_tcn2                    AS tcn2,
        format_gs1_tcn2                AS gs1_tcn2,
        format_gs2_tcn2                AS gs2_tcn2,

        format_cnl1                    AS cnl1,
        format_gs1_cnl1                AS gs1_cnl1,
        format_gs2_cnl1                AS gs2_cnl1,
        format_tcn1_cnl1               AS tcn1_cnl1,
        format_gs1_tcn1_cnl1           AS gs1_tcn1_cnl1,
        format_gs2_tcn1_cnl1           AS gs2_tcn1_cnl1,
        format_tcn2_cnl1               AS tcn2_cnl1,
        format_gs1_tcn2_cnl1           AS gs1_tcn2_cnl1,
        format_gs2_tcn2_cnl1           AS gs2_tcn2_cnl1,

        format_cnl2                    AS cnl2,
        format_gs1_cnl2                AS gs1_cnl2,
        format_gs2_cnl2                AS gs2_cnl2,
        format_tcn1_cnl2               AS tcn1_cnl2,
        format_gs1_tcn1_cnl2           AS gs1_tcn1_cnl2,
        format_gs2_tcn1_cnl2           AS gs2_tcn1_cnl2,
        format_tcn2_cnl2               AS tcn2_cnl2,
        format_gs1_tcn2_cnl2           AS gs1_tcn2_cnl2,
        format_gs2_tcn2_cnl2           AS gs2_tcn2_cnl2,

        -- Vị trí CUỐI CÙNG có ĐÌNH CHỈ (0 = không có)
        GREATEST(
            CASE WHEN format_ngay_cap        = 'ĐÌNH CHỈ' THEN  1 ELSE 0 END,
            CASE WHEN format_gs1             = 'ĐÌNH CHỈ' THEN  2 ELSE 0 END,
            CASE WHEN format_gs2             = 'ĐÌNH CHỈ' THEN  3 ELSE 0 END,
            CASE WHEN format_tcn1            = 'ĐÌNH CHỈ' THEN  4 ELSE 0 END,
            CASE WHEN format_gs1_tcn1        = 'ĐÌNH CHỈ' THEN  5 ELSE 0 END,
            CASE WHEN format_gs2_tcn1        = 'ĐÌNH CHỈ' THEN  6 ELSE 0 END,
            CASE WHEN format_tcn2            = 'ĐÌNH CHỈ' THEN  7 ELSE 0 END,
            CASE WHEN format_gs1_tcn2        = 'ĐÌNH CHỈ' THEN  8 ELSE 0 END,
            CASE WHEN format_gs2_tcn2        = 'ĐÌNH CHỈ' THEN  9 ELSE 0 END,
            CASE WHEN format_cnl1            = 'ĐÌNH CHỈ' THEN 10 ELSE 0 END,
            CASE WHEN format_gs1_cnl1        = 'ĐÌNH CHỈ' THEN 11 ELSE 0 END,
            CASE WHEN format_gs2_cnl1        = 'ĐÌNH CHỈ' THEN 12 ELSE 0 END,
            CASE WHEN format_tcn1_cnl1       = 'ĐÌNH CHỈ' THEN 13 ELSE 0 END,
            CASE WHEN format_gs1_tcn1_cnl1   = 'ĐÌNH CHỈ' THEN 14 ELSE 0 END,
            CASE WHEN format_gs2_tcn1_cnl1   = 'ĐÌNH CHỈ' THEN 15 ELSE 0 END,
            CASE WHEN format_tcn2_cnl1       = 'ĐÌNH CHỈ' THEN 16 ELSE 0 END,
            CASE WHEN format_gs1_tcn2_cnl1   = 'ĐÌNH CHỈ' THEN 17 ELSE 0 END,
            CASE WHEN format_gs2_tcn2_cnl1   = 'ĐÌNH CHỈ' THEN 18 ELSE 0 END,
            CASE WHEN format_cnl2            = 'ĐÌNH CHỈ' THEN 19 ELSE 0 END,
            CASE WHEN format_gs1_cnl2        = 'ĐÌNH CHỈ' THEN 20 ELSE 0 END,
            CASE WHEN format_gs2_cnl2        = 'ĐÌNH CHỈ' THEN 21 ELSE 0 END,
            CASE WHEN format_tcn1_cnl2       = 'ĐÌNH CHỈ' THEN 22 ELSE 0 END,
            CASE WHEN format_gs1_tcn1_cnl2   = 'ĐÌNH CHỈ' THEN 23 ELSE 0 END,
            CASE WHEN format_gs2_tcn1_cnl2   = 'ĐÌNH CHỈ' THEN 24 ELSE 0 END,
            CASE WHEN format_tcn2_cnl2       = 'ĐÌNH CHỈ' THEN 25 ELSE 0 END,
            CASE WHEN format_gs1_tcn2_cnl2   = 'ĐÌNH CHỈ' THEN 26 ELSE 0 END,
            CASE WHEN format_gs2_tcn2_cnl2   = 'ĐÌNH CHỈ' THEN 27 ELSE 0 END
        ) AS last_dc_pos,

        -- Vị trí ĐẦU TIÊN có ĐÌNH CHỈ (28 = sentinel không có)
        LEAST(
            CASE WHEN format_ngay_cap        = 'ĐÌNH CHỈ' THEN  1 ELSE 28 END,
            CASE WHEN format_gs1             = 'ĐÌNH CHỈ' THEN  2 ELSE 28 END,
            CASE WHEN format_gs2             = 'ĐÌNH CHỈ' THEN  3 ELSE 28 END,
            CASE WHEN format_tcn1            = 'ĐÌNH CHỈ' THEN  4 ELSE 28 END,
            CASE WHEN format_gs1_tcn1        = 'ĐÌNH CHỈ' THEN  5 ELSE 28 END,
            CASE WHEN format_gs2_tcn1        = 'ĐÌNH CHỈ' THEN  6 ELSE 28 END,
            CASE WHEN format_tcn2            = 'ĐÌNH CHỈ' THEN  7 ELSE 28 END,
            CASE WHEN format_gs1_tcn2        = 'ĐÌNH CHỈ' THEN  8 ELSE 28 END,
            CASE WHEN format_gs2_tcn2        = 'ĐÌNH CHỈ' THEN  9 ELSE 28 END,
            CASE WHEN format_cnl1            = 'ĐÌNH CHỈ' THEN 10 ELSE 28 END,
            CASE WHEN format_gs1_cnl1        = 'ĐÌNH CHỈ' THEN 11 ELSE 28 END,
            CASE WHEN format_gs2_cnl1        = 'ĐÌNH CHỈ' THEN 12 ELSE 28 END,
            CASE WHEN format_tcn1_cnl1       = 'ĐÌNH CHỈ' THEN 13 ELSE 28 END,
            CASE WHEN format_gs1_tcn1_cnl1   = 'ĐÌNH CHỈ' THEN 14 ELSE 28 END,
            CASE WHEN format_gs2_tcn1_cnl1   = 'ĐÌNH CHỈ' THEN 15 ELSE 28 END,
            CASE WHEN format_tcn2_cnl1       = 'ĐÌNH CHỈ' THEN 16 ELSE 28 END,
            CASE WHEN format_gs1_tcn2_cnl1   = 'ĐÌNH CHỈ' THEN 17 ELSE 28 END,
            CASE WHEN format_gs2_tcn2_cnl1   = 'ĐÌNH CHỈ' THEN 18 ELSE 28 END,
            CASE WHEN format_cnl2            = 'ĐÌNH CHỈ' THEN 19 ELSE 28 END,
            CASE WHEN format_gs1_cnl2        = 'ĐÌNH CHỈ' THEN 20 ELSE 28 END,
            CASE WHEN format_gs2_cnl2        = 'ĐÌNH CHỈ' THEN 21 ELSE 28 END,
            CASE WHEN format_tcn1_cnl2       = 'ĐÌNH CHỈ' THEN 22 ELSE 28 END,
            CASE WHEN format_gs1_tcn1_cnl2   = 'ĐÌNH CHỈ' THEN 23 ELSE 28 END,
            CASE WHEN format_gs2_tcn1_cnl2   = 'ĐÌNH CHỈ' THEN 24 ELSE 28 END,
            CASE WHEN format_tcn2_cnl2       = 'ĐÌNH CHỈ' THEN 25 ELSE 28 END,
            CASE WHEN format_gs1_tcn2_cnl2   = 'ĐÌNH CHỈ' THEN 26 ELSE 28 END,
            CASE WHEN format_gs2_tcn2_cnl2   = 'ĐÌNH CHỈ' THEN 27 ELSE 28 END
        ) AS first_dc_pos,

        current_timestamp()            AS dbt_updated_at

    FROM fmt
)

SELECT
    *,
    CASE WHEN last_dc_pos > 0  THEN true ELSE false END AS co_dinh_chi,
    CASE WHEN first_dc_pos < 28 THEN first_dc_pos ELSE NULL END AS vi_tri_dinh_chi_dau_tien
FROM with_pos
