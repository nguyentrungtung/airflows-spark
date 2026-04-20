{{
    config(
        materialized = 'view',
        tags          = ['intermediate', 'cn_sap_xep']
    )
}}

/*
  LAYER 3 — FORMAT DATES
  Mục đích: chuyển chuỗi *_r → format_* chuẩn dd-MM-yyyy, hoặc 'ĐÌNH CHỈ', 'SAI', ''.
  Dùng macro parse_date_col để tránh lặp logic CASE × 27 cột.
*/

SELECT
    *,
    -- ngay_cap: không check ĐÌNH CHỈ
    {{ parse_date_col('ngay_cap', 'ngay_cap_raw', check_dinh_chi=false) }} AS format_ngay_cap,

    {{ parse_date_col('gs1_raw_col',            'gs1_r')            }} AS format_gs1,
    {{ parse_date_col('gs2_raw_col',            'gs2_r')            }} AS format_gs2,
    {{ parse_date_col('tcn1_raw_col',           'tcn1_r')           }} AS format_tcn1,
    {{ parse_date_col('gs1_tcn1_raw_col',       'gs1_tcn1_r')       }} AS format_gs1_tcn1,
    {{ parse_date_col('gs2_tcn1_raw_col',       'gs2_tcn1_r')       }} AS format_gs2_tcn1,
    {{ parse_date_col('tcn2_raw_col',           'tcn2_r')           }} AS format_tcn2,
    {{ parse_date_col('gs1_tcn2_raw_col',       'gs1_tcn2_r')       }} AS format_gs1_tcn2,
    {{ parse_date_col('gs2_tcn2_raw_col',       'gs2_tcn2_r')       }} AS format_gs2_tcn2,
    {{ parse_date_col('cnl1_raw_col',           'cnl1_r')           }} AS format_cnl1,
    {{ parse_date_col('gs1_cnl1_raw_col',       'gs1_cnl1_r')       }} AS format_gs1_cnl1,
    {{ parse_date_col('gs2_cnl1_raw_col',       'gs2_cnl1_r')       }} AS format_gs2_cnl1,
    {{ parse_date_col('tcn1_cnl1_raw_col',      'tcn1_cnl1_r')      }} AS format_tcn1_cnl1,
    {{ parse_date_col('gs1_tcn1_cnl1_raw_col',  'gs1_tcn1_cnl1_r')  }} AS format_gs1_tcn1_cnl1,
    {{ parse_date_col('gs2_tcn1_cnl1_raw_col',  'gs2_tcn1_cnl1_r')  }} AS format_gs2_tcn1_cnl1,
    {{ parse_date_col('tcn2_cnl1_raw_col',      'tcn2_cnl1_r')      }} AS format_tcn2_cnl1,
    {{ parse_date_col('gs1_tcn2_cnl1_raw_col',  'gs1_tcn2_cnl1_r')  }} AS format_gs1_tcn2_cnl1,
    {{ parse_date_col('gs2_tcn2_cnl1_raw_col',  'gs2_tcn2_cnl1_r')  }} AS format_gs2_tcn2_cnl1,
    {{ parse_date_col('cnl2_raw_col',           'cnl2_r')           }} AS format_cnl2,
    {{ parse_date_col('gs1_cnl2_raw_col',       'gs1_cnl2_r')       }} AS format_gs1_cnl2,
    {{ parse_date_col('gs2_cnl2_raw_col',       'gs2_cnl2_r')       }} AS format_gs2_cnl2,
    {{ parse_date_col('tcn1_cnl2_raw_col',      'tcn1_cnl2_r')      }} AS format_tcn1_cnl2,
    {{ parse_date_col('gs1_tcn1_cnl2_raw_col',  'gs1_tcn1_cnl2_r')  }} AS format_gs1_tcn1_cnl2,
    {{ parse_date_col('gs2_tcn1_cnl2_raw_col',  'gs2_tcn1_cnl2_r')  }} AS format_gs2_tcn1_cnl2,
    {{ parse_date_col('tcn2_cnl2_raw_col',      'tcn2_cnl2_r')      }} AS format_tcn2_cnl2,
    {{ parse_date_col('gs1_tcn2_cnl2_raw_col',  'gs1_tcn2_cnl2_r')  }} AS format_gs1_tcn2_cnl2,
    {{ parse_date_col('gs2_tcn2_cnl2_raw_col',  'gs2_tcn2_cnl2_r')  }} AS format_gs2_tcn2_cnl2

FROM {{ ref('int_cn_sap_xep__raw_extract') }}
