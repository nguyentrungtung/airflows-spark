{{
    config(
        materialized = 'view',
        tags          = ['staging', 'office', 'cn_sap_xep']
    )
}}

/*
  LAYER 1 — COERCE
  Mục đích: UNION ALL tất cả loại chứng nhận, chuẩn hóa tên cột về schema chung.
  Dùng macro cn_sap_xep_select để tránh lặp 27 cột × 15 bảng.
*/

{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_13485'),         '13485') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_14001'),         '14001') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_22000'),         '22000') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_22716'),         '22716') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_27001'),         '27001') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_45001'),         '45001') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_5s'),            '5s') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_9001'),          '9001') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_gmp'),           'gmp') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_haccp'),         'haccp') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_qcvn'),          'qcvn') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_san_pham'),      'san_pham') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_tcvn_11041_22017'), '11041_22017') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_vietgahp'),      'vietgahp') }}
UNION ALL
{{ cn_sap_xep_select(source('cn_sap_xep', 'tbl_vietgap'),       'vietgap') }}
