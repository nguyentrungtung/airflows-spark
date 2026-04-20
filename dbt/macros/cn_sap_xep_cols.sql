{% macro cn_sap_xep_select(source_relation, loai_chung_nhan) %}
SELECT
    id,
    cong_ty,
    '{{ loai_chung_nhan }}'   AS loai_chung_nhan,
    ngay_cap,
    -- ROOT 1
    giam_sat_lan_1            AS gs1_raw_col,
    giam_sat_lan_2            AS gs2_raw_col,
    tcn_1                     AS tcn1_raw_col,
    giam_sat_lan_1_tcn_1      AS gs1_tcn1_raw_col,
    giam_sat_lan_2_tcn_1      AS gs2_tcn1_raw_col,
    tcn_2                     AS tcn2_raw_col,
    giam_sat_lan_1_tcn_2      AS gs1_tcn2_raw_col,
    giam_sat_lan_2_tcn_2      AS gs2_tcn2_raw_col,
    -- ROOT 2
    cnl_1                     AS cnl1_raw_col,
    giam_sat_lan_1_cnl_1      AS gs1_cnl1_raw_col,
    giam_sat_lan_2_cnl_1      AS gs2_cnl1_raw_col,
    tcn1_cnl_1                AS tcn1_cnl1_raw_col,
    giam_sat_lan_1_tcn1_cnl_1 AS gs1_tcn1_cnl1_raw_col,
    giam_sat_lan_2_tcn1cnl_1  AS gs2_tcn1_cnl1_raw_col,
    tcn2_cnl_1                AS tcn2_cnl1_raw_col,
    giam_sat_lan_1_tcn2_cnl_1 AS gs1_tcn2_cnl1_raw_col,
    giam_sat_lan_2_tcn2_cnl_1 AS gs2_tcn2_cnl1_raw_col,
    -- ROOT 3
    cnl_2                     AS cnl2_raw_col,
    giam_sat_lan_1_cnl_2      AS gs1_cnl2_raw_col,
    giam_sat_lan_2_cnl_2      AS gs2_cnl2_raw_col,
    tcn1_cnl_2                AS tcn1_cnl2_raw_col,
    giam_sat_lan_1_tcn1_cnl_2 AS gs1_tcn1_cnl2_raw_col,
    giam_sat_lan_2_tcn1cnl_2  AS gs2_tcn1_cnl2_raw_col,
    tcn2_cnl2                 AS tcn2_cnl2_raw_col,
    giam_sat_lan_1_tcn2_cnl_2 AS gs1_tcn2_cnl2_raw_col,
    giam_sat_lan_2_tcn2_cnl_2 AS gs2_tcn2_cnl2_raw_col
FROM {{ source_relation }}
{% endmacro %}


{# Macro dùng chung để parse + format 1 cột ngày dd/MM/yy hoặc dd/MM/yyyy #}
{% macro parse_date_col(col_raw, col_r, check_dinh_chi=true) %}
CASE
    WHEN TRIM({{ col_raw }}) IS NULL OR TRIM({{ col_raw }}) = '' THEN ''
    {% if check_dinh_chi %}
    WHEN LOWER(TRIM({{ col_raw }})) LIKE '%đình chỉ%' THEN 'ĐÌNH CHỈ'
    {% endif %}
    WHEN {{ col_r }} != '' THEN date_format(
        CAST(from_unixtime(unix_timestamp(
            CASE
                WHEN {{ col_r }} RLIKE '/\\d{2}$'
                    THEN regexp_replace({{ col_r }}, '/(\\d{2})$', '/20$1')
                WHEN {{ col_r }} RLIKE '/00\\d{2}$'
                    THEN regexp_replace({{ col_r }}, '/00(\\d{2})$', '/20$1')
                ELSE {{ col_r }}
            END,
            'd/M/yyyy'
        )) AS DATE),
        'dd-MM-yyyy'
    )
    ELSE 'SAI'
END
{% endmacro %}
