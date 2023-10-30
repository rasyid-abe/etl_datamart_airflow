SELECT
    m_user_id_user,
    m_cabang_id_cabang,
    CAST(DATE_PART('month', mart_tax_report_date) AS INT) AS mart_tax_report_month,
    CAST(DATE_PART('year', mart_tax_report_date) AS INT) AS mart_tax_report_year,
    mart_tax_report_base_type AS mart_tax_report_base_type,
    SUM(mart_tax_report_transaction_value) AS mart_tax_report_transaction_value,
    SUM(mart_tax_report_tax_value) AS mart_tax_report_tax_value,
    SUM(mart_tax_report_tax_product) AS mart_tax_report_tax_product
FROM mart_prd.mart_tax_report_daily
WHERE mart_tax_report_date BETWEEN
    '{{ macros.ds_format(data_interval_start, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-01") }}'
    AND
    '{{ macros.ds_format(data_interval_start, "%Y-%m-%dT%H:%M:%S%z",  "%Y-%m-%d") }}'
GROUP BY m_user_id_user,
    CAST(DATE_PART('year', mart_tax_report_date) AS INT),
    CAST(DATE_PART('month', mart_tax_report_date) AS INT),
    m_cabang_id_cabang,
    mart_tax_report_base_type
