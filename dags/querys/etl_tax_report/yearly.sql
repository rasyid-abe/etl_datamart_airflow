SELECT
    m_user_id_user,
    m_cabang_id_cabang,
    mart_tax_report_year AS mart_tax_report_year,
    mart_tax_report_base_type AS mart_tax_report_base_type,
    SUM(mart_tax_report_transaction_value) AS mart_tax_report_transaction_value,
    SUM(mart_tax_report_tax_value) AS mart_tax_report_tax_value,
    SUM(mart_tax_report_tax_product) AS mart_tax_report_tax_product
FROM mart_prd.mart_tax_report_monthly
WHERE mart_tax_report_year = CAST('{{ macros.ds_format(data_interval_end, "%Y-%m-%dT%H:%M:%S%z", "%Y") }}' as int)
GROUP BY m_user_id_user, m_cabang_id_cabang, mart_tax_report_year, mart_tax_report_base_type
