SELECT m_user_id_user,
       m_cabang_id_cabang,
       raw_mart_tax_report_date as mart_tax_report_date,
       raw_mart_tax_report_base_type as mart_tax_report_base_type,
       SUM(raw_mart_tax_report_transaction_value) as mart_tax_report_transaction_value,
       SUM(raw_mart_tax_report_tax_value) as mart_tax_report_tax_value,
       SUM(raw_mart_tax_report_tax_product) as mart_tax_report_tax_product
FROM mart_prd.raw_mart_tax_report
WHERE raw_mart_tax_report_date = '{{ macros.ds_format(data_interval_start, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d") }}'
GROUP BY m_user_id_user, raw_mart_tax_report_date, m_cabang_id_cabang, raw_mart_tax_report_base_type
