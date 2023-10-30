SELECT
    t.id_transaction as raw_mart_tax_report_id_transaction,
    t.M_User_id_user,
    t.M_Cabang_id_cabang,
    DATE_FORMAT(t.transaction_tgl,'%Y-%m-%d %H:%i:%s +07:00') as raw_mart_tax_report_datetime,
    DATE(t.transaction_tgl) as raw_mart_tax_report_date,
    t.transaction_no_nota as raw_mart_tax_report_no_nota,
    t.transaction_total as raw_mart_tax_report_transaction_value,
    IFNULL(
    CASE
    	WHEN t.transaction_type_detail != ''
    	THEN IF(
    		CAST(t.transaction_type_detail->'$.tax_base_type' AS UNSIGNED) = 0,
    		CAST(1 AS UNSIGNED), CAST(t.transaction_type_detail->'$.tax_base_type' AS UNSIGNED)
    	)
    	ELSE CAST(1 AS UNSIGNED)
    END, CAST(1 AS UNSIGNED)) as raw_mart_tax_report_base_type,
    CASE
	    WHEN tde.id_transaction_detail_extend IS NULL
        THEN t.transaction_tax_nominal
	    ELSE SUM(td.transaction_detail_total_pajak_produk)
    END as raw_mart_tax_report_tax_value,
    IFNULL(SUM(td.transaction_detail_pajak_produk), 0) as raw_mart_tax_report_tax_product
FROM Transactions t
LEFT JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
LEFT JOIN transaction_detail_extend tde ON tde.Transaction_Detail_id_transaction_detail = td.id_transaction_detail
WHERE
    t.Transaction_purpose IN ('5', '9')
    AND t.status = '1'
    AND t.transaction_tgl BETWEEN
        '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
        AND
        '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
    AND t.transaction_tgl > '1970-01-01 00:00:00'
    AND t.transaction_refund = 0
    AND t.transaction_no_nota IS NOT NULL
GROUP BY t.id_transaction
