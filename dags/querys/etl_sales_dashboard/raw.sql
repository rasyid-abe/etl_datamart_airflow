select
	tbl1.id_transaction as raw_mart_sales_dashboard_id_transaction,
	M_User_id_user as m_user_id_user,
	M_Cabang_id_cabang as m_cabang_id_cabang,
	transaction_refund as raw_mart_sales_dashboard_is_refund,
	transaction_tgl as raw_mart_sales_dashboard_datetime,
	tanggal as raw_mart_sales_dashboard_date,
	COALESCE(transaction_total, 0) as raw_mart_sales_dashboard_sales_value,
	COALESCE(payment_value, 0) as raw_mart_sales_dashboard_payment_value,
	COALESCE(penjualan_kotor, 0) as raw_mart_sales_dashboard_gross_sales,
	COALESCE(transaction_refund, 0) as raw_mart_sales_dashboard_refund,
	order_type as raw_mart_sales_dashboard_order_type,
	COALESCE(trx_produk, 0) as raw_mart_sales_dashboard_product,
	COALESCE(hpp_product, 0) as raw_mart_sales_dashboard_hpp_product,
	COALESCE(hpp_addon, 0) as raw_mart_sales_dashboard_hpp_addon,
	COALESCE(mdr, 0) as raw_mart_sales_dashboard_mdr,
	COALESCE(komisi, 0) as raw_mart_sales_dashboard_commission,
  	CASE WHEN (COALESCE(transaction_pisah, 0) = 1 AND (
  		transaction_no_nota_parent IS NOT NULL AND
  		transaction_no_nota_parent != ''))
    	OR (COALESCE(transaction_refund, 0) > 0 OR
    	coalesce(transaction_refund_reason, '') != '')
    	THEN
        	0
    	ELSE
        	1
	END AS raw_mart_sales_dashboard_count_transaction
from (
	SELECT
		id_transaction,
		DATE_FORMAT(transaction_tgl,'%Y-%m-%d %H:%i:%s +07:00') as transaction_tgl,
		DATE(transaction_tgl) as tanggal,
		M_User_id_user,
		M_Cabang_id_cabang,
		transaction_total,
		transaction_paid,
		COALESCE(transaction_pisah, 0) transaction_pisah,
		transaction_no_nota_parent,
		transaction_type,
		transaction_refund_reason,
		IFNULL(transaction_type, 99) as order_type,
		COALESCE(transaction_refund, 0) transaction_refund,
		updatedate,
		status
	FROM
		Transactions
	WHERE
		transaction_tgl >= '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND transaction_tgl <= '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND M_User_id_user = 1555722
		AND status in ('1')
		AND transaction_purpose IN ('5', '9')
) tbl1
left join (
	SELECT
        t.id_transaction,
        COALESCE(SUM(tc.commission_total_nominal), 0) komisi
    FROM
        Transactions t
    JOIN
        Transaction_Commission tc on tc.Transactions_id_transaction = t.id_transaction
    WHERE
        transaction_tgl >= '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND transaction_tgl <= '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND t.M_User_id_user = 1555722
		AND status in ('1')
		AND transaction_purpose IN ('5', '9')
        AND (
            (t.transaction_pisah = 1 AND t.transaction_no_payment <= 1)
            OR t.transaction_pisah != 1 OR t.transaction_pisah IS NULL
        )
        AND t.transaction_tgl_payment IS NOT NULL
    GROUP BY 1
) tbl2 on tbl1.id_transaction = tbl2.id_transaction
left join (
	SELECT
        t.id_transaction,
        COALESCE(SUM(td.transaction_detail_price_modal * td.transaction_detail_jumlah), 0) hpp_product,
        COALESCE(SUM(td.transaction_detail_jumlah), 0) trx_produk,
        COALESCE(SUM(td.transaction_detail_total_price), 0) as penjualan_kotor
    FROM Transactions t
    LEFT JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
    WHERE
        transaction_tgl >= '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND transaction_tgl <= '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND t.M_User_id_user = 1555722
		AND status in ('1')
		AND transaction_purpose IN ('5', '9')
    GROUP BY 1
) tbl3 on tbl1.id_transaction = tbl3.id_transaction
left join (
	SELECT
        t.id_transaction,
        COALESCE(SUM(tad.transaction_addon_detail_quantity * tad.transaction_addon_detail_quantity_bahan * tad.transaction_addon_detail_harga_modal), 0) hpp_addon
    FROM Transactions t
    LEFT JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
    LEFT JOIN
        Transaction_Addon_Detail tad on tad.Transaction_Detail_id_transaction_detail = td.id_transaction_detail
    WHERE
        transaction_tgl >= '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND transaction_tgl <= '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND t.M_User_id_user = 1555722
		AND status in ('1')
		AND transaction_purpose IN ('5', '9')
    GROUP BY 1
) tbl4 on tbl1.id_transaction = tbl4.id_transaction
left join  (
	SELECT
        t.id_transaction,
        COALESCE(SUM(thpm.transaction_has_payment_method_MDR), 0) mdr,
 		CASE
        	WHEN thpm.Payment_method_id_payment_method = 1 THEN
    	        SUM(thpm.transaction_has_payment_method_value - t.transaction_kembalian)
	        ELSE
        	    SUM(thpm.transaction_has_payment_method_value)
		END AS payment_value
    FROM Transactions t
    LEFT JOIN Transaction_has_Payment_Method thpm ON t.id_transaction = thpm.Transaction_id_transaction
    WHERE
        transaction_tgl >= '{{ macros.ds_format(data_interval_start + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND transaction_tgl <= '{{ macros.ds_format(data_interval_end + macros.timedelta(hours=6), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S") }}'
		AND t.M_User_id_user = 1555722
		AND status in ('1')
		AND transaction_purpose IN ('5', '9')
    GROUP BY 1
) tbl5 on tbl1.id_transaction = tbl5.id_transaction
