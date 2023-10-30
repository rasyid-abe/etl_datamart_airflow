SELECT
	uuid() as raw_mart_compliment_no,
    thp.Transactions_id_transaction as raw_mart_compliment_transaction_id,
	ts.M_User_id_user as m_user_id_user,
	ts.M_Cabang_id_cabang as m_cabang_id_cabang,
	ts.transaction_no_nota as raw_mart_compliment_no_nota,
	CONVERT_TZ(ts.transaction_tgl , '+07:00', '+00:00') as raw_mart_compliment_datetime,
    CAST(ts.transaction_tgl AS DATE) as raw_mart_compliment_date,
	ts.transaction_id_kasir_bayar as raw_mart_compliment_cashier_id,
	ts.transaction_jumlah_pesanan as raw_mart_compliment_product_qty,
    ts.transaction_otoritas as raw_mart_compliment_auth_id,
	thp.transaction_promo_value as raw_mart_compliment_value,
    ts.transaction_catatan as raw_mart_compliment_note
FROM
	Transactions ts
JOIN
	Transaction_has_Promo thp
	ON thp.Transactions_id_transaction = ts.id_transaction
WHERE
	ts.status = '1'
	AND ts.transaction_otoritas IS NOT NULL
	AND ts.transaction_id_kasir_bayar IS NOT NULL
	AND thp.M_Category_Promo_id_category_promo = 4
	AND Transaction_purpose = '5'
	AND ts.transaction_tgl
    BETWEEN '{{ macros.ds_format(data_interval_start - macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d") }}'
    AND '{{ macros.ds_format(data_interval_end - macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d") }}'
GROUP BY thp.Transactions_id_transaction
