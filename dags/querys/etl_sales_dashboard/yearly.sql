SELECT
    m_user_id_user,
    m_cabang_id_cabang,
    mart_sales_dashboard_monthly_order_type as mart_sales_dashboard_yearly_order_type,
    mart_sales_dashboard_monthly_year AS mart_sales_dashboard_yearly_year,
    SUM(mart_sales_dashboard_monthly_sales_value) AS mart_sales_dashboard_yearly_sales_value,
    SUM(mart_sales_dashboard_monthly_payment_value) AS mart_sales_dashboard_yearly_payment_value,
    SUM(mart_sales_dashboard_monthly_gross_value) AS mart_sales_dashboard_yearly_gross_value,
    SUM(mart_sales_dashboard_monthly_gross_sales) AS mart_sales_dashboard_yearly_gross_sales,
    SUM(mart_sales_dashboard_monthly_product_qty) AS mart_sales_dashboard_yearly_product_qty,
    SUM(mart_sales_dashboard_monthly_transaction) AS mart_sales_dashboard_yearly_transaction,
    SUM(mart_sales_dashboard_monthly_refund) AS mart_sales_dashboard_yearly_refund,
    SUM(mart_sales_dashboard_monthly_commission) AS mart_sales_dashboard_yearly_commission
FROM mart_prd.mart_sales_dashboard_monthly
WHERE mart_sales_dashboard_monthly_year = CAST('{{ macros.ds_format(data_interval_end, "%Y-%m-%dT%H:%M:%S%z", "%Y") }}' as int)
group by m_user_id_user, m_cabang_id_cabang, mart_sales_dashboard_monthly_year, mart_sales_dashboard_monthly_order_type
