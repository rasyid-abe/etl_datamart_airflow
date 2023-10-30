SELECT
    m_user_id_user,
    m_cabang_id_cabang,
    mart_sales_dashboard_daily_order_type as mart_sales_dashboard_monthly_order_type,
    CAST(DATE_PART('month', mart_sales_dashboard_daily_date) AS INT) AS mart_sales_dashboard_monthly_month,
    CAST(DATE_PART('year', mart_sales_dashboard_daily_date) AS INT) AS mart_sales_dashboard_monthly_year,
    SUM(mart_sales_dashboard_daily_sales_value) AS mart_sales_dashboard_monthly_sales_value,
    SUM(mart_sales_dashboard_daily_payment_value) AS mart_sales_dashboard_monthly_payment_value,
    SUM(mart_sales_dashboard_daily_gross_value) AS mart_sales_dashboard_monthly_gross_value,
    SUM(mart_sales_dashboard_daily_gross_sales) AS mart_sales_dashboard_monthly_gross_sales,
    SUM(mart_sales_dashboard_daily_product_qty) AS mart_sales_dashboard_monthly_product_qty,
    SUM(mart_sales_dashboard_daily_transaction) AS mart_sales_dashboard_monthly_transaction,
    SUM(mart_sales_dashboard_daily_refund) AS mart_sales_dashboard_monthly_refund,
    SUM(mart_sales_dashboard_daily_commission) AS mart_sales_dashboard_monthly_commission
FROM
    mart_prd.mart_sales_dashboard_daily
WHERE mart_sales_dashboard_daily_date
    BETWEEN '{{ macros.ds_format(data_interval_start, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-01") }}'
    AND '{{ macros.ds_format(data_interval_start, "%Y-%m-%dT%H:%M:%S%z",  "%Y-%m-%d") }}'
group by m_user_id_user, m_cabang_id_cabang, CAST(DATE_PART('year', mart_sales_dashboard_daily_date) AS INT), CAST(DATE_PART('month', mart_sales_dashboard_daily_date) AS INT), mart_sales_dashboard_daily_order_type
