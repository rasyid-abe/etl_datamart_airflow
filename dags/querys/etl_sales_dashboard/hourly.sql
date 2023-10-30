SELECT
    m_user_id_user,
    m_cabang_id_cabang,
    raw_mart_sales_dashboard_order_type as mart_sales_dashboard_hourly_order_type,
    CAST(date_part('hour',raw_mart_sales_dashboard_datetime)  as INT) as mart_sales_dashboard_hourly_hour,
    raw_mart_sales_dashboard_date AS mart_sales_dashboard_hourly_date,
    SUM(CASE WHEN raw_mart_sales_dashboard_is_refund THEN 0 ELSE raw_mart_sales_dashboard_sales_value END) mart_sales_dashboard_hourly_sales_value,
    SUM(CASE WHEN raw_mart_sales_dashboard_is_refund THEN 0 ELSE raw_mart_sales_dashboard_payment_value END) mart_sales_dashboard_hourly_payment_value,
    SUM (
        case when raw_mart_sales_dashboard_is_refund is false then
            raw_mart_sales_dashboard_sales_value
            - raw_mart_sales_dashboard_mdr
            - raw_mart_sales_dashboard_hpp_product
            - raw_mart_sales_dashboard_hpp_addon
            - raw_mart_sales_dashboard_commission
        else
            raw_mart_sales_dashboard_sales_value
            + raw_mart_sales_dashboard_mdr
            + raw_mart_sales_dashboard_hpp_product
            + raw_mart_sales_dashboard_hpp_addon
            + raw_mart_sales_dashboard_commission
        end
    ) AS mart_sales_dashboard_hourly_gross_value,
    SUM(CASE WHEN raw_mart_sales_dashboard_is_refund THEN 0 ELSE raw_mart_sales_dashboard_gross_sales END) mart_sales_dashboard_hourly_gross_sales,
    SUM(CASE WHEN raw_mart_sales_dashboard_is_refund THEN 0 ELSE raw_mart_sales_dashboard_product END) mart_sales_dashboard_hourly_product_qty,
    SUM(raw_mart_sales_dashboard_count_transaction) mart_sales_dashboard_hourly_transaction,
    SUM(CASE WHEN raw_mart_sales_dashboard_is_refund THEN raw_mart_sales_dashboard_refund ELSE 0 END) mart_sales_dashboard_hourly_refund,
    (
        SUM(CASE WHEN NOT raw_mart_sales_dashboard_is_refund THEN raw_mart_sales_dashboard_commission ELSE 0 END) -
        SUM(CASE WHEN raw_mart_sales_dashboard_is_refund THEN raw_mart_sales_dashboard_commission ELSE 0 END)
    ) as  mart_sales_dashboard_hourly_commission
FROM
    mart_prd.raw_mart_sales_dashboard
WHERE raw_mart_sales_dashboard_date = '{{ macros.ds_format(data_interval_start, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d") }}'
GROUP BY m_user_id_user, raw_mart_sales_dashboard_date, m_cabang_id_cabang, raw_mart_sales_dashboard_order_type, DATE_PART('hour', raw_mart_sales_dashboard_datetime)
