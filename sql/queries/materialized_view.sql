CREATE MATERIALIZED VIEW dw.mv_report AS
SELECT
    locale_state,
    product_category,
    customer_type,
    time_hour,
    SUM(sales_result)
FROM
    dw.d_product AS tb1,
    dw.d_customer AS tb2,
    dw.d_locale AS tb3,
    dw.d_time AS tb4,
    dw.fact_sales AS tb5
WHERE
    tb5.sk_product = tb1.sk_product
    AND tb5.sk_customer = tb2.sk_customer
    AND tb5.sk_locale = tb3.sk_locale
    AND tb5.sk_time = tb4.sk_time
GROUP BY
    locale_state,
    product_category,
    customer_type,
    time_hour
ORDER BY
    locale_state,
    product_category,
    customer_type,
    time_hour;