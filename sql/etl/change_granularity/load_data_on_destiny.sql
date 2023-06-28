INSERT INTO dw.d_time (
    time_year, 
    time_month, 
    time_day, 
    time_hour, 
    time_date
    )
SELECT 
    EXTRACT(YEAR FROM d)::INT, 
    EXTRACT(MONTH FROM d)::INT, 
    EXTRACT(DAY FROM d)::INT, 
    LPAD(EXTRACT(HOUR FROM d)::integer::text, 2, '0'), 
    d::DATE
FROM generate_series('2020-01-01'::DATE, '2024-12-31'::DATE, '1 HOUR'::INTERVAL) d;

INSERT INTO dw.fact_sales (sk_product, 
    sk_customer, 
    sk_locale, 
    sk_time, 
    quantity, 
    price_sale, 
    price_product, 
    sales_revenue)
SELECT 
    sk_product,
    sk_customer,
    sk_locale,
    sk_time, 
    SUM(sale_quantity) AS quantity, 
    SUM(sale_price) AS price_sale, 
    SUM(product_cost) AS price_product, 
    SUM(ROUND((CAST(sale_quantity AS numeric) * CAST(sale_price AS numeric)), 2)) AS sales_revenue
FROM sta.sta_sr_sales tb1, 
    sta.sta_sr_customers tb2, 
    sta.sta_sr_localities tb3, 
    sta.sta_sr_products tb4,
    dw.d_time tb5,
    dw.d_product tb6,
    dw.d_locale tb7,
    dw.d_customer tb8
WHERE tb2.customer_id = tb1.customer_id
    AND tb3.locality_id = tb1.locality_id
    AND tb4.product_id = tb1.sale_product
    AND to_char(tb1.sale_date, 'YYYY-MM-DD') = to_char(tb5.time_date, 'YYYY-MM-DD')
    AND to_char(tb1.sale_date, 'HH') = tb5.time_hour
    AND tb2.customer_id = tb8.customer_id
    AND tb3.locality_id = tb7.locale_id
    AND tb4.product_id = tb6.product_id
GROUP BY sk_product, sk_customer, sk_locale, sk_time;
