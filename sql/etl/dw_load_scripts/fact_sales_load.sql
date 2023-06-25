INSERT INTO dw.fact_sales (
    sk_product, 
    sk_customer, 
    sk_locale, 
    sk_time, 
    quantity, 
    price_sale, 
    price_product, 
    sales_revenue
)
SELECT 
    sk_product,
    sk_customer,
    sk_locale,
    sk_time, 
    SUM(sale_quantity) AS sale_quantity, 
    SUM(sale_price) AS sale_price, 
    SUM(product_cost) AS product_cost, 
    SUM(ROUND
        ((CAST(sale_quantity AS numeric) * CAST(sale_price AS numeric)),
        2)) AS sales_revenue
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
    AND tb1.sale_date = tb5.time_date
    AND tb2.customer_id = tb8.customer_id
    AND tb3.locality_id = tb7.locale_id
    AND tb4.product_id = tb6.product_id
GROUP BY sk_product, sk_customer, sk_locale, sk_time;

