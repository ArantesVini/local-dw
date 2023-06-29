TRUNCATE TABLE dw.fact_sales;

ALTER TABLE
    IF EXISTS dw.fact_sales
ADD
    COLUMN IF NOT EXISTS sales_result numeric(10, 2) NOT NULL;

INSERT INTO
    dw.fact_sales (
        sk_product,
        sk_customer,
        sk_locale,
        sk_time,
        quantity,
        price_sale,
        price_product,
        sales_revenue,
        sales_result
    )
SELECT
    sk_product,
    sk_customer,
    sk_locale,
    sk_time,
    SUM(sale_quantity) AS sale_quantity,
    SUM(sale_price) AS sale_price,
    SUM(product_cost) AS product_cost,
    SUM(
        ROUND (
            (
                CAST(sale_quantity AS numeric) * CAST(sale_price AS numeric)
            ),
            2
        )
    ) AS sales_revenue,
    SUM(
        ROUND (
            (
                CAST(sale_quantity AS numeric) * CAST(sale_price AS numeric)
            ),
            2
        ) - product_cost
    ) AS sales_result
FROM
    sta.sta_sr_sales tb1
    JOIN sta.sta_sr_customers tb2 ON tb2.customer_id = tb1.customer_id
    JOIN sta.sta_sr_products tb4 ON tb4.product_id = tb1.sale_product
    JOIN dw.d_time tb5 ON (
        to_char(tb5.time_date, 'YYYY-MM-DD') = to_char(tb1.sale_date, 'YYYY-MM-DD')
        AND tb5.time_hour = to_char(tb1.sale_date, 'HH')
    )
    JOIN dw.d_product tb6 ON tb4.product_id = tb6.product_id
    JOIN sta.sta_sr_localities tb3 ON tb3.locality_id = tb1.locality_id -- TODO problem
    JOIN dw.d_locale tb7 ON tb3.locality_id = tb7.locale_id -- TODO problem
    JOIN dw.d_customer tb8 ON tb2.customer_id = tb8.customer_id
GROUP BY
    sk_product,
    sk_customer,
    sk_locale,
    sk_time;