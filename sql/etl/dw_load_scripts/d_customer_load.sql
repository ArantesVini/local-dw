INSERT INTO dw.d_customer (
    customer_id,
    customer_name, 
    customer_type
    )
SELECT 
    customer_id, 
    customer_name, 
    customer_type_name
FROM sta.sta_sr_customers tb1,
     sta.sta_sr_customer_type tb2
WHERE tb2.customer_type_id = tb1.customer_type_id;