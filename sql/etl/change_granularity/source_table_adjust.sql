DROP TABLE dbs.sr_sales;


CREATE TABLE dbs.sr_sales (
  sale_transation_id VARCHAR(50) NOT NULL,
  sale_product INT NOT NULL,
  customer_id INT NOT NULL,
  locality_id INT NOT NULL,
  sale_date TIMESTAMP NULL,
  sale_quantity INT NOT NULL,
  sale_price DECIMAL(10,2) NOT NULL,
  product_cost DECIMAL(10,2) NOT NULL
);

WITH ddata AS (
  SELECT 
    floor(random() * 1000000)::text AS sale_transation_id,
    floor(random() * 6 + 1) AS sale_product,
    floor(random() * 10 + 1) AS customer_id,
    floor(random() * 4 + 1) AS locality_id,
    ('2022-01-01'::date + (random() * 365)::integer) + interval '1 second' * (floor(random() * 86400)::integer) AS sale_date,
    floor(random() * 10 + 1) AS sale_quantity,
    round(CAST(random() * 100 + 1 AS numeric), 2) AS sale_price,
    round(CAST(random() * 50 + 1 AS numeric), 2) AS product_cost
  FROM generate_series(1,1000)
)

INSERT INTO dbs.sr_sales (
    sale_transation_id, 
    sale_product, 
    customer_id, 
    locality_id, 
    sale_date, 
    sale_quantity, 
    sale_price, 
    product_cost
    )
SELECT 
  'TRAN-' || sale_transation_id AS sale_transation_id,
  sale_product,
  customer_id,
  locality_id,
  sale_date,
  sale_quantity,
  round(CAST(sale_price AS numeric),2),
  round(CAST(product_cost AS numeric),2)
FROM ddata;
