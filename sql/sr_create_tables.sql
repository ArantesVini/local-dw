CREATE TABLE dbs.sr_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL
);

INSERT INTO dbs.sr_categories (category_name) VALUES ('Computers');
INSERT INTO dbs.sr_categories (category_name) VALUES ('Smartphones');
INSERT INTO dbs.sr_categories (category_name) VALUES ('Printers');

CREATE TABLE dbs.sr_subcategory (
    subcategory_id SERIAL PRIMARY KEY,
    subcategory_name VARCHAR(255) NOT NULL,
    category_id INTEGER REFERENCES dbs.sr_categories(category_id)
);

INSERT INTO dbs.sr_subcategory (subcategory_name, category_id) VALUES ('Notebook', 1);
INSERT INTO dbs.sr_subcategory (subcategory_name, category_id) VALUES ('Desktop', 1);
INSERT INTO dbs.sr_subcategory (subcategory_name, category_id) VALUES ('iPhone', 2);
INSERT INTO dbs.sr_subcategory (subcategory_name, category_id) VALUES ('Samsung Galaxy', 2);
INSERT INTO dbs.sr_subcategory (subcategory_name, category_id) VALUES ('Laser', 3);
INSERT INTO dbs.sr_subcategory (subcategory_name, category_id) VALUES ('Matricial', 3);

CREATE TABLE dbs.sr_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_price NUMERIC(10,2) NOT NULL,
    subcategory_id INTEGER REFERENCES dbs.sr_subcategory(subcategory_id)
);

INSERT INTO dbs.sr_products (product_name, product_price, subcategory_id) VALUES ('Apple MacBook Pro M2', 6589.99, 1);
INSERT INTO dbs.sr_products (product_name, product_price, subcategory_id) VALUES ('Desktop Dell 16 GB', 1500.50, 1);
INSERT INTO dbs.sr_products (product_name, product_price, subcategory_id) VALUES ('iPhone 14', 4140.00, 2);
INSERT INTO dbs.sr_products (product_name, product_price, subcategory_id) VALUES ('Samsung Galaxy Z', 3500.99, 2);
INSERT INTO dbs.sr_products (product_name, product_price, subcategory_id) VALUES ('HP 126A Original LaserJet Imaging Drum', 300.90, 3);
INSERT INTO dbs.sr_products (product_name, product_price, subcategory_id) VALUES ('Epson LX-300 II USB', 350.99, 3);

CREATE TABLE dbs.sr_cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) NOT NULL
);

INSERT INTO dbs.sr_cities (city_name) VALUES
    ('Natal'),
    ('Rio de Janeiro'),
    ('Belo Horizonte'),
    ('Salvador'),
    ('Blumenau'),
    ('Curitiba'),
    ('Fortaleza'),
    ('Recife'),
    ('Porto Alegre'),
    ('Manaus');

CREATE TABLE dbs.sr_localities (
    locality_id SERIAL PRIMARY KEY,
    locality_country VARCHAR(255) NOT NULL,
    locality_region VARCHAR(255) NOT NULL,
    city_id INTEGER REFERENCES dbs.sr_cities(city_id)
);

INSERT INTO dbs.sr_localities (locality_country, locality_region, city_id) VALUES
    ('Brasil', 'Nordeste', 1),
    ('Brasil', 'Sudeste', 2),
    ('Brasil', 'Sudeste', 3),
    ('Brasil', 'Nordeste', 4),
    ('Brasil', 'Sul', 5),
    ('Brasil', 'Sul', 6),
    ('Brasil', 'Nordeste', 7),
    ('Brasil', 'Nordeste', 8),
    ('Brasil', 'Sul', 9),
    ('Brasil', 'Norte', 10);

CREATE TABLE dbs.sr_customer_type (
    customer_type_id SERIAL PRIMARY KEY,
    customer_type_name VARCHAR(255) NOT NULL
);

INSERT INTO dbs.sr_customer_type (customer_type_name) VALUES ('Corporativo');
INSERT INTO dbs.sr_customer_type (customer_type_name) VALUES ('Consumidor');
INSERT INTO dbs.sr_customer_type (customer_type_name) VALUES ('Desativado');

CREATE TABLE dbs.sr_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NULL,
    customer_email VARCHAR(255) NULL,
    city_id INTEGER REFERENCES dbs.sr_cities(city_id),
    customer_type_id INTEGER REFERENCES dbs.sr_customer_type(customer_type_id)
);

INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('João Silva', 'joao.silva@exemplo.com', 1, 1);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Maria Santos', 'maria.santos@exemplo.com', 2, 2);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Pedro Lima', 'pedro.lima@exemplo.com', 3, 2);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Ana Rodrigues', 'ana.rodrigues@exemplo.com', 4, 2);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('José Oliveira', 'jose.oliveira@exemplo.com', 1, 2);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Carla Santos', 'carla.santos@exemplo.com', 4, 1);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Marcos Souza', 'marcos.souza@exemplo.com', 5, 2);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Julia Silva', 'julia.silva@exemplo.com', 1, 1);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Lucas Martins', 'lucas.martins@exemplo.com', 3, 3);
INSERT INTO dbs.sr_customers (customer_name, customer_email, city_id, customer_type_id) VALUES ('Fernanda Lima', 'fernanda.lima@exemplo.com', 4, 2);


CREATE TABLE dbs.sr_sales (
  sale_transation_id VARCHAR(50) NOT NULL,
  sale_product INT NOT NULL,
  customer_id INT NOT NULL,
  locality_id INT NOT NULL,
  sale_date DATE NULL,
  sale_quantity INT NOT NULL,
  sale_price DECIMAL(10,2) NOT NULL,
  product_cost DECIMAL(10,2) NOT NULL
);


WITH t_data AS (
  SELECT 
    floor(random() * 1000000)::text AS sale_transation_id,
    floor(random() * 6 + 1) AS sale_product,
    floor(random() * 10 + 1) AS customer_id,
    floor(random() * 4 + 1) AS locality_id,
    '2022-01-01'::date + floor(random() * 365)::integer AS sale_date,
    floor(random() * 10 + 1) AS sale_quantity,
    round(CAST(random() * 100 + 1 AS numeric), 2) AS sale_price,
    round(CAST(random() * 50 + 1 AS numeric), 2) AS product_cost
  FROM generate_series(1,1000)
)
INSERT INTO dbs.sr_sales (sale_transation_id, sale_product, customer_id, locality_id, sale_date, sale_quantity, sale_price, product_cost)
SELECT 
  'TRAN-' || sale_transation_id AS sale_transation_id,
  sale_product,
  customer_id,
  locality_id,
  sale_date,
  sale_quantity,
  round(CAST(sale_price AS numeric),2),
  round(CAST(product_cost AS numeric),2)
FROM t_data;


