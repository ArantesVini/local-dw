CREATE TABLE sta.sta_sr_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL
);

CREATE TABLE sta.sta_sr_subcategory (
    subcategory_id SERIAL PRIMARY KEY,
    subcategory_name VARCHAR(255) NOT NULL,
    category_id INTEGER REFERENCES sta.sta_sr_categories(category_id)
);

CREATE TABLE sta.sta_sr_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_price NUMERIC(10,2) NOT NULL,
    subcategory_id INTEGER REFERENCES sta.sta_sr_subcategory(subcategory_id)
);

CREATE TABLE sta.sta_sr_cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) NOT NULL
);

CREATE TABLE sta.sta_sr_localities (
    locality_id SERIAL PRIMARY KEY,
    locality_country VARCHAR(255) NOT NULL,
    locality_region VARCHAR(255) NOT NULL,
    city_id INTEGER REFERENCES sta.sta_sr_cities(city_id)
);

CREATE TABLE sta.sta_sr_customer_type (
    customer_type_id SERIAL PRIMARY KEY,
    customer_type_name VARCHAR(255) NOT NULL
);

CREATE TABLE sta.sta_sr_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NULL,
    customer_email VARCHAR(255) NULL,
    city_id INTEGER REFERENCES sta.sta_sr_cities(city_id),
    customer_type_id INTEGER REFERENCES sta.sta_sr_customer_type(customer_type_id)
);


CREATE TABLE sta.sta_sr_sales (
  sale_transation_id VARCHAR(50) NOT NULL,
  sale_product INT NOT NULL,
  customer_id INT NOT NULL,
  locality_id INT NOT NULL,
  sale_date DATE NULL,
  sale_quantity INT NOT NULL,
  sale_price DECIMAL(10,2) NOT NULL,
  product_cost DECIMAL(10,2) NOT NULL
);
