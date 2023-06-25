-- Dimension Time table
CREATE TABLE dw.d_time (
  sk_time SERIAL PRIMARY KEY,
  time_date DATE,
  time_year INT NOT NULL,
  time_month INT NOT NULL,
  time_day INT NOT NULL
  
);

-- Dimension Product table
CREATE TABLE dw.d_product (
  sk_product SERIAL PRIMARY KEY,
  product_id INT NOT NULL,
  product_name VARCHAR(50) NOT NULL,
  product_category VARCHAR(50) NOT NULL,
  product_subcategory VARCHAR(50) NOT NULL
);

-- Dimension Locale table
CREATE TABLE dw.d_locale (
  sk_locale SERIAL PRIMARY KEY,
  locale_id INT NOT NULL,
  locale_country VARCHAR(50) NOT NULL,
  locale_region VARCHAR(50) NOT NULL,
  locale_state VARCHAR(50) NOT NULL,
  locale_city VARCHAR(50) NOT NULL
);

-- Dimension Customer table
CREATE TABLE dw.d_customer (
  sk_customer SERIAL PRIMARY KEY,
  customer_id INT NOT NULL,
  customer_name VARCHAR(50) NOT NULL,
  customer_type VARCHAR(50) NOT NULL
);

-- Fact Sales table
CREATE TABLE dw.fact_sales (
  sk_product INT NOT NULL,
  sk_customer INT NOT NULL,
  sk_locale INT NOT NULL,
  sk_time INT NOT NULL,
  quantity INT NOT NULL,
  price_sale DECIMAL(10,2) NOT NULL,
  price_product DECIMAL(10,2) NOT NULL,
  sales_revenue DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (sk_product, sk_customer, sk_locale, sk_time),
  FOREIGN KEY (sk_product) REFERENCES dw.d_product (sk_product),
  FOREIGN KEY (sk_customer) REFERENCES dw.d_customer (sk_customer),
  FOREIGN KEY (sk_locale) REFERENCES dw.d_locale (sk_locale),
  FOREIGN KEY (sk_time) REFERENCES dw.d_time (sk_time)
);
