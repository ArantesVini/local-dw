-- Dimension Time table
CREATE TABLE d_time (
  time_id INT PRIMARY KEY AUTO_INCREMENT,
  time_year INT NOT NULL,
  time_month INT NOT NULL,
  time_day INT NOT NULL
);

-- Dimension Product table
CREATE TABLE d_product (
  product_id INT PRIMARY KEY AUTO_INCREMENT,
  product_name VARCHAR(50) NOT NULL,
  product_category VARCHAR(50) NOT NULL,
  product_subcategory VARCHAR(50) NOT NULL
);

-- Dimension Locale table
CREATE TABLE d_locale (
  locale_id INT PRIMARY KEY AUTO_INCREMENT,
  locale_country VARCHAR(50) NOT NULL,
  locale_region VARCHAR(50) NOT NULL,
  locale_state VARCHAR(50) NOT NULL,
  locale_city VARCHAR(50) NOT NULL
);

-- Dimension Customer table
CREATE TABLE d_customer (
  customer_id INT PRIMARY KEY AUTO_INCREMENT,
  customer_name VARCHAR(50) NOT NULL,
  customer_type VARCHAR(50) NOT NULL
);

-- Fact Sales table
CREATE TABLE fact_sales (
  product_id INT NOT NULL,
  customer_id INT NOT NULL,
  locale_id INT NOT NULL,
  time_id INT NOT NULL,
  quantity INT NOT NULL,
  price_sale DECIMAL(10,2) NOT NULL,
  price_product DECIMAL(10,2) NOT NULL,
  sales_revenue DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (product_id, customer_id, locale_id, time_id),
  FOREIGN KEY (product_id) REFERENCES d_product (product_id),
  FOREIGN KEY (customer_id) REFERENCES d_customer (customer_id),
  FOREIGN KEY (locale_id) REFERENCES d_locale (locale_id),
  FOREIGN KEY (time_id) REFERENCES d_time (time_id)
);
