-- Dimension Customer table
SELECT
	customer_id, 
	customer_name, 
	customer_type_name AS customer_type
FROM sta.sta_sr_customers cs,
	sta.sta_sr_customer_type ct
WHERE cs.customer_type_id = ct.customer_type_id;

-- Dimension Product table
SELECT 
	product_id, 
	product_name, 
	category_name AS product_category, 
	subcategory_name AS product_subcategory
FROM sta.sta_sr_products pr,
	sta.sta_sr_subcategory sc,
	sta.sta_sr_categories ct
WHERE ct.category_id = sc.category_id
AND sc.subcategory_id = pr.subcategory_id;

-- Dimension Locale table
SELECT locality_id AS locale_id, 
       locality_country AS locale_country, 
       locality_region AS locale_region, 
       CASE
        WHEN city_name = 'Natal' THEN 'Rio Grande do Norte'
        WHEN city_name = 'Rio de Janeiro' THEN 'Rio de Janeiro'
        WHEN city_name = 'Belo Horizonte' THEN 'Minas Gerais'
        WHEN city_name = 'Salvador' THEN 'Bahia'
        WHEN city_name = 'Blumenau' THEN 'Santa Catarina'
        WHEN city_name = 'Curitiba' THEN 'Paraná'
        WHEN city_name = 'Fortaleza' THEN 'Ceará'
        WHEN city_name = 'Recife' THEN 'Pernambuco'
        WHEN city_name = 'Porto Alegre' THEN 'Rio Grande do Sul'
        WHEN city_name = 'Manaus' THEN 'Amazonas'
       END locale_state, 
       city_name AS locale_city
FROM sta.sta_sr_localities lo,
	sta.sta_sr_cities ci
WHERE ci.city_id = lo.city_id;

-- Dimension Time table
SELECT EXTRACT(YEAR FROM d)::INT AS time_year, 
       EXTRACT(MONTH FROM d)::INT AS time_month, 
       EXTRACT(DAY FROM d)::INT AS time_day,
	   d::DATE
FROM generate_series('2020-01-01'::DATE, '2024-12-31'::DATE,
	'1 day'::INTERVAL) d;
