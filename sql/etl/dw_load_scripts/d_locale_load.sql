INSERT INTO dw.d_locale (
    locale_id, 
    locale_country, 
    locale_region, 
    locale_state, 
    locale_city)
SELECT 
    locality_id AS locale_id, 
    locality_country AS locale_country, 
    locality_region AS locale_region, 
    CASE
        WHEN city_name = 'Natal' 
            THEN 'Rio Grande do Norte'
        WHEN city_name = 'Rio de Janeiro' 
            THEN 'Rio de Janeiro'
        WHEN city_name = 'Belo Horizonte' 
            THEN 'Minas Gerais'
        WHEN city_name = 'Salvador' 
            THEN 'Bahia'
        WHEN city_name = 'Blumenau' 
            THEN 'Santa Catarina'
        WHEN city_name = 'Curitiba' 
            THEN 'Paraná'
        WHEN city_name = 'Fortaleza' 
            THEN 'Ceará'
        WHEN city_name = 'Recife' 
            THEN 'Pernambuco'
        WHEN city_name = 'Porto Alegre' 
            THEN 'Rio Grande do Sul'
        WHEN city_name = 'Manaus' 
            THEN 'Amazonas'
    END locale_state, 
    city_name AS locale_city
FROM sta.sta_sr_localities lo,
	sta.sta_sr_cities ci
WHERE ci.city_id = lo.city_id;
