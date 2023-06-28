ALTER TABLE IF EXISTS dw.dim_time
    ADD COLUMN time_hour text;

TRUNCATE TABLE dw.fact_sales;
TRUNCATE TABLE dw.dim_time CASCADE;
