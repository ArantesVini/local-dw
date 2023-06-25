INSERT INTO dw.d_time (
    time_year,
    time_month,
    time_day,
    time_date
)
SELECT 
    EXTRACT(YEAR FROM d)::INT, 
    EXTRACT(MONTH FROM d)::INT, 
    EXTRACT(DAY FROM d)::INT, d::DATE
FROM generate_series('2020-01-01'::DATE, '2024-12-31'::DATE,
     '1 day'::INTERVAL) d;
