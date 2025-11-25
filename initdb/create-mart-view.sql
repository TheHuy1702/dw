\connect datawarehouse
CREATE SCHEMA IF NOT EXISTS mart;
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.dm_daily_price AS
SELECT f.date, p.name, COUNT(*) as cnt, AVG(f.price) as avg_price, MIN(f.price) as min_price, MAX(f.price) as max_price
FROM dw.fact_price_snapshot f
JOIN dw.dim_product p ON f.product_id = p.product_id
GROUP BY f.date, p.name;
