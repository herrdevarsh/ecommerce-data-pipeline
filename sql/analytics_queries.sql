-- Top 10 products by total revenue
SELECT product_id,
       SUM(total_amount) AS revenue
FROM fact_orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- Monthly revenue
SELECT substr(order_date, 1, 7) AS year_month,
       SUM(total_amount) AS revenue
FROM fact_orders
GROUP BY year_month
ORDER BY year_month;

-- Total revenue per customer
SELECT customer_id,
       SUM(total_amount) AS revenue
FROM fact_orders
GROUP BY customer_id
ORDER BY revenue DESC;
