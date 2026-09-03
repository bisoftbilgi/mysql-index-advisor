-- sample queries for tuning_demo (also in the web dropdown)

-- 1) status filter, expect type=ALL until you add an index
SELECT * FROM orders WHERE status = 'shipped';

-- 2) function on column, index on order_date still may not be used
SELECT order_id, amount FROM orders WHERE YEAR(order_date) = 2024;

-- 3) join, orders.customer_id has no index
SELECT c.full_name, o.amount, o.order_date
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE c.email = 'user100@example.com';

-- 4) leading wildcard
SELECT * FROM customers WHERE full_name LIKE '%ahmet%';

-- 5) filesort likely
SELECT order_id, amount FROM orders ORDER BY order_date DESC LIMIT 50;

-- 6) city + join + group
SELECT c.city, COUNT(*) AS cnt
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE c.city = 'Ankara'
GROUP BY c.city;

