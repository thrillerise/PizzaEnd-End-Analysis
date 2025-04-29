SELECT * FROM order_details_2023
UNION ALL
SELECT * FROM order_details_2024
UNION ALL
SELECT * FROM order_details_2025;


CREATE TABLE order_details AS(
SELECT * FROM order_details_2023
UNION ALL
SELECT * FROM order_details_2024
UNION ALL
SELECT * FROM order_details_2025
);

CREATE TABLE order_details AS(
SELECT * FROM orders_2023
UNION ALL
SELECT * FROM orders_2024
UNION ALL
SELECT * FROM orders_2025
);
