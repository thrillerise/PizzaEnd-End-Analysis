SELECT
    *
FROM
    order_details_2023
UNION ALL
SELECT
    *
FROM
    order_details_2024
UNION ALL
SELECT
    *
FROM
    order_details_2025;

CREATE TABLE
    order_details AS (
        SELECT
            *
        FROM
            order_details_2023
        UNION ALL
        SELECT
            *
        FROM
            order_details_2024
        UNION ALL
        SELECT
            *
        FROM
            order_details_2025
    );

CREATE TABLE
    ORDERS AS (
        SELECT
            *
        FROM
            orders_2023
        UNION ALL
        SELECT
            *
        FROM
            orders_2024
        UNION ALL
        SELECT
            *
        FROM
            orders_2025
    );

--checking for duplicates
select count(order_id)
from orders
group by order_id
having count(order_id) > 1;


--having a quick look of the order_details table
select *from order_details;

select * from pizzas

-- Getting the total revenue of all ordered pizza
select pt.name,sum(p.price * od.quantity) as revenue
from pizza_types as pt
left join pizzas as p
on p.pizza_type_id = pt.pizza_type_id
left join order_details as od
on od.pizza_id = p.pizza_id
group by pt.name
order by revenue desc limit 1;

--To Extract the year in the order table
select distinct(extract(YEAR from CAST(order_date AS DATE)))
FROM orders;

--Pizza that contributes the most revenue each year from 2023 to 2025
select pt.name,extract (YEAR FROM o.order_date::DATE) as year, sum(od.quantity * p.price) as total_revenue
from pizza_types as pt
left join pizzas as p
on p.pizza_type_id = pt.pizza_type_id
left join order_details as od
on od.pizza_id = p.pizza_id
left join orders as o
on o.order_id = od.order_id
group by year,name
order by year, total_revenue desc;