/*use `dw`;
#Query 1
SELECT 
    product.product_name,
    date.month,
    SUM(total_sale) AS revenue,
    CASE 
        WHEN date.weekday BETWEEN 1 AND 5 THEN 'Weekday'
        ELSE 'Weekend'
    END AS day_type
FROM sales 
JOIN product ON product.product_id = sales.product_id
JOIN date ON date.time_id = sales.time_id
WHERE date.year = 2017
GROUP BY product.product_id, date.month, day_type
ORDER BY revenue DESC;


#Query 2
WITH quarterly_revenue AS (
    SELECT 
        store.store_name,
        date.quarter,
        SUM(total_sale) AS revenue
    FROM sales
    JOIN store ON store.store_id = sales.store_id
    JOIN date ON date.time_id = sales.time_id
    WHERE date.year = 2017
    GROUP BY store.store_id, date.quarter
)
SELECT 
    store_name,
    quarter,
    revenue,
    LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter) AS previous_quarter_revenue,
    CASE 
        WHEN LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter) IS NULL THEN NULL
        ELSE (revenue - LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter)) / LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter) * 100
    END AS growth_rate
FROM quarterly_revenue;



#Query 3

SELECT 
    store.store_name,
    supplier.supplier_name,
    product.product_name,
    SUM(total_sale) AS revenue
FROM sales
JOIN store ON store.store_id = sales.store_id
JOIN supplier ON supplier.supplier_id = sales.supplier_id
JOIN product ON product.product_id = sales.product_id
GROUP BY store.store_name, supplier.supplier_name, product.product_name
ORDER BY store.store_name, supplier.supplier_name, revenue DESC;

#Query 4
SELECT 
    product.product_name,
    CASE 
        WHEN date.month IN (3, 4, 5) THEN 'Spring'
        WHEN date.month IN (6, 7, 8) THEN 'Summer'
        WHEN date.month IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END AS season,
    SUM(total_sale) AS revenue
FROM sales
JOIN product ON product.product_id = sales.product_id
JOIN date ON date.time_id = sales.time_id
WHERE date.year = 2017
GROUP BY product.product_name, season;

#Query 5
WITH monthly_revenue AS (
    SELECT 
        store.store_name,
        supplier.supplier_name,
        date.month,
        SUM(total_sale) AS revenue
    FROM sales
    JOIN store ON store.store_id = sales.store_id
    JOIN supplier ON supplier.supplier_id = sales.supplier_id
    JOIN date ON date.time_id = sales.time_id
    WHERE date.year = 2017
    GROUP BY store.store_name, supplier.supplier_name, date.month
)
SELECT 
    store_name,
    supplier_name,
    month,
    revenue,
    LAG(revenue) OVER (PARTITION BY store_name, supplier_name ORDER BY month) AS previous_month_revenue,
    CASE 
        WHEN LAG(revenue) OVER (PARTITION BY store_name, supplier_name ORDER BY month) IS NULL THEN NULL
        ELSE (revenue - LAG(revenue) OVER (PARTITION BY store_name, supplier_name ORDER BY month)) / LAG(revenue) OVER (PARTITION BY store_name, supplier_name ORDER BY month) * 100
    END AS volatility_percentage
FROM monthly_revenue;

#Query 6
SELECT 
    p1.product_name AS product_1,
    p2.product_name AS product_2,
    COUNT(*) AS frequency
FROM sales s1
JOIN sales s2 ON s1.order_id = s2.order_id AND s1.product_id != s2.product_id
JOIN product p1 ON p1.product_id = s1.product_id
JOIN product p2 ON p2.product_id = s2.product_id
GROUP BY p1.product_name, p2.product_name
ORDER BY frequency DESC
LIMIT 5;

#Query 7
SELECT 
    store.store_name,
    supplier.supplier_name,
    product.product_name,
    SUM(total_sale) AS revenue
FROM sales
JOIN store ON store.store_id = sales.store_id
JOIN supplier ON supplier.supplier_id = sales.supplier_id
JOIN product ON product.product_id = sales.product_id
GROUP BY store.store_name, supplier.supplier_name, product.product_name WITH ROLLUP
ORDER BY store.store_name, supplier.supplier_name, revenue DESC;


#Query 8
SELECT 
    product.product_name,
    CASE 
        WHEN date.month <= 6 THEN 'H1'
        ELSE 'H2'
    END AS half_of_year,
    SUM(total_sale) AS revenue,
    SUM(quantity) AS volume
FROM sales
JOIN product ON product.product_id = sales.product_id
JOIN date ON date.time_id = sales.time_id
WHERE date.year = 2017
GROUP BY product.product_name, half_of_year;

#Query 9
WITH daily_sales AS (
    SELECT 
        product.product_name,
        date.date,
        SUM(total_sale) AS daily_sales
    FROM sales
    JOIN product ON product.product_id = sales.product_id
    JOIN date ON date.time_id = sales.time_id
    WHERE date.year = 2017
    GROUP BY product.product_name, date.date
),
avg_daily_sales AS (
    SELECT 
        product_name,
        AVG(daily_sales) AS avg_sales
    FROM daily_sales
    GROUP BY product_name
)
SELECT 
    ds.product_name,
    ds.date,
    ds.daily_sales,
    ads.avg_sales,
    CASE 
        WHEN ds.daily_sales > ads.avg_sales * 2 THEN 'Outlier'
        ELSE 'Normal'
    END AS sales_status
FROM daily_sales ds
JOIN avg_daily_sales ads ON ds.product_name = ads.product_name;

#Query 10
DROP VIEW IF EXISTS `dw`.`STORE_QUARTERLY_SALES`;
CREATE VIEW `dw`.`STORE_QUARTERLY_SALES` AS 
SELECT 
    store.store_name,
    date.quarter,
    SUM(total_sale) AS revenue
FROM sales
JOIN store ON store.store_id = sales.store_id
JOIN date ON date.time_id = sales.time_id
GROUP BY store.store_name, date.quarter;
*/

use `dw`;
#Query 1
SELECT store.store_name,SUM(total_sale) as revenue FROM sales JOIN store on store.store_id = sales.store_id JOIN date on date.time_id = sales.time_id where date.month = "september" and date.year = 2017 group by sales.store_id order by revenue desc LIMIT 3;

#Query 2
SELECT supplier.supplier_name,SUM(total_sale) as Revenue FROM sales JOIN date on date.time_id = sales.time_id JOIN supplier on supplier.supplier_id = sales.supplier_id where date.weekend = 1 group by supplier.supplier_id order by Revenue desc LIMIT 10;

#We can forcast the next top suppliers by extracting data only for weekends where there wasnt any promotion which might give us a hint of the top suppliers
#whose product is needed by the customers regardless of a promotion or not, also suppliers dealing in necessity goods would be more in demand.

#Query 3
SELECT sales.product_id,product.product_name,date.month,date.quarter,SUM(total_sale) as Revenue FROM sales JOIN product on product.product_id = sales.product_id JOIN date on date.time_id = sales.time_id group by sales.product_id, date.quarter,date.month;

#Query 4
SELECT store.store_name,product.product_name,SUM(total_sale) as Revenue from sales join store on store.store_id = sales.store_id join product on product.product_id = sales.product_id group by sales.store_id,sales.product_id;

#Query 5
SELECT store.store_name,date.quarter,SUM(total_sale) as Revenue from sales join date on date.time_id = sales.time_id join store on store.store_id = sales.store_id group by store.store_id,date.quarter;

#Query 6
SELECT product.product_name,SUM(total_sale) as Revenue from sales join date on date.time_id = sales.time_id join product on product.product_id = sales.product_id where date.weekend = 1 group by sales.product_id order by Revenue desc limit 5;

#Query 7
SELECT store_id,supplier_id,product_id from sales group by store_id,supplier_id,product_id with rollup;

#This query assumes that there is a hierarchy between store,supplier and product which is store > supplier > product.
#ROLLUP makes grouping sets from this hierarchy we can see in the query result that all values matching the first value of store_id are mentioned and so on
#also if a value is not present then it shows NULL.

#Query 8
#Half of Year Analysis
SELECT product.product_name,date.half_of_year,SUM(total_sale) as Revenue from sales join product on product.product_id = sales.product_id join date on date.time_id = sales.time_id where date.year = 2017 group by sales.product_id, half_of_year;

#Yearly Analysis
SELECT product.product_name,SUM(total_sale) as Revenue from sales join product on product.product_id = sales.product_id join date on date.time_id = sales.time_id where date.year = 2017 group by sales.product_id;

#Query 9
SELECT * FROM dw.product where product_name = "Tomatoes";
#There are two entries for Tomatoes in the Products Table.

#Query 10
DROP TABLE if exists `dw`.`STORE_PRODUCT_ANALYSIS`;
CREATE TABLE `STORE_PRODUCT_ANALYSIS` AS 
  SELECT store_name,product_name,total_sale FROM sales join store on sales.store_id = store.store_id join product on sales.product_id = product.product_id order by store_name,product_name;

SELECT * from STORE_PRODUCT_ANALYSIS;
#The benefit of materialized view is it reduces the execution time for complex queries with joins and aggregate functions
#it is most useful when the query execution time is high but the result is small.











